"""
This python module is used for fetching and processing FII DII Data
"""

# pylint: disable=line-too-long,len-as-condition,invalid-name,old-division

import random
import string
import datetime
import json
import subprocess

from datetime import timedelta, datetime

import xlrd  # EXCEL Processing

from config import constants, app_context
from helpers.nse_bhav_copy_helper import fetch_day_wise_historical_quotes
from models.fii_dii_data import FiiDiiModel
from models.nse_data import NSEDataModel
from utils import trigger_pagerduty
from utils.compress_utils import convert_to_gzip_format
from utils.google_sheet import get_google_sheet_data
from utils.trade_time import (
    get_market_open,
    get_next_working_day,
    get_previous_working_day,
    is_market_day,
    get_last_trading_day,
    localize_with_timezone,
)
from utils.custom_logger import get_custom_logger_for_tasks

from time import sleep

from config import app_context

MINIMUM_RECORDS_REQUIRED = 30

FII_DII_DAILY_LATEST_MONTH_REDIS_KEY = "LATEST"
FII_DII_CASH_REDIS_KEY = "FII_DII_CASH"
FII_DII_CASH_TTL = 30 * 24 * 60 * 60  # 30 days

INDEX_FUTURE_NSE_FILE_NAME = "INDEX FUTURES"
NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING_FOR_INDEX = {
    # We are using "FUT" since the column in db is varchar (16)
    "FINNIFTY FUTURES": "FUT-FINNIFTY",
    "BANKNIFTY FUTURES": "FUT-BANKNIFTY",
    "NIFTY FUTURES": "FUT-NIFTY",
    "MIDCPNIFTY FUTURES": "FUT-MIDCPNIFTY",
    "NIFTYNXT50 FUTURES": "FUT-NIFTYNXT50"
}
NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING = {
    INDEX_FUTURE_NSE_FILE_NAME: "FUTURE",
    **NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING_FOR_INDEX,
}
NSE_FILE_OPTIONS_DATA_ROW_MAPPING = {"FII": 4, "DII": 3, "PRO": 5, "CLIENT": 2}
HISTORICAL_RANGE_IN_DAYS = 180

INDECISIVE = ""

BULLISH = "BULLISH"
BEARISH = "BEARISH"
VOLATILE = "VOLATILE"
NEUTRAL = "NEUTRAL"

STRONG = "Strong"
MEDIUM = "Medium"
MILD = "Mild"


def notify_pagerduty(summery):
    """
    Call Pager Duty to report error
    :return:
    """
    trigger_pagerduty(
        "generate_fii_dii_data_cache",
        {"source": "Celery Worker", "severity": "error", "summary": summery},
    )


def generate_participants_options_data_url(data_fetch_date):
    """
    Generate FII OPTIONS DATA URL FOR SPECIFIED DATE
    :param data_fetch_date:
    :return:
    """
    # https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_28082020.csv
    date = data_fetch_date.strftime("%d%m%Y")
    return f"https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{date}.csv"


def generate_fii_futures_data_url(data_fetch_date):
    """
    Generate FII Futures Data URL for specific date
    :param data_fetch_date:
    :return:
    """
    # https://nsearchives.nseindia.com/content/fo/fii_stats_27-Aug-2020.xls
    date = data_fetch_date.strftime("%d-%b-%Y")
    return f"https://nsearchives.nseindia.com/content/fo/fii_stats_{date}.xls"


async def fetch_cash_data():
    """
    we fetch 2 urls,
      first to get nse home page and save cookies
      second to load the required url using the cookies from above

    this seems to bypass nse restrictions for now
    """

    rand_text = "".join(random.choices(string.ascii_letters + string.digits, k=8))
    cookie_file = f"/tmp/nse_cookie_{rand_text}"
    base_command = [
        "curl",
        "-L",
        "--connect-timeout",
        "10",
        "--max-time",
        "60",
        "-H",
        "authority: www.nseindia.com",
        "-H",
        "cache-control: max-age=0",
        "-H",
        "dnt: 1",
        "-H",
        "upgrade-insecure-requests: 1",
        "-H",
        "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36",
        "-H",
        "sec-fetch-user: ?1",
        "-H",
        "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "-H",
        "sec-fetch-site: none",
        "-H",
        "sec-fetch-mode: navigate",
        "-H",
        "accept-encoding: gzip, deflate, br",
        "-H",
        "accept-language: en-US,en;q=0.9,hi;q=0.8",
        "--compressed",
    ]

    # command to load home page and save cookies
    root_url_cmd = ["--cookie-jar", cookie_file, "https://www.nseindia.com/reports/fii-dii"]

    # command to load the required url using cookies from above
    data_url_cmd = ["--cookie", cookie_file, "https://www.nseindia.com/api/fiidiiTradeReact"]

    subprocess.check_output(base_command + root_url_cmd)
    data = subprocess.check_output(base_command + data_url_cmd)
    return data.decode("utf-8")


async def fetch_data_from_url(url, expected_content_type):
    """
    Fetches data from url, checks if its content type matches with the mentioned data type
    If not it should raise an issue
    else return the data to calle

    :param url: URL from which data need to be fetched
    :param expected_content_type: content type of data to be fetched
    :return: Return Fetched data
    """

    download_command = [
        "curl",
        url,
        "-L",
        "--connect-timeout",
        "10",
        "--max-time",
        "60",
        "-H",
        "Connection: keep-alive",
        "-H",
        "Host: nsearchives.nseindia.com",
        # "-H",
        # "authority: www.nseindia.com",
        "-H",
        "Upgrade-Insecure-Requests: 1",
        "-H",
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
        "-H",
        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "-H",
        "Sec-Fetch-Site: none",
        "-H",
        "Sec-Fetch-Mode: navigate",
        "-H",
        "Accept-Encoding: gzip, deflate, br",
        "-H",
        "Accept-Language: en-US,en;q=0.9",
        "--compressed",
    ]

    data = subprocess.check_output(download_command)
    if expected_content_type == "application/vnd.ms-excel":
        return data
    if expected_content_type == "application/csv":
        return data.decode("utf-8")
    if expected_content_type == "text/html":
        return data.decode("utf-8")
    notify_pagerduty(f"fii dii unexpected content type {expected_content_type} for {url}")
    return None


def process_fii_fut_excel(data, data_fetch_date):
    """
    :param data: excel data stream
    :param data_fetch_date: date of data
    :return: array of fii futures data
    """

    work_book = xlrd.open_workbook(file_contents=data)
    xl_sheet = work_book.sheet_by_index(0)

    indexes_not_found_in_file = list(NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING.keys())

    all_index_data = []

    for i in range(3, 10):
        index_name_from_file = str(xl_sheet.cell(i, 0).value).strip().upper()
        if NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING.get(index_name_from_file) is not None:
            all_index_data.extend(
                [
                    {
                        "traded_day": data_fetch_date.date(),
                        "trading_institution": "FII",
                        "trade_type": NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING[index_name_from_file],
                        "buy_or_sell": "BUY_AMOUNT",
                        "value": float(xl_sheet.cell(i, 2).value),
                    },
                    {
                        "traded_day": data_fetch_date.date(),
                        "trading_institution": "FII",
                        "trade_type": NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING[index_name_from_file],
                        "buy_or_sell": "SELL_AMOUNT",
                        "value": float(xl_sheet.cell(i, 4).value),
                    },
                    {
                        "traded_day": data_fetch_date.date(),
                        "trading_institution": "FII",
                        "trade_type": NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING[index_name_from_file],
                        "buy_or_sell": "BUY_QUANTITY",
                        "value": float(xl_sheet.cell(i, 1).value),
                    },
                    {
                        "traded_day": data_fetch_date.date(),
                        "trading_institution": "FII",
                        "trade_type": NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING[index_name_from_file],
                        "buy_or_sell": "SELL_QUANTITY",
                        "value": float(xl_sheet.cell(i, 3).value),
                    },
                ]
            )
            if index_name_from_file == INDEX_FUTURE_NSE_FILE_NAME:
                all_index_data.append(
                    {
                        "traded_day": data_fetch_date.date(),
                        "trading_institution": "FII",
                        "trade_type": "FUTURE",
                        "buy_or_sell": "OI",
                        "value": float(xl_sheet.cell(i, 6).value),
                    }
                )
            indexes_not_found_in_file.remove(index_name_from_file)

    # We will raise pd and move on, for error we will have to manually enter the rows in db.
    if len(indexes_not_found_in_file) > 0:
        print(
            "fii fut excel read error : unable to find future for indexes",
            indexes_not_found_in_file,
            " for",
            data_fetch_date,
        )
        notify_pagerduty(
            f"fii fut excel read error: unable to find future for indexes {indexes_not_found_in_file} for {data_fetch_date}"
        )

        # This is compulsory data and must be available, otherwise error out.
        if INDEX_FUTURE_NSE_FILE_NAME in indexes_not_found_in_file:
            raise Exception(
                f"fii fut excel read error: {INDEX_FUTURE_NSE_FILE_NAME} was not found in the file, therefore skipping write to db."
            )

    return all_index_data


def process_participants_option_csv(data, data_fetch_date):
    return (
        process_nse_option_csv(data, "FII", data_fetch_date)
        + process_nse_option_csv(data, "DII", data_fetch_date)
        + process_nse_option_csv(data, "PRO", data_fetch_date)
        + process_nse_option_csv(data, "CLIENT", data_fetch_date)
    )


def process_nse_option_csv(data, participant, data_fetch_date):
    """
    :param data: CSV Text Data
    :param participant: Option participant name
    :param data_fetch_date:  Date of data being processed
    :return: array of options data for passed participant
    """
    row_number = NSE_FILE_OPTIONS_DATA_ROW_MAPPING[participant]
    data_row_item = data.split("\n")[row_number].split(",")

    future_index_long = data_row_item[1]
    future_index_short = data_row_item[2]
    future_stock_long = data_row_item[3]
    future_stock_short = data_row_item[4]

    call_long = data_row_item[5]
    put_long = data_row_item[6]
    call_short = data_row_item[7]
    put_short = data_row_item[8]

    call_long_obj = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "CALL",
        "buy_or_sell": "LONG",
        "value": int(float(call_long)),
    }

    put_long_obj = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "PUT",
        "buy_or_sell": "LONG",
        "value": int(float(put_long)),
    }

    call_short_obj = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "CALL",
        "buy_or_sell": "SHORT",
        "value": int(float(call_short)),
    }

    put_short_obj = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "PUT",
        "buy_or_sell": "SHORT",
        "value": int(float(put_short)),
    }

    buy_contract_quantity_object = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "FUTURE-INDEX",
        "buy_or_sell": "LONG",
        "value": future_index_long,
    }
    sell_contract_quantity_object = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "FUTURE-INDEX",
        "buy_or_sell": "SHORT",
        "value": future_index_short,
    }

    future_stock_long_object = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "FUTURE-STOCK",
        "buy_or_sell": "LONG",
        "value": future_stock_long,
    }
    future_stock_short_object = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": participant,
        "trade_type": "FUTURE-STOCK",
        "buy_or_sell": "SHORT",
        "value": future_stock_short,
    }

    return [
        call_long_obj,
        put_long_obj,
        call_short_obj,
        put_short_obj,
        buy_contract_quantity_object,
        sell_contract_quantity_object,
        future_stock_long_object,
        future_stock_short_object,
    ]


def _process_fii_dii_cash_row(row, trading_institution, data_fetch_date):
    fetch_dt_str = data_fetch_date.strftime("%d-%b-%Y")

    data_date = row["date"]
    buy = float(row["buyValue"])
    sell = float(row["sellValue"])
    if fetch_dt_str != data_date:
        raise Exception(
            f"DII Cash market data date mismatch, expected {data_fetch_date}, got {data_date}"
        )
    buy_object = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": trading_institution,
        "trade_type": "CASH",
        "buy_or_sell": "BUY",
        "value": buy,
    }
    sell_object = {
        "traded_day": data_fetch_date.date(),
        "trading_institution": trading_institution,
        "trade_type": "CASH",
        "buy_or_sell": "SELL",
        "value": sell,
    }
    return [buy_object, sell_object]


def process_cash_response(data_text, data_fetch_date):
    """
    [
      {"category":"DII **","date":"28-Aug-2020","buyValue":"5341.4","sellValue":"5884.96","netValue":"-543.56"},
      {"category":"FII/FPI *","date":"28-Aug-2020","buyValue":"6618.88","sellValue":"5614.77","netValue":"1004.11"}
    ]
    """
    data = json.loads(data_text)
    result = []
    for row in data:
        if row["category"].startswith("DII"):
            result = result + _process_fii_dii_cash_row(row, "DII", data_fetch_date)
        if row["category"].startswith("FII"):
            result = result + _process_fii_dii_cash_row(row, "FII", data_fetch_date)

    if len(result) != 4:
        print("FII DII CASH DATA\n", data_text)
        raise Exception(f"FII DII Cash market data expected to have 4 row got {len(result)} rows")

    return result


async def fetch_and_process_data(data_fetch_date, is_current_date):
    """
    :param data_fetch_date: date
    :param is_current_date: this flag hold true if we are fetching data of current day.
    this is to prevent fetching cash market data for past dates
    :return:
    """

    options_url = generate_participants_options_data_url(data_fetch_date)
    fii_futures_url = generate_fii_futures_data_url(data_fetch_date)

    complete_fii_dii_data = []

    #  CASH MARKET DATA
    if is_current_date:
        fii_dii_cash_data = await fetch_cash_data()
        complete_fii_dii_data = process_cash_response(fii_dii_cash_data, data_fetch_date)

    # FUTURE MARKET BUY/SELL & OI DATA
    fii_futures_data = await fetch_data_from_url(fii_futures_url, "application/vnd.ms-excel")
    fii_futures_processed_data = process_fii_fut_excel(fii_futures_data, data_fetch_date)
    # OPTION MARKET BUY/SELL DATA
    options_data = await fetch_data_from_url(options_url, "application/csv")
    all_options_processed_data = process_participants_option_csv(options_data, data_fetch_date)
    complete_fii_dii_data = (
        complete_fii_dii_data + fii_futures_processed_data + all_options_processed_data
    )
    return complete_fii_dii_data


async def fetch_fii_dii_data(traded_day, is_current_date):
    """
    This function takes a date and checks if number of
    records on day is greater than 25, only then we can confirm that all valid data are present,
    else we have to delete and fetch data again
    :param traded_day: date to fetch data for
    :param is_current_date:  if present day or not
    :return: fetched FII DII data
    """
    complete_fii_dii_data = []

    no_of_records_for_date = await FiiDiiModel.get_count_of_data_on(traded_day)
    if no_of_records_for_date < MINIMUM_RECORDS_REQUIRED:
        complete_fii_dii_data = await fetch_and_process_data(traded_day, is_current_date)
        await FiiDiiModel.delete_fii_dii_record(traded_day)
        # Multiple fetches to NSE will result in being blacklisted for 2-3 days
        print("Sleeping for 20sec to be within NSE rate limit")
        sleep(20)

    return complete_fii_dii_data


def get_action_for_net_value(net_value):
    if net_value is None:
        return INDECISIVE
    if net_value == 0:
        return INDECISIVE
    elif net_value > 0:
        return "BUY"
    else:
        return "SELL"


def get_view_for_net_value(net_value):
    if net_value is None:
        return INDECISIVE
    if net_value == 0:
        return INDECISIVE
    elif net_value > 0:
        return BULLISH
    else:
        return BEARISH


# view for put option is as follows
def get_view_for_put_option(net_oi):
    if net_oi is None:
        return INDECISIVE
    if net_oi == 0:
        return INDECISIVE
    elif net_oi > 0:
        return BEARISH
    else:
        return BULLISH


def calculate_cash_redis_values(todays_values):
    fii_net = todays_values["FII_CASH_BUY"] - todays_values["FII_CASH_SELL"]
    dii_net = todays_values["DII_CASH_BUY"] - todays_values["DII_CASH_SELL"]
    return {
        "fii": {
            "buy_sell_difference": fii_net,
            "buy": todays_values["FII_CASH_BUY"],
            "sell": todays_values["FII_CASH_SELL"],
        },
        "dii": {
            "buy_sell_difference": dii_net,
            "buy": todays_values["DII_CASH_BUY"],
            "sell": todays_values["DII_CASH_SELL"],
        },
    }


def calculate_futures_redis_values_for_fii(todays_values, previous_values, future_expiry_map, date):
    today_futures_index_outstanding_oi = (
        todays_values["FII_FUTURE-INDEX_LONG"] - todays_values["FII_FUTURE-INDEX_SHORT"]
    )
    previous_futures_index_outstanding_oi = (
        previous_values["FII_FUTURE-INDEX_LONG"] - previous_values["FII_FUTURE-INDEX_SHORT"]
    )
    # oi_change = todays_values["FII_FUTURE_BUY_QUANTITY"] - todays_values["FII_FUTURE_SELL_QUANTITY"]
    futures_index_oi_change = (
        today_futures_index_outstanding_oi - previous_futures_index_outstanding_oi
    )
    futures_index_amount_change = (
        todays_values["FII_FUTURE_BUY_AMOUNT"] - todays_values["FII_FUTURE_SELL_AMOUNT"]
    )

    today_futures_stock_outstanding_oi = (
        todays_values["FII_FUTURE-STOCK_LONG"] - todays_values["FII_FUTURE-STOCK_SHORT"]
    )
    previous_futures_stock_outstanding_oi = (
        previous_values["FII_FUTURE-STOCK_LONG"] - previous_values["FII_FUTURE-STOCK_SHORT"]
    )
    futures_stock_oi_change = (
        today_futures_stock_outstanding_oi - previous_futures_stock_outstanding_oi
    )

    data = {
        "quantity-wise": {
            "outstanding_oi": today_futures_index_outstanding_oi,
            "net_oi": futures_index_oi_change,
        },
        "amount-wise": {
            "net_oi": futures_index_amount_change,
        },
        "futures_outstanding_oi": today_futures_index_outstanding_oi,
        "futures_outstanding_net_oi": futures_index_oi_change,
        "futures_stock_outstanding_oi": today_futures_stock_outstanding_oi,
        "futures_stock_net_oi": futures_stock_oi_change,
    }

    index_wise_data = {}
    index_expirying_today_count = 0
    for nse_name, sensibull_name in NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING_FOR_INDEX.items():
        index_name = nse_name.split()[0].lower()
        expiries = future_expiry_map[index_name.upper()]
        expirying_today = date in expiries
        if expirying_today:
            index_expirying_today_count += 1

        buy_amount_key = f"FII_{sensibull_name}_BUY_AMOUNT"
        sell_amount_key = f"FII_{sensibull_name}_SELL_AMOUNT"
        today_buy_amount = todays_values.get(buy_amount_key, 0)
        today_sell_amount = todays_values.get(sell_amount_key, 0)

        buy_quantity_key = f"FII_{sensibull_name}_BUY_QUANTITY"
        sell_quantity_key = f"FII_{sensibull_name}_SELL_QUANTITY"
        today_buy_quantity = todays_values.get(buy_quantity_key, 0)
        today_sell_quantity = todays_values.get(sell_quantity_key, 0)

        index_wise_data[index_name] = {
            "expirying_today": expirying_today,
            "net_amount": opt_sub(today_buy_amount, today_sell_amount),
            "net_quantity": opt_sub(today_buy_quantity, today_sell_quantity),
        }

    for index_name, index_data in index_wise_data.items():
        index_name_oi_key = index_name + "_net_oi"

        expirying_today = index_data["expirying_today"]

        data["amount-wise"][index_name_oi_key] = 0
        data["quantity-wise"][index_name_oi_key] = 0

        if expirying_today and index_expirying_today_count == 1:
            sum_of_remaining_indices = sum(
                map(
                    lambda x: index_wise_data[x]["net_quantity"],
                    filter(lambda x: x != index_name, index_wise_data.keys()),
                )
            )
            data["quantity-wise"][index_name_oi_key] = (
                data["quantity-wise"]["net_oi"] - sum_of_remaining_indices
            )
        elif expirying_today and index_expirying_today_count > 1:
            # data["amount-wise"][index_name_oi_key] = index_data["net_amount"]
            # data["quantity-wise"][index_name_oi_key] = None
            pass
        else:
            data["amount-wise"][index_name_oi_key] = index_data["net_amount"]
            data["quantity-wise"][index_name_oi_key] = index_data["net_quantity"]

    return data


# For other participants, only quantity wise data is available from fao participants file
def calculate_futures_redis_values_for_other_participants(
    participant, todays_values, previous_values
):
    data = {
        "quantity-wise": {"net_oi": 0, "outstanding_oi": 0},
        "futures_outstanding_oi": 0,
        "futures_outstanding_net_oi": 0,
        "futures_stock_outstanding_oi": 0,
        "futures_stock_net_oi": 0,
    }

    today_futures_outstanding_oi = (
        todays_values[f"{participant}_FUTURE-INDEX_LONG"]
        - todays_values[f"{participant}_FUTURE-INDEX_SHORT"]
    )
    data["futures_outstanding_oi"] = today_futures_outstanding_oi
    previous_futures_outstanding_oi = (
        previous_values[f"{participant}_FUTURE-INDEX_LONG"]
        - previous_values[f"{participant}_FUTURE-INDEX_SHORT"]
    )
    data["futures_outstanding_net_oi"] = (
        today_futures_outstanding_oi - previous_futures_outstanding_oi
    )
    data["quantity-wise"]["outstanding_oi"] = today_futures_outstanding_oi
    data["quantity-wise"]["net_oi"] = today_futures_outstanding_oi - previous_futures_outstanding_oi

    today_futures_stock_oi = (
        todays_values[f"{participant}_FUTURE-STOCK_LONG"]
        - todays_values[f"{participant}_FUTURE-STOCK_SHORT"]
    )
    previous_futures_stock_oi = (
        previous_values[f"{participant}_FUTURE-STOCK_LONG"]
        - previous_values[f"{participant}_FUTURE-STOCK_SHORT"]
    )
    data["futures_stock_outstanding_oi"] = today_futures_stock_oi
    data["futures_stock_net_oi"] = today_futures_stock_oi - previous_futures_stock_oi

    return data


def calculate_options_redis_values(participant, todays_values, previous_values):
    call_long_oi = todays_values[f"{participant}_CALL_LONG"]
    call_long_oi_change = (
        todays_values[f"{participant}_CALL_LONG"] - previous_values[f"{participant}_CALL_LONG"]
    )
    call_short_oi = todays_values[f"{participant}_CALL_SHORT"]
    call_short_oi_change = (
        todays_values[f"{participant}_CALL_SHORT"] - previous_values[f"{participant}_CALL_SHORT"]
    )
    call_net_oi = call_long_oi - call_short_oi

    put_long_oi = todays_values[f"{participant}_PUT_LONG"]
    put_long_oi_change = (
        todays_values[f"{participant}_PUT_LONG"] - previous_values[f"{participant}_PUT_LONG"]
    )
    put_short_oi = todays_values[f"{participant}_PUT_SHORT"]
    put_short_oi_change = (
        todays_values[f"{participant}_PUT_SHORT"] - previous_values[f"{participant}_PUT_SHORT"]
    )
    put_net_oi = put_long_oi - put_short_oi

    data = {
        "call": {
            "long": {"oi_current": call_long_oi, "oi_change": call_long_oi_change},
            "short": {"oi_current": call_short_oi, "oi_change": call_short_oi_change},
            "net_oi": call_net_oi,
            "net_oi_change": 0,
        },
        "put": {
            "long": {"oi_current": put_long_oi, "oi_change": put_long_oi_change},
            "short": {"oi_current": put_short_oi, "oi_change": put_short_oi_change},
            "net_oi": put_net_oi,
            "net_oi_change": 0,
        },
    }

    data["call"]["net_oi_change"] = (
        data["call"]["long"]["oi_change"] - data["call"]["short"]["oi_change"]
    )
    data["put"]["net_oi_change"] = (
        data["put"]["long"]["oi_change"] - data["put"]["short"]["oi_change"]
    )

    data["overall_net_oi"] = data["call"]["net_oi"] - data["put"]["net_oi"]
    data["overall_net_oi_change"] = data["call"]["net_oi_change"] - data["put"]["net_oi_change"]

    return data


# Notion doc for below calcs - [https://www.notion.so/sensibull/Calculations-c3bcfb7a25924728bc53f9c78bae86cb]
def calculate_fii_dii_changes(todays_values, previous_values, future_expiry_map, date):
    """
    :param todays_values: current day's fii & dii data
    :param previous_values: previous day's fii & dii data
    :return: returns values to be stored in redis.
    """

    redis_data = {
        "cash": calculate_cash_redis_values(todays_values),
        "future": {
            "fii": calculate_futures_redis_values_for_fii(
                todays_values, previous_values, future_expiry_map, date
            ),
            "dii": calculate_futures_redis_values_for_other_participants(
                "DII", todays_values, previous_values
            ),
            "pro": calculate_futures_redis_values_for_other_participants(
                "PRO", todays_values, previous_values
            ),
            "client": calculate_futures_redis_values_for_other_participants(
                "CLIENT", todays_values, previous_values
            ),
        },
        "option": {
            "fii": calculate_options_redis_values("FII", todays_values, previous_values),
            "dii": calculate_options_redis_values("DII", todays_values, previous_values),
            "pro": calculate_options_redis_values("PRO", todays_values, previous_values),
            "client": calculate_options_redis_values("CLIENT", todays_values, previous_values),
        },
        "date": str(date),
    }

    return redis_data


def map_fii_dii_data_to_dict(data):
    """
    Convert DB Records to python dict
    :param data:
    :return:
    """
    obj = {
        # cash
        "FII_CASH_BUY": 0,
        "FII_CASH_SELL": 0,
        "DII_CASH_BUY": 0,
        "DII_CASH_SELL": 0,
        # future
        "FII_FUTURE_BUY_AMOUNT": 0,
        "FII_FUTURE_SELL_AMOUNT": 0,
        "FII_FUTURE_BUY_QUANTITY": 0,
        "FII_FUTURE_SELL_QUANTITY": 0,
        # "FII_FUTURE_OI": 0,
        "FII_FUTURE-INDEX_LONG": 0,
        "FII_FUTURE-INDEX_SHORT": 0,
        "DII_FUTURE-INDEX_LONG": 0,
        "DII_FUTURE-INDEX_SHORT": 0,
        "PRO_FUTURE-INDEX_LONG": 0,
        "PRO_FUTURE-INDEX_SHORT": 0,
        "CLIENT_FUTURE-INDEX_LONG": 0,
        "CLIENT_FUTURE-INDEX_SHORT": 0,
        "FII_FUTURE-STOCK_LONG": 0,
        "FII_FUTURE-STOCK_SHORT": 0,
        "DII_FUTURE-STOCK_LONG": 0,
        "DII_FUTURE-STOCK_SHORT": 0,
        "PRO_FUTURE-STOCK_LONG": 0,
        "PRO_FUTURE-STOCK_SHORT": 0,
        "CLIENT_FUTURE-STOCK_LONG": 0,
        "CLIENT_FUTURE-STOCK_SHORT": 0,
        # option
        "FII_CALL_LONG": 0,
        "FII_CALL_SHORT": 0,
        "FII_PUT_LONG": 0,
        "FII_PUT_SHORT": 0,
        "DII_CALL_LONG": 0,
        "DII_CALL_SHORT": 0,
        "DII_PUT_LONG": 0,
        "DII_PUT_SHORT": 0,
        "PRO_CALL_LONG": 0,
        "PRO_CALL_SHORT": 0,
        "PRO_PUT_LONG": 0,
        "PRO_PUT_SHORT": 0,
        "CLIENT_CALL_LONG": 0,
        "CLIENT_CALL_SHORT": 0,
        "CLIENT_PUT_LONG": 0,
        "CLIENT_PUT_SHORT": 0,
    }

    for each_row in data:
        obj[f"{each_row.trading_institution}_{each_row.trade_type}_{each_row.buy_or_sell}"] = (
            each_row.value
        )

    return obj


def map_fii_dii_cash_data_to_dict(data):
    """
    Convert DB Records to python dict
    :param data:
    :return:
    """
    obj = {
        # cash
        "FII_CASH_BUY": 0,
        "FII_CASH_SELL": 0,
        "DII_CASH_BUY": 0,
        "DII_CASH_SELL": 0,
    }

    for each_row in data:
        obj[f"{each_row.trading_institution}_{each_row.trade_type}_{each_row.buy_or_sell}"] = (
            each_row.value
        )

    return obj


async def get_nifty_and_banknifty_ohlc(context, start_date, end_date):
    """
    :param start_date: from date for fetching nifty ohlc
    :param end_date: to date for fetching nifty ohlc
    :return:
    """
    historical_quotes = await fetch_day_wise_historical_quotes(
        context, ["NIFTY", "BANKNIFTY"], start_date, end_date
    )
    return historical_quotes


def calculate_close_change_from_ohlc(data):
    """
    :param data: array of ohlc data fetched from nse table
    :return: array of ohlc data with close change percent sorted by date
    """
    sorted_ohlc_data = sorted(data, key=lambda d: d["date"])
    if len(sorted_ohlc_data) > 0:
        sorted_ohlc_data[0]["close_change"] = None
        previous = sorted_ohlc_data[0]
        for each in sorted_ohlc_data[1:]:
            previous_close = previous.get("close")
            current_close = each.get("close")
            each["close_change"] = 0
            if previous_close != 0:
                each["close_change"] = (
                    (float(current_close) - float(previous_close)) / float(previous_close)
                ) * 100
            previous = each
    return sorted_ohlc_data


async def generate_and_store_complete_fii_dii_data_to_redis(context, skip_download):
    """
    Main function which fetches missing data from NSE reports and stores in DB, then creates cache and stores in redis
    :param skip_download: boolean var to determine whether download to DB is required
    :return:
    """
    try:
        logger = get_custom_logger_for_tasks("generate_and_store_complete_fii_dii_data_to_redis")

        today = datetime.strptime(str(get_last_trading_day(for_bhav_copy=True)), "%Y-%m-%d")
        # Set from_date to approx 6 months back
        from_date = (today - timedelta(days=HISTORICAL_RANGE_IN_DAYS)).replace(day=1)
        if skip_download:
            # if true, then download only today's data and proceed
            # done so that job has min data required to pass
            from_date = today
        logger.info(f"Generating FII DII data cache from: {from_date}")

        # Check and update missing data to DB
        logger.info("Fetching and updating FII DII data to DB")
        await fetch_and_store_historical_fii_dii_data(context, from_date, today, today)
        logger.info("Successfully updated FII DII data in DB")

        # Map out date wise data and get list of redis year-month keys
        fii_dii_data_per_date = {}
        year_month_set = set()
        day_before_from_date = get_previous_working_day(
            from_date
        )  # required to calc change values for from_date
        fetched_data_from_db = await FiiDiiModel.get_records_with_date_range(
            day_before_from_date, today
        )

        future_expiry_map = await NSEDataModel.get_underlying_future_expiries(
            ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "NIFTYNXT50"],
            day_before_from_date,
            today,
        )

        for data in fetched_data_from_db:
            traded_day = data.traded_day
            if str(traded_day) not in fii_dii_data_per_date:
                fii_dii_data_per_date[str(traded_day)] = []
            fii_dii_data_per_date[str(traded_day)].append(data)
            # get redis month key
            traded_day = datetime(year=traded_day.year, month=traded_day.month, day=traded_day.day)
            if traded_day >= from_date:
                redis_year_month_key = datetime.strftime(traded_day, "%Y-%B")
                year_month_set.add(redis_year_month_key)
        redis_key_list = list(year_month_set)
        redis_key_list.sort(key=lambda d: datetime.strptime(d, "%Y-%B"))
        # take last month as latest
        latest_month = ""
        if len(redis_key_list) > 0:
            latest_month = redis_key_list[-1]
        else:
            print("No fii dii data found in DB")
            raise Exception("Empty key list. No fii dii data found in DB")

        # Fetch NIFTY and BANKNIFTY data
        day_before_start_date = get_previous_working_day(
            from_date
        )  # required to calculate change %
        nifty_and_bank_ohlc = await get_nifty_and_banknifty_ohlc(
            context, day_before_start_date, today
        )
        nifty_ohlc = nifty_and_bank_ohlc.get("NIFTY")
        nifty_per_date_ohlc = {}
        if nifty_ohlc:
            # calculate nifty close change percent
            data = calculate_close_change_from_ohlc(nifty_ohlc)
            for each in data:
                nifty_per_date_ohlc[str(each["date"])] = each
        else:
            raise Exception(
                f"Could not get OHLC values for NIFTY from: {day_before_start_date} to: {today}"
            )

        banknifty_ohlc = nifty_and_bank_ohlc.get("BANKNIFTY")
        banknifty_per_date_ohlc = {}
        if banknifty_ohlc:
            # calculate bank nifty close change percent
            data = calculate_close_change_from_ohlc(banknifty_ohlc)
            for each in data:
                banknifty_per_date_ohlc[str(each["date"])] = each
        else:
            raise Exception(
                f"Could not get OHLC values for BANKNIFTY from: {day_before_start_date} to: {today}"
            )

        per_day_data = calculate_fii_dii_data_cache(
            from_date,
            today,
            fii_dii_data_per_date,
            nifty_per_date_ohlc,
            banknifty_per_date_ohlc,
            future_expiry_map,
        )

        redis_data_per_month = {}
        # Fetch month wise data by setting start_date to first day of month and end_date to last working day of month
        # In case of current month, store 30 days back
        while from_date <= today:
            # Key eg: 2023-July
            start_date = from_date
            redis_key = datetime.strftime(start_date, "%Y-%B")
            end_date = get_previous_working_day(get_start_of_next_month(start_date))
            if end_date > today:
                # this happens when iterating over current month and today is before month end
                # Store 30 days back
                start_date = get_previous_working_day(today - timedelta(days=29))
                end_date = today

            logger.info(
                f"Calculating FII DII cache for: {redis_key}, start_date: {start_date}, end_date: {end_date}"
            )

            redis_data_per_day = {}
            for k, v in per_day_data.items():
                if k >= start_date.date() and k <= end_date.date():
                    redis_data_per_day[str(k)] = v
            redis_month_object = {
                "year_month": redis_key,
                "key_list": redis_key_list,
                "data": redis_data_per_day,
            }
            gzipped_data = convert_to_gzip_format(redis_month_object)
            redis_data_per_month[redis_key] = gzipped_data
            # Move to next month
            from_date = get_start_of_next_month(from_date)

        latest = redis_data_per_month.get(latest_month)
        if latest:
            redis_data_per_month[FII_DII_DAILY_LATEST_MONTH_REDIS_KEY] = latest
        else:
            print(f"Latest month: {latest_month} not found. Can't set default")
            raise Exception(f"Latest month: {latest_month} not found. Can't set default")

        await FiiDiiModel.set_fii_dii_to_redis_v2(context, redis_data_per_month)
        logger.info(f"Successfully generated FII DII cache for: {redis_key_list}")

    except Exception as exp:
        raise exp


async def generate_and_store_fii_dii_monthly_cash_data_to_redis(context):
    try:
        logger = get_custom_logger_for_tasks(
            "generate_and_store_fii_dii_monthly_cash_data_to_redis"
        )

        today = datetime.strptime(str(get_last_trading_day(for_bhav_copy=True)), "%Y-%m-%d")
        # start cache from Sept 2018
        start_date = datetime(2018, 9, 1)

        # Map out date wise data
        cash_data_per_date = {}
        fetched_data_from_db = await FiiDiiModel.get_cash_records_with_date_range(start_date, today)
        for data in fetched_data_from_db:
            traded_day = data.traded_day
            if str(traded_day) not in cash_data_per_date:
                cash_data_per_date[str(traded_day)] = []
            cash_data_per_date[str(traded_day)].append(data)

        # Fetch NIFTY data
        day_before_start_date = get_previous_working_day(
            start_date
        )  # required to calculate change %
        nifty_and_bank_ohlc = await get_nifty_and_banknifty_ohlc(
            context, day_before_start_date, today
        )
        nifty_ohlc = nifty_and_bank_ohlc.get("NIFTY")
        nifty_per_date_ohlc = {}
        if nifty_ohlc:
            # calculate nifty close change percent
            data = calculate_close_change_from_ohlc(nifty_ohlc)
            for each in data:
                nifty_per_date_ohlc[str(each["date"])] = each
        else:
            raise Exception(
                f"Could not get OHLC values for NIFTY from: {day_before_start_date} to: {today}"
            )

        monthly_cash_data = {}
        # Fetch month wise data by setting start_date to first day of month and end_date to last working day of month
        while start_date <= today:
            end_date = get_previous_working_day(get_start_of_next_month(start_date))
            if end_date > today:
                # this happens when iterating over current month and today is before month end
                end_date = today
            logger.info(
                f"Calculating FII DII monthly cash data for start_date: {start_date}, end_date: {end_date}"
            )
            aggregate_cash_data_for_month = calculate_fii_dii_monthly_cash_data(
                start_date, end_date, cash_data_per_date, nifty_per_date_ohlc
            )
            monthly_cash_data[str(start_date.date())] = aggregate_cash_data_for_month
            # Move to next month
            start_date = get_start_of_next_month(start_date)


        # nifty change percent needs to be calculated w.r.t to previous month close
        previous_nifty_close = 0
        for _, data in monthly_cash_data.items():
            current_nifty_close = data["nifty"]
            if previous_nifty_close != 0:
                data["nifty_change_percent"] = (
                        (float(current_nifty_close) - float(previous_nifty_close)) / float(previous_nifty_close)
                                ) * 100
            previous_nifty_close = current_nifty_close

        # gzip and store in redis
        gzipped_data = convert_to_gzip_format(monthly_cash_data)
        await set_fii_monthly_data_to_redis(context, gzipped_data)

        logger.info(f"Successfully generated FII DII monthly cash data cache ")

    except Exception as exp:
        raise exp


def calculate_fii_dii_monthly_cash_data(
    start_date: datetime, end_date: datetime, cash_data_per_date, nifty_per_date_ohlc
):
    start_date_str = str(start_date.date())
    fii_buy_total = 0
    fii_sell_total = 0
    dii_buy_total = 0
    dii_sell_total = 0
    while start_date <= end_date:
        if is_market_day(start_date):
            current_cash_data = cash_data_per_date.get(str(start_date.date()), [])
            # Map to value objects
            current_values = map_fii_dii_cash_data_to_dict(current_cash_data)
            fii_buy_total += current_values["FII_CASH_BUY"]
            fii_sell_total += current_values["FII_CASH_SELL"]
            dii_buy_total += current_values["DII_CASH_BUY"]
            dii_sell_total += current_values["DII_CASH_SELL"]

        start_date = start_date + timedelta(days=1)

    fii_net = fii_buy_total - fii_sell_total
    dii_net = dii_buy_total - dii_sell_total

    cash_data_for_month = {
        "date": start_date_str,  # 1st day of month
        "fii_buy": fii_buy_total,
        "fii_sell": fii_sell_total,
        "fii_net": fii_net,
        "dii_buy": dii_buy_total,
        "dii_sell": dii_sell_total,
        "dii_net": dii_net,
        "nifty": 0,
        "nifty_change_percent": 0.0,
    }

    # Add nifty close for the month
    nifty_values = nifty_per_date_ohlc.get(str(end_date.date()))
    if nifty_values:
        cash_data_for_month["nifty"] = nifty_values.get("close")

    return cash_data_for_month


def get_fii_dii_monthly_data_from_google_sheet():
    """
    :return: Fetches and returns cash market data from google sheet
    """
    fii_dii_sheet = constants.GOOGLE_SHEETS["fii_dii_monthly"]
    return get_google_sheet_data(fii_dii_sheet, "Monthly!A1:G120")


async def set_fii_monthly_data_to_redis(context, data):
    """
    :param data: gzipped fii dii cash Monthly Data
    :return:
    """
    await context.redis.set(FII_DII_CASH_REDIS_KEY, data, ex=FII_DII_CASH_TTL)


async def get_fii_monthly_data_from_redis():
    """
    :return: Monthly FII Cash market data from redis
    """
    context = app_context.get()
    data = await context.redis.get(FII_DII_CASH_REDIS_KEY)

    return data


async def load_monthly_fii_dii_cash_data_to_redis(
    context,
):
    """
    This function is used to fetch cash market data from google sheet and store them to csv
    :return:
    """
    logger = get_custom_logger_for_tasks("load_fii_dii_cash_data_to_redis")
    fii_dii_monthly_cash_data = get_fii_dii_monthly_data_from_google_sheet()

    data_per_month = {}
    for data in fii_dii_monthly_cash_data:
        # This logging shows the fii dii cash data pulled from excel sheet
        logger.debug(data)
        date_string = data["date"]
        data_per_month[date_string] = data
    # gzip and store in redis
    gzipped_data = convert_to_gzip_format(data_per_month)
    await set_fii_monthly_data_to_redis(context, gzipped_data)


async def fetch_and_store_historical_fii_dii_data(context, min_date, max_date, current_date):
    """
    :param min_date: Starting date from which historical data is available
    :param max_date: Ending date up to which historical data needs to be fetched
    :param current_date: Current date
    :return:
    """

    logger = get_custom_logger_for_tasks("fetch_and_store_historical_fii_dii_data")
    iteration_date = min_date

    while iteration_date <= max_date:
        if is_market_day(iteration_date):
            try:
                previous_day = get_previous_working_day(iteration_date)
                previous_fii_dii_data = await fetch_fii_dii_data(
                    previous_day, previous_day == current_date
                )
                current_fii_dii_data = await fetch_fii_dii_data(
                    iteration_date, iteration_date == current_date
                )
                complete_fii_dii_data = previous_fii_dii_data + current_fii_dii_data
                # Insert Data into DB
                for each_data in complete_fii_dii_data:
                    await FiiDiiModel.create_fii_dii_record(**each_data)

                logger.debug(f"SUCCESS {iteration_date}")
            except Exception as x:
                logger.debug(f"FAILED {iteration_date} : {x}")
                raise Exception(
                    f"Fetch historical fii dii data failed for {iteration_date} with error: {x}"
                )
        iteration_date = iteration_date + timedelta(days=1)


def calculate_fii_dii_data_cache(
    start_date: datetime,
    end_date: datetime,
    fii_dii_data_per_date,
    nifty_per_date_ohlc,
    banknifty_per_date_ohlc,
    future_expiry_map,
):
    """
    Calculates FII DII DATA to be stored in redis for passed dates
    :param context:
    :param start_date: start date for month
    :param end_date: end date for month
    :param fii_dii_data_per_date: map of fii dii records to date
    :param redis_key: redis key name
    :param redis_key_list: list of redis keys stored in cache
    :param nifty_per_date_ohlc: nifty ohlc per date
    :param banknifty_per_date_ohlc: banknifty ohlc per date
    :return: gzipped redis data
    """
    try:
        per_day_data = {}

        while start_date <= end_date:
            if is_market_day(start_date):
                previous_day = get_previous_working_day(start_date)
                previous_fii_dii_data = fii_dii_data_per_date.get(str(previous_day.date()), [])
                current_fii_dii_data = fii_dii_data_per_date.get(str(start_date.date()), [])
                # Map to value objects
                current_values = map_fii_dii_data_to_dict(current_fii_dii_data)
                previous_values = map_fii_dii_data_to_dict(previous_fii_dii_data)
                # Create per day redis values
                computed_data_for_date = calculate_fii_dii_changes(
                    current_values, previous_values, future_expiry_map, start_date.date()
                )

                # Add nifty values
                computed_data_for_date["nifty"] = 0
                computed_data_for_date["nifty_change_percent"] = 0
                nifty_values = nifty_per_date_ohlc.get(str(start_date.date()))
                if nifty_values:
                    computed_data_for_date["nifty"] = nifty_values["close"]
                    computed_data_for_date["nifty_change_percent"] = nifty_values["close_change"]

                # Add bank nifty values
                computed_data_for_date["banknifty"] = 0
                computed_data_for_date["banknifty_change_percent"] = 0
                banknifty_values = banknifty_per_date_ohlc.get(str(start_date.date()))
                if banknifty_values:
                    computed_data_for_date["banknifty"] = banknifty_values["close"]
                    computed_data_for_date["banknifty_change_percent"] = banknifty_values[
                        "close_change"
                    ]

                computed_data_for_date["next_market_open"] = localize_with_timezone(
                    datetime.combine(get_next_working_day(start_date), get_market_open())
                )
                per_day_data[start_date.date()] = computed_data_for_date

            start_date = start_date + timedelta(days=1)

        augment_indicators(per_day_data)

        return per_day_data
    except Exception as expt:
        raise expt


# Link to Notion doc for view strength calc
# https://www.notion.so/sensibull/Calculations-c3bcfb7a25924728bc53f9c78bae86cb
# https://www.notion.so/sensibull/FII-Summary-Strength-Calculation-Logic-New-6e665e64df394d5c8bb4fc346f7c822c
# Typesafety has left the chat >:(
def augment_indicators(per_day_data):
    index_futures_oi_outstanding_vals = []
    index_futures_oi_change_vals = []
    stock_futures_oi_outstanding_vals = []
    stock_futures_oi_change_vals = []
    cash_vals = []
    index_futures_per_index_vals = []
    index_options_oi_change_vals = []
    index_call_options_oi_vals = []
    index_call_options_oi_change_vals = []
    index_put_options_oi_vals = []
    index_put_options_oi_change_vals = []

    last_sixty_days_of_data = list(per_day_data.items())[-60:]
    for dt, data in last_sixty_days_of_data:
        x = data["future"]["fii"]["quantity-wise"]
        opt_push(index_futures_per_index_vals, opt_abs(x["nifty_net_oi"]))
        opt_push(index_futures_per_index_vals, opt_abs(x["banknifty_net_oi"]))
        # index_futures_per_index_vals.append(abs(x["midcpnifty_net_oi"]))
        # index_futures_per_index_vals.append(abs(x["finnifty_net_oi"]))
        for participant, future_data in data["future"].items():
            if participant == "dii":
                continue
            opt_push(
                index_futures_oi_outstanding_vals, opt_abs(future_data["futures_outstanding_oi"])
            )
            opt_push(
                index_futures_oi_change_vals, opt_abs(future_data["futures_outstanding_net_oi"])
            )
            opt_push(
                stock_futures_oi_outstanding_vals,
                opt_abs(future_data["futures_stock_outstanding_oi"]),
            )
            opt_push(stock_futures_oi_change_vals, opt_abs(future_data["futures_stock_net_oi"]))
        for participant, cash_data in data["cash"].items():
            if participant == "dii":
                continue
            opt_push(cash_vals, opt_abs(cash_data["buy_sell_difference"]))
        for participant, option_data in data["option"].items():
            if participant == "dii":
                continue
            opt_push(index_options_oi_change_vals, opt_abs(option_data["overall_net_oi_change"]))
            opt_push(index_call_options_oi_vals, opt_abs(option_data["call"]["net_oi"]))
            opt_push(
                index_call_options_oi_change_vals, opt_abs(option_data["call"]["net_oi_change"])
            )
            opt_push(index_put_options_oi_vals, opt_abs(option_data["put"]["net_oi"]))
            opt_push(index_put_options_oi_change_vals, opt_abs(option_data["put"]["net_oi_change"]))

    index_futures_oi_outstanding_vals.sort()
    index_futures_oi_change_vals.sort()
    stock_futures_oi_outstanding_vals.sort()
    stock_futures_oi_change_vals.sort()
    cash_vals.sort()
    index_futures_per_index_vals.sort()
    index_options_oi_change_vals.sort()
    index_call_options_oi_vals.sort()
    index_call_options_oi_change_vals.sort()
    index_put_options_oi_vals.sort()
    index_put_options_oi_change_vals.sort()

    # update strength in data
    for dt, data in per_day_data.items():
        for nse_name, sensibull_name in NSE_FILE_NAME_TO_SENSIBULL_NAME_MAPPING_FOR_INDEX.items():
            index_name = nse_name.split()[0].lower()
            index_name_oi_key = index_name + "_net_oi"
            index_name_view_key = index_name + "_net_view"
            index_name_view_strength_key = index_name + "_net_view_strength"
            index_name_action_key = index_name + "_net_action"
            future_data = data["future"]["fii"]

            future_data["quantity-wise"][index_name_action_key] = get_action_for_net_value(
                future_data["quantity-wise"][index_name_oi_key]
            )
            future_data["quantity-wise"][index_name_view_key] = get_view_for_net_value(
                future_data["quantity-wise"][index_name_oi_key]
            )
            future_data["quantity-wise"][index_name_view_strength_key] = get_view_strength(
                future_data["quantity-wise"][index_name_oi_key],
                index_futures_per_index_vals,
                20,
                80,
            )
            future_data["amount-wise"][index_name_view_key] = get_view_for_net_value(
                future_data["amount-wise"][index_name_oi_key]
            )

        for participant, future_data in data["future"].items():
            future_data["quantity-wise"]["net_action"] = get_action_for_net_value(
                future_data["quantity-wise"]["net_oi"],
            )
            if "amount-wise" in future_data:
                future_data["amount-wise"]["net_view"] = get_view_for_net_value(
                    future_data["amount-wise"]["net_oi"]
                )
            future_data["futures_stock_net_action"] = get_action_for_net_value(
                future_data["futures_stock_net_oi"]
            )

            oi = future_data["quantity-wise"]["outstanding_oi"]
            oi_view = get_view_for_net_value(oi)
            oi_strength = get_view_strength_for_oi(oi, index_futures_oi_outstanding_vals)
            oi_change = future_data["quantity-wise"]["net_oi"]
            oi_change_view = get_view_for_net_value(oi_change)
            oi_change_strength = get_view_strength_for_oi_change(
                oi_change, index_futures_oi_change_vals
            )
            strength, view = get_strength_view_from_oi_and_oi_change_strength(
                oi_view, oi_strength, oi_change_view, oi_change_strength
            )
            future_data["quantity-wise"]["net_view_summary"] = view
            future_data["quantity-wise"]["net_view_strength_summary"] = strength
            future_data["quantity-wise"]["net_view"] = oi_change_view
            future_data["quantity-wise"]["net_view_strength"] = oi_change_strength

            oi = future_data["futures_stock_outstanding_oi"]
            oi_view = get_view_for_net_value(oi)
            oi_strength = get_view_strength_for_oi(oi, stock_futures_oi_outstanding_vals)
            oi_change = future_data["futures_stock_net_oi"]
            oi_change_view = get_view_for_net_value(oi_change)
            oi_change_strength = get_view_strength_for_oi_change(
                oi_change, stock_futures_oi_change_vals
            )
            strength, view = get_strength_view_from_oi_and_oi_change_strength(
                oi_view, oi_strength, oi_change_view, oi_change_strength
            )
            future_data["futures_stock_net_view_summary"] = view
            future_data["futures_stock_net_view_strength_summary"] = strength
            future_data["futures_stock_net_view"] = oi_change_view
            future_data["futures_stock_net_view_strength"] = oi_change_strength

        for participant, cash_data in data["cash"].items():
            cash_data["net_action"] = get_action_for_net_value(cash_data["buy_sell_difference"])
            cash_data["net_view"] = get_view_for_net_value(cash_data["buy_sell_difference"])
            cash_data["net_view_strength"] = get_view_strength_old(
                cash_data["buy_sell_difference"], cash_vals
            )

        for participant, option_data in data["option"].items():
            call_option_data = option_data["call"]
            put_option_data = option_data["put"]

            call_option_data["net_oi_change_action"] = get_action_for_net_value(
                call_option_data["net_oi_change"]
            )
            put_option_data["net_oi_change_action"] = get_action_for_net_value(
                put_option_data["net_oi_change"]
            )
            option_data["overall_net_oi_change_action"] = get_action_for_net_value(
                option_data["overall_net_oi_change"]
            )

            call_oi = call_option_data["net_oi"]
            call_oi_view = get_view_for_net_value(call_oi)
            call_oi_strength = get_view_strength_for_oi(call_oi, index_call_options_oi_vals)
            call_oi_change = call_option_data["net_oi_change"]
            call_oi_change_view = get_view_for_net_value(call_oi_change)
            call_oi_change_strength = get_view_strength_for_oi_change(
                call_oi_change, index_call_options_oi_change_vals
            )
            call_strength, call_view = get_strength_view_from_oi_and_oi_change_strength(
                call_oi_view, call_oi_strength, call_oi_change_view, call_oi_change_strength
            )
            call_option_data["net_oi_change_view_summary"] = call_view
            call_option_data["net_oi_change_view_summary_strength"] = call_strength
            call_option_data["net_oi_change_view"] = call_oi_change_view
            call_option_data["net_oi_change_view_strength"] = call_oi_change_strength

            put_oi = put_option_data["net_oi"]
            put_oi_view = get_view_for_put_option(put_oi)
            put_oi_strength = get_view_strength_for_oi(put_oi, index_put_options_oi_vals)
            put_oi_change = put_option_data["net_oi_change"]
            put_oi_change_view = get_view_for_put_option(put_oi_change)
            put_oi_change_strength = get_view_strength_for_oi_change(
                put_oi_change, index_put_options_oi_change_vals
            )
            put_strength, put_view = get_strength_view_from_oi_and_oi_change_strength(
                put_oi_view, put_oi_strength, put_oi_change_view, put_oi_change_strength
            )
            put_option_data["net_oi_change_view_summary"] = put_view
            put_option_data["net_oi_change_view_summary_strength"] = put_strength
            put_option_data["net_oi_change_view"] = put_oi_change_view
            put_option_data["net_oi_change_view_strength"] = put_oi_change_strength

            strength, view = get_net_strength_view_from_call_put_values(
                call_strength, call_view, put_strength, put_view
            )
            option_data["overall_net_oi_change_view_summary"] = view
            option_data["overall_net_oi_change_view_summary_strength"] = strength
            option_data["overall_net_oi_change_view"] = get_view_for_net_value(
                option_data["overall_net_oi_change"]
            )
            option_data["overall_net_oi_change_view_strength"] = get_view_strength_for_oi(
                option_data["overall_net_oi_change"], index_options_oi_change_vals
            )


def get_net_strength_view_from_call_put_values(call_strength, call_view, put_strength, put_view):
    mapping = {
        ((MEDIUM, BULLISH), (INDECISIVE, INDECISIVE)): (MEDIUM, BULLISH),
        ((MEDIUM, BULLISH), (MEDIUM, BEARISH)): (INDECISIVE, VOLATILE),
        ((MEDIUM, BULLISH), (MEDIUM, BULLISH)): (MEDIUM, BULLISH),
        ((MEDIUM, BULLISH), (STRONG, BEARISH)): (MEDIUM, BEARISH),
        ((MEDIUM, BULLISH), (STRONG, BULLISH)): (STRONG, BULLISH),
        ((MEDIUM, BULLISH), (MILD, BEARISH)): (MEDIUM, BULLISH),
        ((MEDIUM, BULLISH), (MILD, BULLISH)): (MEDIUM, BULLISH),
        ((MEDIUM, BEARISH), (INDECISIVE, INDECISIVE)): (MEDIUM, BEARISH),
        ((MEDIUM, BEARISH), (INDECISIVE, INDECISIVE)): (MEDIUM, BEARISH),
        ((MEDIUM, BEARISH), (MEDIUM, BEARISH)): (MEDIUM, BEARISH),
        ((MEDIUM, BEARISH), (MEDIUM, BULLISH)): (INDECISIVE, NEUTRAL),
        ((MEDIUM, BEARISH), (STRONG, BEARISH)): (STRONG, BEARISH),
        ((MEDIUM, BEARISH), (STRONG, BULLISH)): (STRONG, BULLISH),
        ((MEDIUM, BEARISH), (MILD, BEARISH)): (MEDIUM, BEARISH),
        ((MEDIUM, BEARISH), (MILD, BULLISH)): (MEDIUM, BEARISH),
        ((MEDIUM, BULLISH), (INDECISIVE, INDECISIVE)): (MEDIUM, BULLISH),
        ((STRONG, BULLISH), (INDECISIVE, INDECISIVE)): (STRONG, BULLISH),
        ((STRONG, BULLISH), (MEDIUM, BEARISH)): (MEDIUM, BULLISH),
        ((STRONG, BULLISH), (MEDIUM, BULLISH)): (STRONG, BULLISH),
        ((STRONG, BULLISH), (STRONG, BEARISH)): (INDECISIVE, VOLATILE),
        ((STRONG, BULLISH), (STRONG, BULLISH)): (STRONG, BULLISH),
        ((STRONG, BULLISH), (MILD, BEARISH)): (STRONG, BULLISH),
        ((STRONG, BULLISH), (MILD, BULLISH)): (STRONG, BULLISH),
        ((STRONG, BEARISH), (INDECISIVE, INDECISIVE)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (INDECISIVE, INDECISIVE)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (MEDIUM, BEARISH)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (MEDIUM, BULLISH)): (MEDIUM, BEARISH),
        ((STRONG, BEARISH), (STRONG, BEARISH)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (STRONG, BULLISH)): (INDECISIVE, NEUTRAL),
        ((STRONG, BEARISH), (MILD, BEARISH)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (MILD, BULLISH)): (STRONG, BEARISH),
        ((STRONG, BULLISH), (INDECISIVE, INDECISIVE)): (STRONG, BULLISH),
        ((MILD, BULLISH), (INDECISIVE, INDECISIVE)): (INDECISIVE, INDECISIVE),
        ((MILD, BULLISH), (MEDIUM, BEARISH)): (MILD, BEARISH),
        ((MILD, BULLISH), (MEDIUM, BULLISH)): (MEDIUM, BULLISH),
        ((MILD, BULLISH), (STRONG, BEARISH)): (STRONG, BEARISH),
        ((MILD, BULLISH), (STRONG, BULLISH)): (STRONG, BULLISH),
        ((MILD, BULLISH), (MILD, BEARISH)): (INDECISIVE, INDECISIVE),
        ((MILD, BULLISH), (MILD, BULLISH)): (MILD, BULLISH),
        ((MILD, BEARISH), (INDECISIVE, INDECISIVE)): (INDECISIVE, INDECISIVE),
        ((MILD, BEARISH), (INDECISIVE, INDECISIVE)): (INDECISIVE, INDECISIVE),
        ((MILD, BEARISH), (MEDIUM, BEARISH)): (MEDIUM, BEARISH),
        ((MILD, BEARISH), (MEDIUM, BULLISH)): (MEDIUM, BULLISH),
        ((MILD, BEARISH), (STRONG, BEARISH)): (STRONG, BEARISH),
        ((MILD, BEARISH), (STRONG, BULLISH)): (STRONG, BULLISH),
        ((MILD, BEARISH), (MILD, BEARISH)): (INDECISIVE, INDECISIVE),
        ((MILD, BEARISH), (MILD, BULLISH)): (INDECISIVE, INDECISIVE),
        ((MILD, BULLISH), (INDECISIVE, INDECISIVE)): (INDECISIVE, INDECISIVE),
    }
    strength, view = mapping.get(
        ((call_strength, call_view), (put_strength, put_view)), (INDECISIVE, INDECISIVE)
    )
    return strength, view


def get_strength_view_from_oi_and_oi_change_strength(
    oi_view, oi_strength, oi_change_view, oi_change_strength
):
    mapping = {
        ((STRONG, BULLISH), (STRONG, BULLISH)): (STRONG, BULLISH),
        ((STRONG, BULLISH), (MEDIUM, BULLISH)): (STRONG, BULLISH),
        ((STRONG, BULLISH), (MILD, BULLISH)): (STRONG, BULLISH),
        ((STRONG, BULLISH), (STRONG, BEARISH)): (INDECISIVE, INDECISIVE),
        ((STRONG, BULLISH), (MEDIUM, BEARISH)): (INDECISIVE, INDECISIVE),
        ((STRONG, BULLISH), (MILD, BEARISH)): (STRONG, BULLISH),
        ((MEDIUM, BULLISH), (STRONG, BULLISH)): (STRONG, BULLISH),
        ((MEDIUM, BULLISH), (MEDIUM, BULLISH)): (MEDIUM, BULLISH),
        ((MEDIUM, BULLISH), (MILD, BULLISH)): (MEDIUM, BULLISH),
        ((MEDIUM, BULLISH), (STRONG, BEARISH)): (INDECISIVE, INDECISIVE),
        ((MEDIUM, BULLISH), (MEDIUM, BEARISH)): (INDECISIVE, INDECISIVE),
        ((MEDIUM, BULLISH), (MILD, BEARISH)): (MEDIUM, BULLISH),
        ((MILD, BULLISH), (STRONG, BULLISH)): (MEDIUM, BULLISH),
        ((MILD, BULLISH), (MEDIUM, BULLISH)): (MILD, BULLISH),
        ((MILD, BULLISH), (MILD, BULLISH)): (MILD, BULLISH),
        ((MILD, BULLISH), (STRONG, BEARISH)): (MEDIUM, BEARISH),
        ((MILD, BULLISH), (MEDIUM, BEARISH)): (INDECISIVE, INDECISIVE),
        ((MILD, BULLISH), (MILD, BEARISH)): (INDECISIVE, INDECISIVE),
        ((STRONG, BEARISH), (STRONG, BULLISH)): (INDECISIVE, INDECISIVE),
        ((STRONG, BEARISH), (MEDIUM, BULLISH)): (INDECISIVE, INDECISIVE),
        ((STRONG, BEARISH), (MILD, BULLISH)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (STRONG, BEARISH)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (MEDIUM, BEARISH)): (STRONG, BEARISH),
        ((STRONG, BEARISH), (MILD, BEARISH)): (STRONG, BEARISH),
        ((MEDIUM, BEARISH), (STRONG, BULLISH)): (INDECISIVE, INDECISIVE),
        ((MEDIUM, BEARISH), (MEDIUM, BULLISH)): (INDECISIVE, INDECISIVE),
        ((MEDIUM, BEARISH), (MILD, BULLISH)): (MEDIUM, BEARISH),
        ((MEDIUM, BEARISH), (STRONG, BEARISH)): (STRONG, BEARISH),
        ((MEDIUM, BEARISH), (MEDIUM, BEARISH)): (MEDIUM, BEARISH),
        ((MEDIUM, BEARISH), (MILD, BEARISH)): (MEDIUM, BEARISH),
        ((MILD, BEARISH), (STRONG, BULLISH)): (MEDIUM, BULLISH),
        ((MILD, BEARISH), (MEDIUM, BULLISH)): (INDECISIVE, INDECISIVE),
        ((MILD, BEARISH), (MILD, BULLISH)): (INDECISIVE, INDECISIVE),
        ((MILD, BEARISH), (STRONG, BEARISH)): (MEDIUM, BEARISH),
        ((MILD, BEARISH), (MEDIUM, BEARISH)): (MILD, BEARISH),
        ((MILD, BEARISH), (MILD, BEARISH)): (MILD, BEARISH),
    }
    strength, view = mapping.get(
        ((oi_strength, oi_view), (oi_change_strength, oi_change_view)), (INDECISIVE, INDECISIVE)
    )
    return strength, view


def get_threshold(percentile, sorted_list):
    t_pos = (percentile / 100) * (len(sorted_list) - 1)
    t_pos_int, t_pos_frac = divmod(t_pos, 1)
    t_pos_int = int(t_pos_int)
    if t_pos_frac == 0:
        return sorted_list[t_pos_int]
    else:
        return sorted_list[t_pos_int] + (
            t_pos_frac * (sorted_list[t_pos_int + 1] - sorted_list[t_pos_int])
        )


def get_view_strength_for_oi(value, sorted_list):
    return get_view_strength(value, sorted_list, 40, 80)


def get_view_strength_for_oi_change(value, sorted_list):
    return get_view_strength(value, sorted_list, 20, 80)


def get_view_strength_old(value, sorted_list):
    return get_view_strength(value, sorted_list, 33, 66)


def get_view_strength(value, sorted_list, medium_threshold, strong_threshold):
    sorted_list_len = len(sorted_list)
    if sorted_list_len == 0:
        return INDECISIVE

    one_third_threshold = get_threshold(medium_threshold, sorted_list)
    two_third_threshold = get_threshold(strong_threshold, sorted_list)

    value = opt_abs(value)
    if value is None:
        return INDECISIVE
    if value >= two_third_threshold:
        return STRONG
    if value >= one_third_threshold:
        return MEDIUM
    return MILD


# Gets the start of next month from passed date
def get_start_of_next_month(input_dt: datetime):
    out = (input_dt.replace(day=1) + timedelta(days=32)).replace(day=1)
    return out


def opt_add(a, b):
    if a is None:
        return None
    if b is None:
        return None
    return a + b


def opt_sub(a, b):
    if a is None:
        return None
    if b is None:
        return None
    return a - b


def opt_push(l, v):
    if v is None:
        return
    l.append(v)


def opt_abs(v):
    if v is None:
        return None
    return abs(v)
