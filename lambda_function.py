import asyncio
import boto3
from botocore.exceptions import ClientError
import ccxt.async_support as ccxta
from datetime import datetime, timedelta
from dateutil.parser import parse
from exceptions import GetExecutionsError, PutS3Error
import json
import logger
import os
from pprint import pprint
from pytz import timezone
import requests
import time
from tzlocal import get_localzone

bucket_name = "bitflyer-executions"
date_format = "%Y/%m/%d %H:%M"
discord_post_url = os.environ["DISCORD_POST_URL"]
local_zone = get_localzone()
log = logger.Logger(__name__)


def now():
    dt = datetime.now(local_zone).strftime(date_format)
    return dt


def post_to_discord(message):

    post_data = {
        "content": message
    }

    try:
        response = requests.post(discord_post_url, data=json.dumps(post_data),
                                 headers={'Content-Type': "application/json"})
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f'Request failed: {e}')


async def public_get_trade_async(symbol, from_=None, to=None, max_count=500):
    """
    max_count の最大は 500(bf 約定履歴取得 api の仕様)

    約定履歴
    https://lightning.bitflyer.com/docs?lang=ja#%E7%B4%84%E5%AE%9A%E5%B1%A5%E6%AD%B4

    symbol の指定はマーケットの一覧を参照
    https://lightning.bitflyer.com/docs?lang=ja#%E3%83%9E%E3%83%BC%E3%82%B1%E3%83%83%E3%83%88%E3%81%AE%E4%B8%80%E8%A6%A7
    "BTC_JPY" とか、"FX_BTC_JPY" とか

    count, after, before の指定はページ形式を参照
    https://lightning.bitflyer.com/docs?lang=ja#%E3%83%9A%E3%83%BC%E3%82%B8%E5%BD%A2%E5%BC%8F
    """
    if 500 < max_count:
        max_count = 500

    bf = getattr(ccxta, "bitflyer")()
    try:
        # print(f'対象 id: {from_} 〜 {to} の取得を開始')
        params = {
            "symbol": symbol,
            "count": max_count,
            "after": from_ - 1 if from_ is not None else None,  # 同値は含まないので調整
            "before": to + 1 if to is not None else None,  # 同値は含まないので調整
        }
        executions = await bf.public_get_getexecutions(params)

        if executions is None or len(executions) == 0:
            log.info(f'約定データなし： {from_} 〜 {to}')
            executions = []

        return executions
    finally:
        if bf:
            await bf.close()


def get_next_range(curr_to, step, last):
    next_from = curr_to + 1
    next_to = curr_to + step
    if last < next_to:
        next_to = last

    return next_from, next_to


# 名前は Lambda の設定名に合わせる
def lambda_handler(event, context):

    first = event["first"]
    last = event["last"]
    symbol = event["symbol"]
    invoke_next = event["invoke_next"]

    msg = f'[{now()}] Lambda が {first} 〜 {last} の取得を開始しました'
    post_to_discord(msg)
    msg_detail = f'{msg}'
    log.info(msg_detail)

    step = 500
    from_, to = get_next_range(first - 1, step, last)
    interval_sec = 5
    get_retry_chance = 3

    while True:

        executions = []
        loop = asyncio.get_event_loop()
        try:
            executions = loop.run_until_complete(
                public_get_trade_async(symbol, from_, to, step))
        except Exception as err:
            get_retry_chance = get_retry_chance - 1
            if get_retry_chance < 0:
                msg = f'[{now()}] {from_} 〜 {to} の取得時にエラーが発生し、リトライでも失敗したので停止します'
                post_to_discord(msg)
                msg_detail = f'{msg} {err}'
                log.error(msg_detail)
                raise GetExecutionsError(msg)
            else:
                msg = f'[{now()}] {from_} 〜 {to} の取得時にエラーが発生しました。リトライします'
                post_to_discord(msg)
                msg_detail = f'{msg} {err}'
                log.error(msg_detail)

                time.sleep(interval_sec)
                continue

        key = f'{from_:0>10}-{to:0>10}'
        try:
            s3_resource = boto3.resource("s3")
            obj = s3_resource.Object(bucket_name, key)
            obj.put(Body=json.dumps(executions),
                    ContentType="application/json")
        except Exception as err:
            msg = f'[{now()}] s3 保存時にエラーが発生したので停止します'
            post_to_discord(msg)
            msg_detail = f'{msg} {err}'
            log.error(msg_detail)
            raise PutS3Error(msg)

        from_, to = get_next_range(to, step, last)
        get_retry_chance = 3

        if last < from_:
            post_to_discord(f'[{now()}] Lambda が {first} 〜 {last} の取得を完了しました')
            msg = json.dumps({
                "name": "bitflyer executions",
                "first": first,
                "last": last,
                "state": "completed",
                "invoke_next": invoke_next
            })
            log.info(msg)
            return msg

        time.sleep(interval_sec)


if __name__ == '__main__':
    event = {
        "symbol": "BTC_JPY",
        "first": 3216501,
        "last": 3226500,
        "invoke_next": "false",
    }
    lambda_handler(event, None)
