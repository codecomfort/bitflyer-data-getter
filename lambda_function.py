from tzlocal import get_localzone
import time
import requests
from pytz import timezone
from pprint import pprint
import os
import logger
import json
from exceptions import GetExecutionsError, PutS3Error
from dateutil.parser import parse
from datetime import datetime, timedelta
import ccxt.async_support as ccxta
from botocore.exceptions import ClientError
import aioboto3
import asyncio

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


async def public_get_trade_async(symbol, from_=None, to=None, max_count=500, retry_chance=0, retry_interval_sec=0):
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

    try_count = 1

    while True:
        # for debug
        # await asyncio.sleep(2)
        # print(f'public_get_trade_async {from_}, {to} の取得を開始')
        # return

        bf = getattr(ccxta, "bitflyer")()
        # 新 api 使ってみる
        bf.urls["api"] = "https://api.bitflyer.com"

        try:
            print(
                f'public_get_trade_async {from_}, {to} の取得を開始({try_count}/{retry_chance} 回目)')
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
        except Exception as err:
            try_count = try_count + 1
            if retry_chance < try_count:
                msg = f'[{now()}] {from_} 〜 {to} の取得時にエラーが発生し({type(err)})、リトライ上限に達しました'
                post_to_discord(msg)

                msg_detail = f'{msg} {type(err)} {err}'
                log.error(msg_detail)
                raise GetExecutionsError(msg_detail)
            else:
                msg = f'[{now()}] {from_} 〜 {to} の取得時にエラーが発生しました({type(err)})。リトライします'
                post_to_discord(msg)

                msg_detail = f'{msg} {type(err)} {err}'
                log.error(msg_detail)

                time.sleep(retry_interval_sec)
                continue
        finally:
            if bf:
                await bf.close()


async def put_to_s3(executions, key, retry_chance=0, retry_interval_sec=0):

    try_count = 1

    while True:
        # for debug
        # await asyncio.sleep(2)
        # print(f'put_to_s3 {key} の保存を開始')
        # return

        try:
            print(f'put_to_s3 {key} の保存を開始({try_count}/{retry_chance} 回目)')
            s3_resource = aioboto3.resource("s3", region_name="ap-northeast-1")
            obj = s3_resource.Object(bucket_name, key)
            await obj.put(Body=json.dumps(executions),
                          ContentType="application/json")
            return
        except Exception as err:
            try_count = try_count + 1
            if retry_chance < try_count:
                msg = f'[{now()}] {key} の保存時にエラーが発生し({type(err)})、リトライ上限に達しました'
                post_to_discord(msg)

                msg_detail = f'{msg} {type(err)} {err}'
                log.error(msg_detail)
                raise PutS3Error(msg_detail)
            else:
                msg = f'[{now()}] {key} の保存時にエラーが発生しました({type(err)})。リトライします'
                post_to_discord(msg)

                msg_detail = f'{msg} {type(err)} {err}'
                log.error(msg_detail)

                time.sleep(retry_interval_sec)
                continue
        finally:
            if s3_resource:
                await s3_resource.close()


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
    # print(msg)
    msg_detail = f'{msg}'
    log.info(msg_detail)

    step = 500
    from_0, to_0 = get_next_range(first - 1, step, last)
    from_1, to_1 = get_next_range(to_0, step, last)
    from_2, to_2 = get_next_range(to_1, step, last)
    from_3, to_3 = get_next_range(to_2, step, last)
    from_4, to_4 = get_next_range(to_3, step, last)
    from_5, to_5 = get_next_range(to_4, step, last)
    from_6, to_6 = get_next_range(to_5, step, last)
    from_7, to_7 = get_next_range(to_6, step, last)

    interval_sec = 0.5

    while True:

        executions = []
        loop = asyncio.get_event_loop()

        # 約定履歴取得
        get_trade_tasks = [
            public_get_trade_async(symbol, from_0, to_0,
                                   step, retry_chance=3, retry_interval_sec=1),
            public_get_trade_async(symbol, from_1, to_1,
                                   step, retry_chance=3, retry_interval_sec=1),
            public_get_trade_async(symbol, from_2, to_2,
                                   step, retry_chance=3, retry_interval_sec=1),
            public_get_trade_async(symbol, from_3, to_3,
                                   step, retry_chance=3, retry_interval_sec=1),
            public_get_trade_async(symbol, from_4, to_4,
                                   step, retry_chance=3, retry_interval_sec=1),
            public_get_trade_async(symbol, from_5, to_5,
                                   step, retry_chance=3, retry_interval_sec=1),
            public_get_trade_async(symbol, from_6, to_6,
                                   step, retry_chance=3, retry_interval_sec=1),
            public_get_trade_async(symbol, from_7, to_7,
                                   step, retry_chance=3, retry_interval_sec=1),
        ]

        # リトライはそれぞれのタスクごとに行う
        # １つでもリトライ上限に達したらヤメて Lambda 自体失敗とする
        # (なので return_exceptions は指定しない)
        st = time.time()
        get_trade_results = loop.run_until_complete(
            asyncio.gather(*get_trade_tasks))
        print(f'get_trade_tasks: {time.time() - st}')

        # 約定履歴保存
        key0 = f'{from_0:0>10}-{to_0:0>10}'
        key1 = f'{from_1:0>10}-{to_1:0>10}'
        key2 = f'{from_2:0>10}-{to_2:0>10}'
        key3 = f'{from_3:0>10}-{to_3:0>10}'
        key4 = f'{from_4:0>10}-{to_4:0>10}'
        key5 = f'{from_5:0>10}-{to_5:0>10}'
        key6 = f'{from_6:0>10}-{to_6:0>10}'
        key7 = f'{from_7:0>10}-{to_7:0>10}'

        put_to_s3_tasks = [
            put_to_s3(get_trade_results[0], key0,
                      retry_chance=1, retry_interval_sec=1),
            put_to_s3(get_trade_results[1], key1,
                      retry_chance=1, retry_interval_sec=1),
            put_to_s3(get_trade_results[2], key2,
                      retry_chance=1, retry_interval_sec=1),
            put_to_s3(get_trade_results[3], key3,
                      retry_chance=1, retry_interval_sec=1),
            put_to_s3(get_trade_results[4], key4,
                      retry_chance=1, retry_interval_sec=1),
            put_to_s3(get_trade_results[5], key5,
                      retry_chance=1, retry_interval_sec=1),
            put_to_s3(get_trade_results[6], key6,
                      retry_chance=1, retry_interval_sec=1),
            put_to_s3(get_trade_results[7], key7,
                      retry_chance=1, retry_interval_sec=1),
        ]

        st = time.time()
        put_to_s3_results = loop.run_until_complete(
            asyncio.gather(*put_to_s3_tasks))
        print(f'put_to_s3_tasks: {time.time() - st}')

        # 終了条件
        if last <= to_0 or last <= to_1 or last <= to_2 or last <= to_3 or last <= to_4 or last <= to_5 or last <= to_6 or last <= to_7:
            post_to_discord(
                f'[{now()}] Lambda が {first} 〜 {last} の取得を完了しました')
            # print(f'[{now()}] Lambda が {first} 〜 {last} の取得を完了しました')
            msg = json.dumps({
                "name": "bitflyer executions",
                "first": first,
                "last": last,
                "state": "completed",
                "invoke_next": invoke_next
            })
            log.info(msg)
            return msg

        # api 制限対策
        # IP アドレス毎に 60 sec で 500 回が上限
        #
        # いま、取得 1.5 sec(新エンドポイント)、保存 1.5 sec, インターバル 0 sec
        # つまり、8 req/3 sec
        # 60 sec なら 160 request = 80,000 件
        # lambda 起動時間は max 300 sec(5 min)
        # なので、launcher 側で 400,000 件くらいは理論上設定できるが、
        # コケるとこの単位でやりなおしになる

        # 次の初期化
        from_0, to_0 = get_next_range(to_7, step, last)
        from_1, to_1 = get_next_range(to_0, step, last)
        from_2, to_2 = get_next_range(to_1, step, last)
        from_3, to_3 = get_next_range(to_2, step, last)
        from_4, to_4 = get_next_range(to_3, step, last)
        from_5, to_5 = get_next_range(to_4, step, last)
        from_6, to_6 = get_next_range(to_5, step, last)
        from_7, to_7 = get_next_range(to_6, step, last)


if __name__ == '__main__':
    event = {
        "symbol": "BTC_JPY",
        "first": 100001,
        "last": 1000000,
        "invoke_next": "false",
    }
    lambda_handler(event, None)
