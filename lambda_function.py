import asyncio
import ccxt.async_support as ccxta
from datetime import datetime, timedelta
from dateutil.parser import parse
import json
import logger
import os
from pprint import pprint
from pytz import timezone
import requests
import time
from tzlocal import get_localzone

date_format = "%Y/%m/%d %H:%M"
local_zone = get_localzone()
discord_post_url = os.environ["DISCORD_POST_URL"]
log = logger.Logger(__name__)


def post_to_discord(message):

    post_data = {
        "content": message
    }

    try:
        response = requests.post(discord_post_url, data=json.dumps(post_data),
                                 headers={'Content-Type': "application/json"})
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error('Request failed: {}'.format(e))


async def public_get_trade_async(symbol, after=None, before=None):
    """
    約定履歴
    https://lightning.bitflyer.com/docs?lang=ja#%E7%B4%84%E5%AE%9A%E5%B1%A5%E6%AD%B4

    symbol の指定はマーケットの一覧から
    https://lightning.bitflyer.com/docs?lang=ja#%E3%83%9E%E3%83%BC%E3%82%B1%E3%83%83%E3%83%88%E3%81%AE%E4%B8%80%E8%A6%A7
    "BTC_JPY" とか、"FX_BTC_JPY" とか

    count, before, after の指定はページ形式から
    https://lightning.bitflyer.com/docs?lang=ja#%E3%83%9A%E3%83%BC%E3%82%B8%E5%BD%A2%E5%BC%8F
    """
    bf = getattr(ccxta, "bitflyer")()
    try:
        # print(f'対象 id: {after} 〜 {before}')
        params = {
            "symbol": symbol,
            "count": 500,   # Max 500 件が仕様らしい
            "after": after - 1,  # 同値は含まないので調整
            "before": before + 1,  # 同値は含まないので調整
        }
        executions = await bf.public_get_getexecutions(params)
        return executions
    finally:
        if bf:
            await bf.close()

# 名前は Lambda の設定名に合わせる


def lambda_handler(event, context):

    before = 0
    after = 9
    step = 10
    interval_sec = 3
    while True:
        executions = []
        loop = asyncio.get_event_loop()
        try:
            executions = loop.run_until_complete(
                public_get_trade_async("BTC_JPY", before, after))
        except Exception as err:
            log.error(err)
            dt = datetime.now(local_zone).strftime(date_format)
            post_to_discord("[{}] エラーが発生したので停止します".format(dt))
            return

        if executions is None or len(executions) == 0:
            log.info('約定データなし： {} 〜 {}'.format(before, after))
            continue

        for execution in executions:
            native_time = parse(execution["exec_date"])
            utc = timezone("UTC").localize(native_time)
            jst = utc.astimezone(local_zone)
            log.info('execution id: {}, date: {}'.format(
                execution["id"], jst.strftime(date_format)))

        before = after + 1
        after = after + step

        if after > 30:
            return

        time.sleep(interval_sec)


if __name__ == '__main__':
    lambda_handler(None, None)
