from datetime import datetime, timedelta
from pytz import timezone
from tzlocal import get_localzone

date_format = "%Y/%m/%d %H:%M"
local_zone = get_localzone()


# 名前は Lambda の設定名に合わせる
def lambda_handler(event, context):
    utc = datetime.now(timezone("UTC")).strftime(date_format)
    jst = datetime.now(local_zone).strftime(date_format)
    print(f'timezone: {local_zone}, UTC: {utc}, JST: {jst}')


if __name__ == '__main__':
    lambda_handler(None, None)
