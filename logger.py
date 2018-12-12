from logging import Formatter, handlers, StreamHandler, getLogger, DEBUG


class Logger:
    """
    pythonのlog出力 - Qiita
    https://qiita.com/yopya/items/63155923602bf97dec53

    Python の AWS Lambda 関数ログ作成 - AWS Lambda
    https://docs.aws.amazon.com/ja_jp/lambda/latest/dg/python-logging.html

    ログ出力のための print と import logging はやめてほしい - Qiita
    https://qiita.com/amedama/items/b856b2f30c2f38665701
    """

    def __init__(self, name=__name__):
        self.logger = getLogger(name)
        self.logger.setLevel(DEBUG)

        # 以下の hander は使わなくてもロガー自体は動作する
        # というか lambda とともに使用する場合は、以下のように逆に面倒が増えるかも

        # formatter = Formatter(
        #     "[%(asctime)s] [%(process)d] [%(name)s] [%(levelname)s] %(message)s")

        # stdout
        # 便利な反面、lambda 上で有効になってると、logger の分と handler の分の
        # ２つが CloudWatch logs に出力されてしまうのでうざい
        # handler = StreamHandler()
        # handler.setLevel(DEBUG)
        # handler.setFormatter(formatter)
        # self.logger.addHandler(handler)

        # file
        # lambda 上では log.log 作成権限なくてエラーになる
        # handler = handlers.RotatingFileHandler(filename='log.log',
        #                                        maxBytes=1048576,
        #                                        backupCount=3)
        # handler.setLevel(DEBUG)
        # handler.setFormatter(formatter)
        # self.logger.addHandler(handler)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warn(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)
