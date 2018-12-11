from logging import Formatter, handlers, StreamHandler, getLogger, DEBUG


class Logger:
    """
    pythonのlog出力 - Qiita
    https://qiita.com/yopya/items/63155923602bf97dec53

    Python の AWS Lambda 関数ログ作成 - AWS Lambda
    https://docs.aws.amazon.com/ja_jp/lambda/latest/dg/python-logging.html
    """

    def __init__(self, name=__name__):
        self.logger = getLogger(name)
        self.logger.setLevel(DEBUG)
        formatter = Formatter(
            "[%(asctime)s] [%(process)d] [%(name)s] [%(levelname)s] %(message)s")

        # stdout
        handler = StreamHandler()
        handler.setLevel(DEBUG)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # lambda 上では log.log 作成権限なくてエラーになる
        # file
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
