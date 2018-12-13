class DataGetterError(Exception):
    pass


class GetExecutionsError(DataGetterError):
    pass


class PutS3Error(DataGetterError):
    pass
