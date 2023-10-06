from redis import Redis
from time import time


def createKeyEvent(zip_code: str) -> str:
    return "%s__last_update" % zip_code


def saveZipCodeEvent(zip_code: str, **kwargs):
    """
        save last update time in redis as timestamp as a event 
        :param zip_code = valid zip_codes that saved in postgreql db
    """
    r = Redis()
    r.set(createKeyEvent(zip_code), int(time()))
    r.close()


def getZipCodeEvent(zip_code: str, **kwargs):
    """
        get last update time in redis as timestamp
        :param zip_code = valid zip_codes that saved in postgreql db
    """
    if kwargs.get("mode") == "save":
        return
        
    r = Redis()
    return r.get(createKeyEvent(zip_code)).decode()
    