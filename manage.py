from sqlalchemy.orm import Session
from datetime import datetime
from time import sleep, time
from concurrent.futures import ThreadPoolExecutor
from db import ZipCodeModel, initialize_session, OffersModel
from schema import ZipCode
from events import getZipCodeEvent, saveZipCodeEvent
import requests
import argparse
import schedule
import pandas as pd



MAX_THREAD = 5
BASE_URL = "http://automobiles.honda.com/platform/api/v1/specials?zipCode=%s"
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0"
}
payload = {}


def getOffers(zip_code: str) -> dict:
    """
        get offers from api endpint by zip_code (BASE_URL) 
        :param zip_code = valid zip_codes that saved in postgreql db
        :return json = the json response of api endpoint
    """
    url = BASE_URL % zip_code
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(response.text)

    return response.json()


def filterOfferKeys(offer):
    keys = [c.name for c in OffersModel.__table__.columns]
    new_offer = dict()
    for key in keys:
        try:
            new_offer[key] = offer[key]
        except KeyError as err:
            continue

    return new_offer

def saveOrUpdateOffers(offers: list, zip_code: int, **kwargs):
    """
        save or update list of offers in mongodb
        if event of last update is exists then update offers
    """
    offer_objects = [OffersModel(**filterOfferKeys(offer)) for offer in offers]
    OffersModel.bulk_save(offer_objects, zip_code=zip_code)


def processOffers(zip_code: int, **kwargs):
    """
        get offers from api endpoint (by zip_code) and process data then save them in mongodb 
        :param zip_code = valid zip_codes that saved in postgreql db
    """
    zip_code = int(zip_code)
    get_offer_time = time()
    offers = getOffers(zip_code)["Offers"]
    print("%s offers fetched from website at %s seconds" % (len(offers), time()-get_offer_time))
    # create dataframe for add zip_code and created_at and tab columns to dataset
    [offer.update({"zip_code_id": zip_code, "created_at": datetime.now(), "tab": "Special Program" if offer["SalesProgramType"] in ["AcuraLoyaltyAppreciation", "AcuraConquest"] else offer["SalesProgramType"]}) for offer in offers]
    saveOrUpdateOffers(offers, zip_code, **kwargs)
    print("process of storing offers done !")



def logger(message: str, path: str = "./process.log"):
    """
        save log in {current-path}/process.log
        :param message = the message want to log
        :param path = the path of log file | default is ./process.log
    """
    with open(path, "a") as _file:
        msg = "[%s] %s \n-------------------------------------------\n" % (datetime.now(), message)
        _file.write(msg)


def processZipCodes():
    """
        get zip_codes from database (postsgresql) 
        and process them by processOffers function using multi-threading pool method
    """
    # TODO : read_fields must be remove because its not safe
    zip_codes = ZipCodeModel.get_zip_codes()
    print("%s zip_codes found" % len(zip_codes))
    max_workers = MAX_THREAD
    if len(zip_codes) < max_workers:
        max_workers = len(zip_codes)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for zip_code in zip_codes:
            executor.submit(processOffers, zip_code.code)

    logger("%s zip_code offers saved !" % len(zip_codes))


def scheduler():
    """
        schedule the main task : run processZipCodes every day at 00:00:00
    """
    schedule.every(1).days.at("00:00").do(processZipCodes)
    while True:
        schedule.run_pending()
        sleep(1)



arguments = argparse.ArgumentParser()
arguments.add_argument("--command", dest="command", default="run", help="the command of process you want (run -- get-offers or etc)")
arguments.add_argument("--zip-code", dest="zip_code", default=97230, help="the valid zip-codes")
args = arguments.parse_args()


if args.command == "run":
    scheduler()

elif args.command == "process-zip-codes":
    processZipCodes()

elif args.command == "process-offers":
    start_time = time()
    zip_code = args.zip_code
    processOffers(zip_code)
    print("duration of process-offers: ", time()-start_time)

elif args.command == "get-offers":
    zip_code = args.zip_code
    print(getOffers(zip_code))

elif args.command == "test":
    cs = OffersModel.__table__.columns
    k = [[c.name, c.primary_key] for c in cs if not c.primary_key]
    print(k)

