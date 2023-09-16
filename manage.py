from sqlalchemy.orm import Session
from datetime import datetime
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from db import ZipCodeModel, initialize_session
from schema import ZipCode
from db import OffersModel
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


def saveOrUpdateOffers(offers: list, zip_code: str = "", **kwargs):
    """
        save or update list of offers in mongodb
        if event of last update is exists then update offers
        :param zip_code = valid zip_codes that saved in postgreql db
    """
    with OffersModel() as offer:
        zip_code_last_update = getZipCodeEvent(zip_code, **kwargs)
        if not zip_code or not zip_code_last_update:
            offer.save_offers(offers)
        else:
            offer.update_offers(offers, {"zip_code": zip_code})

        saveZipCodeEvent(zip_code, **kwargs)


def processOffers(zip_code: str, **kwargs):
    """
        get offers from api endpoint (by zip_code) and process data then save them in mongodb 
        :param zip_code = valid zip_codes that saved in postgreql db
    """
    zip_code = str(zip_code)
    offers = getOffers(zip_code)["Offers"]
    # create dataframe for add zip_code and created_at and tab columns to dataset
    df = pd.DataFrame(offers)
    df["zip_code"] = zip_code
    df["created_at"] = datetime.now()
    df["tab"] = df["SalesProgramType"].apply(lambda x: "Special Program" if x in ["AcuraLoyaltyAppreciation", "AcuraConquest"] else x)
    df.fillna("", inplace=True)
    offers = df.to_dict("records")
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
    zip_codes = ZipCodeModel.get_zip_codes(read_fields=("code",))
    max_workers = MAX_THREAD
    if len(zip_codes) < max_workers:
        max_workers = len(zip_codes)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for zip_code in zip_codes:
            executor.submit(processOffers, zip_code[0])

    logger("%s zip_code offers saved !" % len(zip_code))


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
    zip_code = args.zip_code
    processOffers(zip_code, mode="save")

elif args.command == "get-offers":
    zip_code = args.zip_code
    print(getOffers(zip_code))

