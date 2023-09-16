from typing import List
from fastapi import FastAPI, Request
from schema import ZipCode, ZipCodeRead
from db import ZipCodeModel, OffersModel
import json
import pandas as pd


app = FastAPI()


@app.post("/zip-codes/")
async def createZipCode(zip_code: ZipCode):
   ZipCodeModel.save_zip_code(zip_code)
   return {"detail": "zip_code saved !"}


@app.get('/zip-codes/', response_model=List[ZipCodeRead])
async def getZipCodes():
   results = ZipCodeModel.get_zip_codes()
   return results


@app.get("/zip-codes/{code}", response_model=ZipCodeRead)
async def getSingleZipCode(code: str):
   result = ZipCodeModel.get_zip_code(code)
   return result


@app.delete("/zip-codes/{code}")
async def deleteZipCode(code: str):
   ZipCodeModel.delete_zip_code(code)
   return {"detail": "zip_code deleted !"}


@app.put("/zip-codes/{code}")
async def updateZipCode(code: str, zip_code: ZipCode):
   ZipCodeModel.update_zip_code(code, zip_code)
   return {"detail": "zip_code updated !"}


@app.get("/offers")
async def getOffers(request: Request):
   args = request.query_params
   print(args.keys())
   with OffersModel() as offer:
      results = offer.get_offers(args)
      return results

