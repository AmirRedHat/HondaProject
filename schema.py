from pydantic import BaseModel, ConfigDict, parse_obj_as
from typing import List
from datetime import datetime



class ZipCode(BaseModel):
   code: str
   class Config:
      orm_mode = True


# create a read class for syncing with database fields and 
# filter some importatnt fields (security issues!)
class ZipCodeRead(ZipCode):
   pass


