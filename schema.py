from pydantic import BaseModel, ConfigDict



class ZipCode(BaseModel):
   code: str
   class Config:
      orm_mode = True


# create a read class for syncing with database fields and 
# filter some importatnt fields (security issues!)
class ZipCodeRead(ZipCode):
   id: int
