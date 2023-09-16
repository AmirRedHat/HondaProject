from pydantic import BaseModel, ConfigDict



class ZipCode(BaseModel):
   code: str
   # model_config = ConfigDict(from_attributes=True)

   class Config:
      orm_mode = True


class ZipCodeRead(ZipCode):
   id: int
