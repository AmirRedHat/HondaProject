from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker, Session, declarative_base


from pymongo import MongoClient
from fastapi import HTTPException

from schema import ZipCode


Base = declarative_base()
engine = create_engine("postgresql+psycopg2://postgres:amir@localhost/postgres")


def initialize_session():
    session = sessionmaker(bind=engine)
    return session


class ZipCodeModel(Base):
    __tablename__ = "zip_code"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20))


    def to_dict(self):
        return {
            "id": self.id,
            "code": self.code
        }

    @classmethod
    def get_zip_code(cls, code: str, **kwargs):
        """
            get zip_code record from postgresql database
            :param code = the code of record (like 97230)
            :return dict or SqlAlchemy-object = single record of zip_code
        """
        with Session(engine) as db:
            result = db.query(cls).filter(cls.code==code).first()
            if kwargs.get("to_dict"):
                return result.to_dict()
            return result


    @classmethod
    def get_zip_codes(cls, **kwargs):
        """
            get all zip_code record from postgresql database
            :param read_fields = the fields that want to read
            :param read_fields = return list of dictionaries if to_dict == true
            :return list = list record of zip_code
        """
        with Session(engine) as db:
            results = db.query(cls)
            if kwargs.get("read_fields"):
                table = Base.metadata.tables["zip_code"]
                results = results.with_entities(*[table.c[field] for field in kwargs.get("read_fields")])

            results = results.all()
            if kwargs.get("to_dict"):
                return [result.to_dict() for result in results]
            
            return results


    @classmethod
    def save_zip_code(cls, zip_code: ZipCode):
        """
            save zip_code into postgresql database
            :param zip_code = the zip_code model by pydantic structure
        """
        with Session(engine) as db:
            is_exists = cls.get_zip_code(zip_code.code)
            if is_exists:
                raise HTTPException(400, "zip_code is already exists")

            instance = ZipCodeModel(**zip_code.__dict__)
            db.add(instance)
            db.commit()


    @classmethod
    def delete_zip_code(cls, code: str):
        """
            delete zip_code record
            :param code = the code of zip_code record (like 97230)
        """
        with Session(engine) as db:
            is_exists = cls.get_zip_code(code)
            if not is_exists:
                raise HTTPException(404, "code %s not found !" % code)

            db.query(cls).filter(cls.code==code).delete()
            db.commit()

    @classmethod
    def update_zip_code(cls, code: str, zip_code: ZipCode):
        """
            update zip_code record in postgresql database 
            :param code = the code of zip_code record (like 97230)
            :param zip_code = the zip_code model by pydantic structure
        """
        with Session(engine) as db:
            instance = db.query(cls).filter(cls.code==code).first()
            instance.code = zip_code.code
            db.flush()
            db.commit()
            
        

def testZipCodeModel():
    # this function is just for testing database operations
    # pydantic_instance = ZipCode(code=97230)

    # z1 = ZipCodeModel.save_zip_code(pydantic_instance)
    z1 = ZipCodeModel.get_zip_codes(read_fields=("code",))
    print(z1)
    print("Done !")


Base.metadata.create_all(engine)


class OffersModel:

    collection_name = "offers"
    db_name = "honda"

    def __init__(self):
        self.client = MongoClient()
        db = self.client[self.db_name]
        self.collection = db[self.collection_name]


    def __enter__(self):
        return self


    def __exit__(self, *args, **kwargs):
        self.client.close()

    
    def get_offers(self, _filter: dict = {}):
        """
            read offer records from mongodb 
            :param _filter = a dictionary of fields that want to filter by
        """
        results = list(self.collection.find(_filter, {"_id":False}))
        return results

    
    def save_offers(self, data: list):
        """
            save offer records in mongodb 
            :param data = a dictionary of offers
        """
        self.collection.insert_many(data)


    def update_offers(self, data: list, _filter: dict):
        """
            update offer records in mongodb 
            :param data = a dictionary of offers
            :param _filter = a dictionary of fields that want to filter by
        """
        self.delete_offers(_filter)
        self.save_offers(data)


    def delete_offers(self, _filter: dict):
        """
            delete offer records from mongodb 
            :param _filter = a dictionary of fields that want to filter by
        """
        x = self.collection.delete_many(_filter)
        print(x.deleted_count, " records deleted !")
