from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship

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
    code = Column(Integer, primary_key=True)
    offers = relationship("OffersModel", back_populates="zip_code")
    
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__tablename__.columns}
    
    @classmethod
    def get_zip_code(cls, code: str, **kwargs):
        """
            get zip_code record from postgresql database
            :param code = the code of record (like 97230)
            :return dict or SqlAlchemy-object = single record of zip_code
        """
        with Session(engine) as db:
            result = db.query(cls).filter(cls.code == code).first()
            
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
            results = results.all()
        
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
            
            instance = ZipCodeModel(**zip_code.dict())
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
            
            db.query(cls).filter(cls.code == code).delete()
            db.commit()
    
    @classmethod
    def update_zip_code(cls, code: str, zip_code: ZipCode):
        """
            update zip_code record in postgresql database 
            :param code = the code of zip_code record (like 97230)
            :param zip_code = the zip_code model by pydantic structure
        """
        with Session(engine) as db:
            instance = db.query(cls).filter(cls.code == code).first()
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


class OffersModel(Base):
    __tablename__ = "offers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    Id = Column(String(100))
    ModelYear = Column(String(10), default=None)
    StartDate = Column(DateTime)
    EndDate = Column(DateTime)
    IsFeatured = Column(Boolean)
    SalesProgramName = Column(String(100))
    SalesProgramType = Column(String(100))
    ModelGroupName = Column(String(100))
    ModelStockPhoto = Column(String(600))
    SpecialDescription = Column(Text, default="")
    ModelSeriesItemName = Column(String(100))
    tab = Column(String(100))
    zip_code_id = Column(Integer, ForeignKey("zip_code.code"))
    zip_code = relationship("ZipCodeModel", back_populates="offers")
    created_at = Column(DateTime)
    

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in OffersModel.__table__.columns}

    def save(self, *args, **kwargs):
        session = initialize_session()
        with session() as sess:
            sess.add(self)
            sess.commit()

    @classmethod
    def _delete(cls, _id: bool = None, session: Session = None, *args, **kwargs):
        if not session:
            session = initialize_session()

        sess: Session
        with session() as sess:
            if _id:
                result = sess.query(cls).filter(cls.id==_id)
            else:
                result = sess.query(cls)

            result.delete()

    @classmethod
    def delete(cls, _id: bool, session: Session = None, *args, **kwargs):
        if not session:
            session = initialize_session()

        sess: Session
        with session() as sess:
            result = sess.query(cls).filter(cls.id==_id)
            result.delete()

    @classmethod
    def delete_by_zip_code(cls, zip_code: int, session: Session = None, *args, **kwargs):
        if not session:
            session = initialize_session()

        sess: Session
        with session() as sess:
            result = sess.query(cls).filter(cls.zip_code_id==zip_code)
            result.delete()
            sess.commit()
    
    @classmethod
    def bulk_save(cls, objects: list, session: Session = None, zip_code: int = 0):
        if not session:
            session = initialize_session()

        if zip_code:
            cls.delete_by_zip_code(zip_code)

        sess: Session
        with session() as sess:
            sess.bulk_save_objects(objects)
            sess.commit()

    @classmethod
    def read(cls, _id: int = None, to_dict: bool = False):
        session = initialize_session()
        with session() as sess:
            if _id:
                result = sess.query(cls).filter(cls.id==_id)
            else:
                result = sess.query(cls)
                
            result = result.all()
        
        if to_dict:
            return [obj.to_dict() for obj in result]

        return result

    @classmethod
    def filter(cls, **kwargs):
        session = initialize_session()
        with session() as sess:
            result = sess.filter_by(**kwargs)
            result = result.all()

        return result


Base.metadata.create_all(engine)