from datetime import datetime

from sqlalchemy import TIMESTAMP, BigInteger, Column, Text

from db import Base


class DictCityVacancy(Base):
    __tablename__ = 'dict_city_vacncy_name_unique'
    __table_args__ = {'schema': 'public'}

    city_vacancyname_key = Column(Text, primary_key=True)
    city_name = Column(Text)
    id_hh = Column(BigInteger)
    id_av = Column(Text)
    vacancy_name = Column(Text)
    decode_name = Column(Text)
    upload_date_at = Column(TIMESTAMP(timezone=False), default=datetime.now())
    client = Column(Text)

    def __repr__(self):
        return f"<DictCityVacancy(city='{self.city_name}', vacancy='{self.vacancy_name}')>"
