from datetime import datetime

from sqlalchemy import TIMESTAMP, BigInteger, Column, Text

from db import Base


class RawAvVacancy(Base):
    __tablename__ = 'raw_scrap_av_vacancy_faas_test'
    __table_args__ = {'schema': 'public'}

    row_id = Column(BigInteger, autoincrement=True, primary_key=True)
    vacancy_id = Column(BigInteger)
    vacancy_name = Column(Text, nullable=False)
    vacancy_url = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=False), default=datetime.now(), nullable=False)

    def __repr__(self):
        return f"<Vacancy(id={self.vacancy_id}, name='{self.vacancy_name}')>"
