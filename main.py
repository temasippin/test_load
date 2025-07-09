import asyncio
import logging
import os

from dotenv import load_dotenv

from db import Base, DatabaseManager
from models.raw_av_vacancy import RawAvVacancy
from repositories.dict_city_vacancy import DictCityVacancyRepository
from repositories.faas_requester import FaasRequester
from repositories.raw_av_vacancy import AvVacancyRepository
from scrappers.vacancy_scrapper import AvitoVacancyScrapper


def initialize_database():
    """Инициализация БД и создание таблиц"""
    db_manager = DatabaseManager()

    try:
        Base.metadata.create_all(bind=db_manager.engine, tables=[RawAvVacancy.__table__])
    except Exception as e:
        raise

    return db_manager


if __name__ == "__main__":
    load_dotenv()

    db_manager = initialize_database()
    raw_av_vacancy_repo = AvVacancyRepository(db_manager)
    dict_vacancy_repo = DictCityVacancyRepository(db_manager)

    raw_av_vacancy_repo = AvVacancyRepository(db_manager)
    dict_vacancy_repo = DictCityVacancyRepository(db_manager)
    avito_scrapper = AvitoVacancyScrapper(
        FaasRequester(os.getenv('FAAS_URL'), retry=50, max_concurrent=5),
        dict_vacancy_repo,
        raw_av_vacancy_repo,
        chunk_size=10,
        verify_retry=3
    )

    asyncio.run(avito_scrapper.run())
