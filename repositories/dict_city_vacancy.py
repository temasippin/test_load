from typing import List, Optional

from sqlalchemy.orm import Session

from db import DatabaseManager
from models.dict_city_vacancy import DictCityVacancy


class DictCityVacancyRepository:
    """Репозиторий для работы с таблицей городов и вакансий (Dependency Inversion)"""
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_by_city_and_vacancy(self, city_name: str, vacancy_name: str) -> Optional[DictCityVacancy]:
        """Получение записи по названию города и вакансии"""
        session: Session = self.db_manager.get_session()
        try:
            return session.query(DictCityVacancy)\
                .filter(DictCityVacancy.city_name == city_name,
                        DictCityVacancy.vacancy_name == vacancy_name)\
                .first()
        finally:
            session.close()

    def get_all(self, limit: int = None) -> List[DictCityVacancy]:
        """Получение всех записей с лимитом"""
        session: Session = self.db_manager.get_session()
        try:
            if limit:
                return session.query(DictCityVacancy).filter(
                    (DictCityVacancy.id_av != '') & (DictCityVacancy.id_av.isnot(None))
                ).limit(limit).all()
            return session.query(DictCityVacancy).filter(
                (DictCityVacancy.id_av != '') & (DictCityVacancy.id_av.isnot(None))
            ).all()
        finally:
            session.close()

    def add_record(self, record_data: dict) -> DictCityVacancy:
        """Добавление новой записи"""
        session: Session = self.db_manager.get_session()
        try:
            record = DictCityVacancy(**record_data)
            session.add(record)
            session.commit()
            session.refresh(record)
            return record
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
