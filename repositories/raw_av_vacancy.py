import logging
from typing import Dict, List, Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from db import DatabaseManager
from models.raw_av_vacancy import RawAvVacancy

logger = logging.getLogger(__name__)


class AvVacancyRepository:
    """Репозиторий для работы с вакансиями (Dependency Inversion)"""
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def insert_vacancy(self, vacancy_data: Dict) -> Optional[RawAvVacancy]:
        """Добавление новой вакансии"""
        session: Session = self.db_manager.get_session()
        try:
            vacancy = RawAvVacancy(**vacancy_data)
            session.add(vacancy)
            session.commit()
            session.refresh(vacancy)
            return vacancy
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error inserting vacancy: {e}")
            return None
        finally:
            session.close()

    def bulk_insert(self, vacancies: List[Dict]) -> bool:
        """Массовое добавление вакансий"""
        session: Session = self.db_manager.get_session()
        try:
            session.bulk_insert_mappings(RawAvVacancy, vacancies)
            session.commit()
            return True
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error bulk inserting vacancies: {e}")
            return False
        finally:
            session.close()
