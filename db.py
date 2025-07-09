import logging
import os
from typing import Optional

from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class DatabaseManager:
    """Класс для управления подключением к БД и миграциями (Single Responsibility)"""
    def __init__(self):
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.metadata = MetaData(schema='public')

    @staticmethod
    def _create_engine():
        db_url = (
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        return create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            connect_args={"connect_timeout": 5}
        )

    def get_session(self):
        """Возвращает сессию для работы с БД"""
        return self.SessionLocal()

    # def create_tables(self):
    #     """Создает таблицы в БД (для инициализации)"""
    #     try:
    #         Base.metadata.create_all(bind=self.engine)
    #         logger.info("Tables created successfully")
    #     except Exception as e:
    #         logger.error(f"Error creating tables: {e}")
    #         raise
