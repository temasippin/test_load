import asyncio
import json
import math
from contextlib import suppress

from bs4 import BeautifulSoup

from repositories.dict_city_vacancy import DictCityVacancyRepository
from repositories.faas_requester import FaasRequester
from repositories.raw_av_vacancy import AvVacancyRepository


class AvitoVacancyScrapper:
    def __init__(
        self,
        requester: FaasRequester,
        dict_city_repo: DictCityVacancyRepository,
        vacancy_repo: AvVacancyRepository,
        chunk_size: int = 10,
        verify_retry: int = 5
    ):
        self.requester = requester
        self.dict_city_repo = dict_city_repo
        self.vacancy_repo = vacancy_repo
        self.chunk_size = chunk_size
        self.verify_retry = verify_retry

    def _chunked(self, lst, size: int = None):
        if size:
            return [lst[i:i + size] for i in range(0, len(lst), size)]
        return [lst[i:i + self.chunk_size] for i in range(0, len(lst), self.chunk_size)]

    def _get_av_url(self, area, vacancy_name):
        return f'https://www.avito.ru/{area}/vakansii?cd=1&q={vacancy_name}&s=104'

    def _is_valid_response(self, result: dict | Exception):
        if isinstance(result, Exception):
            return False
        with suppress(Exception):
            soup = BeautifulSoup(result['text'], 'html.parser')
            return bool(soup.find('script', {'data-mfe-state': 'true'}))
        return False

    def _get_json_page(self, soup):
        with suppress(Exception):
            script = soup.find('script', {'data-mfe-state': 'true'})
            if script and script.string:
                return json.loads(script.string.replace('&quot;', '"'))
        return {}

    def _get_pages_count(self, soup: BeautifulSoup):
        data = self._get_json_page(soup)
        count_vacancies = data.get('data', {}).get('mainCount', 0)
        return math.ceil(min(count_vacancies, 5000) / 50) if count_vacancies else 0

    def _get_page_urls(self, data, pages_count: int):
        if pages_count <= 1:
            return []

        pager = data.get('data', {}).get('catalog', {}).get('pager', {})
        if not pager or 'last' not in pager:
            return []

        last_page = 'https://www.avito.ru' + pager['last'].replace('amp;', '')
        start_index = last_page.find('p=') + 2
        end_index = last_page.find('&', start_index)
        base_url = last_page[:start_index] + '{}' + last_page[end_index:]

        return [base_url.format(page) for page in range(2, min(pages_count, 100) + 1)]

    async def _parse_pages(self, soup: BeautifulSoup, pages_count: int):
        data = self._get_json_page(soup)
        items = data.get('data', {}).get('catalog', {}).get('items', [])
        vacancies = self._get_vacancies_info(items)

        page_urls = self._get_page_urls(data, pages_count)
        for url_chunk in self._chunked(page_urls):
            tasks = [
                {
                    "json": {"url": url},
                    "target_url": url,
                }
                for url in url_chunk
            ]

            results = await self.requester.execute_concurrently(
                requests=tasks,
                request_method="POST",
                response_validator=self._is_valid_response
            )

            verify_results = [(result, _) for result, _ in results if not isinstance(result, Exception)]

            if len(results) != len(verify_results):
                # Тут можно придумать логику обработки отладки
                print(f"Успешно {len(verify_results)}/{len(results)} запросов")

            for result, _ in verify_results:
                page_soup = BeautifulSoup(result['text'], 'html.parser')
                page_data = self._get_json_page(page_soup)
                page_items = page_data.get('data', {}).get('catalog', {}).get('items', [])
                vacancies.extend(self._get_vacancies_info(page_items))

        return vacancies

    def _get_vacancies_info(self, items):
        return [
            {
                "vacancy_id": item.get('id'),
                "vacancy_name": item.get('title'),
                "vacancy_url": f"https://www.avito.ru{item.get('urlPath')}" if item.get('urlPath') else None
            }
            for item in items[1:]
        ]

    async def run(self):
        dict_city_records = self.dict_city_repo.get_all()
        for chunk in self._chunked(dict_city_records, 1):
            tasks = [
                {
                    "json": {"url": self._get_av_url(record.id_av, record.vacancy_name)},
                    "target_url": self._get_av_url(record.id_av, record.vacancy_name),
                }
                for record in chunk
            ]

            responses = await self.requester.execute_concurrently(
                requests=tasks,
                request_method="POST",
                response_validator=self._is_valid_response
            )

            verify_responses = [(response, _) for response, _ in responses if not isinstance(response, Exception)]

            if len(responses) != len(verify_responses):
                # Тут можно придумать логику обработки отладки
                print(f"Успешно {len(verify_responses)}/{len(responses)} запросов")

            all_vacancies = []

            for (response, _) in verify_responses:
                soup = BeautifulSoup(response['text'], 'html.parser')
                pages_count = self._get_pages_count(soup)
                if not pages_count:
                    continue

                vacancies = await self._parse_pages(soup, pages_count)
                all_vacancies.extend(vacancies)

            if all_vacancies:
                self.vacancy_repo.bulk_insert(all_vacancies)
