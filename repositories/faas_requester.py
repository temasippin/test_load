import asyncio
import json
import logging
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import httpx
from fake_useragent import UserAgent
from ua_parser import user_agent_parser


class FaasRequester:
    def __init__(
        self,
        faas_url: str,
        faas_token: Optional[str] = None,
        retry: int = 3,
        timeout: int = 10,
        log_level: int = logging.INFO,
        max_concurrent: int = 10
    ):
        self.faas_url = faas_url.rstrip('/')
        self.faas_token = faas_token
        self.retry = retry
        self.timeout = timeout
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)

        self._init_logger(log_level)

        transport = httpx.AsyncHTTPTransport(retries=0)
        self.client = httpx.AsyncClient(
            timeout=timeout,
            transport=transport,
            limits=httpx.Limits(
                max_connections=max_concurrent,
                max_keepalive_connections=max_concurrent
            )
        )
        self.logger.info(f"Инициализация FaasRequester для {self.faas_url}")

    def _init_logger(self, log_level: int):
        """Инициализирует встроенный логгер для класса."""
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    async def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Tuple[bool, Union[Dict[str, Any], str, Exception]]:
        """Выполняет один HTTP запрос"""
        try:
            async with self._semaphore:
                response = await self.client.request(method, url, **kwargs)
                response.raise_for_status()

                content_type = response.headers.get('content-type', '')
                content_encoding = response.headers.get('content-encoding', '')
                if 'application/json' in content_type and content_encoding != 'deflate':
                    result = response.json()
                elif 'application/json' in content_type and content_encoding == 'deflate':
                    result = json.loads(response.content)
                else:
                    result = response.text

                return True, result
        except httpx.HTTPStatusError as e:
            return False, e
        except httpx.RequestError as e:
            return False, e
        except Exception as e:
            return False, e

    async def _request_with_retry_and_validation(
        self,
        method: str,
        url: str,
        target_url: Union[str, None],
        response_validator: Union[callable, None],
        retry_validator: int,
        **kwargs
    ) -> Tuple[bool, Union[Dict[str, Any], str, Exception]]:
        """Внутренний метод с повторными попытками и валидацией"""
        success = False
        result = None

        for attempt in range(1, self.retry + 1):
            success, result = await self._make_request(method, url, **kwargs)

            if success:
                self.logger.info(f"Успешный ответ ({method} {target_url}): попытка {attempt}")
                break
            else:
                error_msg = (result.response.status_code if isinstance(result, httpx.HTTPStatusError)
                             else str(result))
                self.logger.warning(
                    f"Ошибка запроса ({method} {target_url}): {error_msg}, "
                    f"попытка {attempt}/{self.retry}"
                )

                if attempt < self.retry:
                    wait_time = min(1.0 * (attempt + 1), 5.0)
                    await asyncio.sleep(wait_time)

        if not success:
            return False, result

        if response_validator is None:
            return True, result

        for attempt in range(1, retry_validator + 1):
            try:
                if response_validator(result):
                    self.logger.info(f"Успешная валидация ({method} {target_url}): попытка {attempt}")
                    return True, result
                else:
                    self.logger.warning(
                        f"Ответ не прошел валидацию ({method} {target_url}): "
                        f"попытка {attempt}/{retry_validator}"
                    )
            except Exception as e:
                self.logger.warning(
                    f"Ошибка при валидации ответа ({method} {target_url}): "
                    f"{str(e)}, попытка {attempt}/{retry_validator}"
                )

            success, result = await self._make_request(method, url, **kwargs)

            if not success:
                self.logger.warning(
                    f"Ошибка при повторном запросе валидации ({method} {target_url}): "
                    f"попытка {attempt}/{retry_validator}"
                )

            wait_time = min(1.0 * (attempt + 1), 5.0)
            await asyncio.sleep(wait_time)

        return False, result

    async def execute_concurrently(
        self,
        requests: List[Dict[str, Any]],
        request_method: Literal["POST", "GET"],
        response_validator: callable = None,
        retry_validator: int = 5
    ) -> List[Union[Dict[str, Any], str, Exception]]:
        """
        Выполняет несколько запросов с повторными попытками и валидацией

        :param requests: Список словарей с параметрами запросов
        :param request_method: Тип запроса ("POST" или "GET")
        :param response_validator: Функция для валидации ответа (callable или None) -> bool
        :param retry_validator: Количество циклов повторных попыток при неудачной валидации
        :return: Список результатов или исключений
        """
        tasks = []
        for req in requests:
            url = f"{self.faas_url}/{req.get('endpoint', '').lstrip('/')}".rstrip('/')

            if request_method == "POST":
                task = self._request_with_retry_and_validation(
                    method=request_method,
                    url=url,
                    target_url=req.get("target_url", None),
                    response_validator=response_validator,
                    retry_validator=retry_validator,
                    headers=await self._prepare_headers(req.get("headers")),
                    json=req.get("json", {}),
                )
            elif request_method == "GET":
                task = self._request_with_retry_and_validation(
                    method=request_method,
                    url=url,
                    target_url=req.get("target_url", None),
                    response_validator=response_validator,
                    retry_validator=retry_validator,
                    headers=await self._prepare_headers(req.get("headers")),
                    params=req.get("params", {}),
                )
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        return [(result[1], req.get("target_url", None)) for result in results]

    async def _prepare_headers(self, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Подготавливает заголовки запроса"""
        base_headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "accept-encoding": "gzip, deflate, br, zstd",
        }

        try:
            ua = UserAgent()
            user_agent = ua.chrome
            parsed_ua = user_agent_parser.Parse(user_agent)

            platform = parsed_ua.get("os", {}).get("family", "Windows")
            version = parsed_ua.get("user_agent", {}).get("major", "120")

            headers = {
                **base_headers,
                "User-Agent": user_agent,
                "Sec-Ch-Ua-Platform": f'"{platform}"',
                "Sec-Ch-Ua": f'"Google Chrome";v="{version}", "Chromium";v="{version}", "Not/A)Brand";v="24"',
            }

            if self.faas_token:
                headers["Authorization"] = f"Bearer {self.faas_token}"

            if additional_headers:
                headers.update(additional_headers)

            return headers

        except Exception as e:
            self.logger.error(f"Ошибка при создании заголовков: {str(e)}")
            return base_headers

    async def close(self):
        """Закрывает HTTP клиент."""
        await self.client.aclose()
        self.logger.info("HTTP клиент успешно закрыт")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
