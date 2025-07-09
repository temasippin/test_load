import asyncio
import logging

from repositories.faas_requester import FaasRequester


async def main():
    async with FaasRequester("URL", retry=50, max_concurrent=10) as requester:
        tasks = [
            {"payload": {"url": "https://www.avito.ru/ekaterinburg/vakansii/dvoynaya_oplata_na_ispytatelnyy_srok._montazhnik_na_lyubom_avto_7462380113"}}
            for i in range(10)
        ]
        results = await requester.execute_concurrently(tasks, "post")

if __name__ == "__main__":
    asyncio.run(main())
