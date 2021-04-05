import itertools
import re
import asyncio, aiohttp, aiofiles


class InputProcessor:
    """
    Input processor interface
    """

    def batch_read(self):
        """
        specific input type business logic to process
        :return: batch
        """
        raise NotImplementedError("Subclasses should implement this!")


class FileInput(InputProcessor):
    """
    Read input from file
    """

    def __init__(self, path, BATCH_SIZE=2):
        """

        :param path:
        :param BATCH_SIZE
        """
        self.path = path
        self.BATCH_SIZE = BATCH_SIZE

    def batch_read(self):
        """
        Lazy function (generator) to read a file line by line.
        :return: batch
        """
        try:
            with open(self.path) as f:
                while True:
                    batch = list(itertools.islice(f, self.BATCH_SIZE))
                    if not batch:
                        break
                    yield batch
        except Exception as e:
            print(f'Error during input read file exception: {e.__class__} occurred.')


class DiskSaveProcessor:
    """
    Save to disk
    """
    def __init__(self, path):
        """

        """
        # Regex to check valid URL
        regex = ("((http|https)://)(www.)?" +
                 "[a-zA-Z0-9@:%._\\+~#?&//=]" +
                 "{2,256}\\.[a-z]" +
                 "{2,6}\\b([-a-zA-Z0-9@:%" +
                 "._\\+~#?&//=]*)")

        # Compile the ReGex
        self.pattern = re.compile(regex)
        self.path = path
        self.valid_ext = ['jpg', 'jpeg', 'png', 'gif']
        self.success_code = 200

    def isValidURL(self, url):
        """

        :param url:
        :return:
        """
        # If the string is empty or within valid extension
        # return false
        if not url or url.split('.')[-1] not in self.valid_ext:
            return False

        # Return if the string
        # matched the ReGex
        return True if re.search(self.pattern, url) else False

    async def get_req_fn(self, batch_urls, session):
        """

        :param batch_urls:
        :param session:
        :return:
        """
        tasks = [self.single_req(url.strip(), session) for urls in batch_urls for url in urls if self.isValidURL(url.strip())]

        await asyncio.gather(*tasks)

    async def single_req(self, url, session):
        try:
            async with session.get(url, allow_redirects=False, timeout=5) as resp:
                await self.save_to_disk(resp, url)
        except Exception as e:
            print(f'Error during fetching [{url}] exception {e.__class__} occurred.')

    async def save_to_disk(self, resp, url):
        """
        save to disk
        :param resp:
        :param url
        :return:
        """
        if resp.status == self.success_code:
            try:
                f = await aiofiles.open(f'{self.path}/{resp.url.name}', mode='wb')
                await f.write(await resp.read())
                await f.close()
                print(f'Successfully downloaded [{url}]')
            except Exception as e:
                print(f'Error in output write file exception: {e.__class__} occurred.')
        else:
            print(f'Error fetching {url} with http status code {resp.status}')


async def main():
    input = FileInput(path='/Users/ashish.gupta/PycharmProjects/practice_app/input.text')
    output = DiskSaveProcessor(path='/Users/ashish.gupta/PycharmProjects/practice_app/upload')
    session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=30)
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        await output.get_req_fn(input.batch_read(), session)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


