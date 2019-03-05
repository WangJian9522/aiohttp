# @Time : 2019/02/02 10:02 AM
# @Author : cxa
# @Software: PyCharm
# encoding: utf-8
import os
import aiohttp
import hashlib
import aiofiles
from itertools import islice
import async_timeout
import asyncio
from logger.log import crawler, storage
from db.mongo_helper import Mongo
from db.motor_helper import MotorBase
import datetime
import json
from w3lib.html import remove_tags

base_url = "https://www.infoq.cn/public/v1/article/getDetail"
headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/json",
    "Host": "www.infoq.cn",
    "Origin": "https://www.infoq.cn",
    "Referer": "https://www.infoq.cn/article/Ns2yelhHTd0rhmu2-IzN",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
}

headers2 = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
}
try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass
# Semaphore限制同时请求构造连接的数量，Semphore充足时.
sema = asyncio.Semaphore(5)


async def get_buff(item, session):
    url = item.get("cover")
    with async_timeout.timeout(60):
        async with session.get(url) as r:
            if r.status == 200:
                buff = await r.read()
                if len(buff):
                    crawler.info(f"NOW_IMAGE_URL:, {url}")
                    await get_img(item, buff)


async def get_img(item, buff):
    # 题目层目录是否存在
    file_path = item.get("file_path")
    image_path = item.get("image_path")
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # 文件是否存在
    if not os.path.exists(image_path):
        storage.info(f"SAVE_PATH:{image_path}")
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(buff)


async def get_content(source, item):
    dic = {}
    dic["uuid"] = item.get("uuid")
    dic["title"] = item.get("title")
    dic["author"] = item.get("author")
    dic["publish_time"] = item.get("publish_time")
    dic["cover_url"] = item.get("cover")
    dic["tags"] = item.get("tags")
    dic["image_path"] = item.get("image_path")
    dic["md5name"] = item.get("md5name")
    html_content = source.get("data").get("content")
    dic["html"] = html_content
    dic["content"] = remove_tags(html_content)
    dic["update_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await MotorBase().save_data(dic)


async def fetch(item, session, retry_index=0):
    try:
        refer = item.get("url")
        name = item.get("title")
        uuid = item.get("uuid")
        md5name = hashlib.md5(name.encode("utf-8")).hexdigest()  # 图片的名字
        item["md5name"] = md5name
        data = {"uuid": uuid}
        headers["Referer"] = refer
        if retry_index == 0:
            await MotorBase().change_status(uuid, item, 1)  # 开始下载
        with async_timeout.timeout(60):
            async with session.post(url=base_url, headers=headers, data=json.dumps(data)) as req:
                res_status = req.status

                if res_status == 200:
                    jsondata = await req.json()
                    await get_content(jsondata, item)
        await MotorBase().change_status(uuid, item, 2)  # 下载成功
    except Exception as e:
        jsondata = None
    if not jsondata:
        crawler.error(f'Retry times: {retry_index + 1}')
        retry_index += 1
        return await fetch(item, session, retry_index)


async def bound_fetch(item, session):
    md5name = item.get("md5name")
    file_path = os.path.join(os.getcwd(), "infoq_cover")
    image_path = os.path.join(file_path, f"{md5name}.jpg")

    item["md5name"] = md5name
    item["image_path"] = image_path
    item["file_path"] = file_path
    async with sema:
        await fetch(item, session)
        await get_buff(item, session)


async def run(data):
    crawler.info("Start Spider")
    # TCPConnector维持链接池，限制并行连接的总量，当池满了，有请求退出再加入新请求。默认是100，limit=0的时候是无限制
    # ClientSession调用TCPConnector构造连接，Session可以共用
    async with aiohttp.connector.TCPConnector(limit=300, force_close=True, enable_cleanup_closed=True) as tc:
        async with aiohttp.ClientSession(connector=tc) as session:
            coros = (asyncio.ensure_future(bound_fetch(item, session)) for item in data)
            await start_branch(coros)


async def start_branch(tasks):
    # 分流
    [await _ for _ in limited_as_completed(tasks, 10)]


async def first_to_finish(futures, coros):
    while True:
        await asyncio.sleep(0.01)
        for f in futures:
            if f.done():
                futures.remove(f)
                try:
                    new_future = next(coros)
                    futures.append(asyncio.ensure_future(new_future))
                except StopIteration as e:
                    pass
                return f.result()


def limited_as_completed(coros, limit):
    futures = [asyncio.ensure_future(c) for c in islice(coros, 5, limit)]

    while len(futures) > 0:
        yield first_to_finish(futures, coros)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    url_gen = Mongo().find_data()
    try:
        loop.run_until_complete(run(url_gen))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
