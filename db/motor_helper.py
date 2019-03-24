# -*- coding: utf-8 -*-
# @Time : 2019-02-02 15:57
# @Author : cxa
# @File : motor_helper.py
# @Software: PyCharm
import asyncio
from logger.log import storage
from motor.motor_asyncio import AsyncIOMotorClient
from bson import SON
import pprint

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass
# 数据库基本信息
db_configs = {
    'type': 'mongo',
    'host': '127.0.0.1',
    'port': '27017',
    "user": "",
    "password": "",
    'db_name': 'spider_data'
}


class MotorBase():
    def __init__(self):
        self.__dict__.update(**db_configs)
        if self.user:
            self.motor_uri = f"mongodb://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.db_name}?authSource={self.user}"
        else:
            self.motor_uri = f"mongodb://{self.host}:{self.port}/{self.db_name}"
        self.client = AsyncIOMotorClient(self.motor_uri)
        self.db = self.client.spider_data

    async def save_data(self, item):
        try:
            await self.db.infoq_details.update_one({
                'uuid': item.get("uuid")},
                {'$set': item},
                upsert=True)
        except Exception as e:
            storage.error(f"数据插入出错:{e.args}此时的item是:{item}")

    async def change_status(self, uuid, status_code=0):
        # status_code 0:初始,1:开始下载，2下载完了
        # storage.info(f"修改状态,此时的数据是:{item}")
        item = {}
        item["status"] = status_code
        await self.db.infoq_seed.update_one({'uuid': uuid}, {'$set': item}, upsert=True)

    async def reset_status(self):
        await self.db.infoq_seed.update_many({'status': 1}, {'$set': {"status": 0}})

    async def reset_all_status(self):
        await self.db.infoq_seed.update_many({}, {'$set': {"status": 0}})

    async def get_detail_datas(self):
        data = self.db.infoq_seed.find({'status': 1})

        async for item in data:
            print(item)
        return data

    async def use_count_command(self):
        response = await self.db.command(SON([("count", "infoq_seed")]))
        print(f'response:{pprint.pformat(response)}')


if __name__ == '__main__':
    m = MotorBase()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(m.reset_all_status())
