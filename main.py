from sanic import Sanic
from sanic.response import json, stream

import peewee
import peewee_async
import asyncio
from concurrent.futures import ThreadPoolExecutor

app = Sanic()

db = peewee_async.MySQLDatabase('sanic', user='root', password='',
                         host='localhost', port=3306)


class TestModel(peewee.Model):
    text = peewee.CharField()

    class Meta:
        database = db

objects = peewee_async.Manager(db)

@app.route("/")
def index(request):
    all_testmodels = TestModel.select().limit(10)
    return json(all_testmodels)

@app.route("/spammer")
async def spammer(request):
    async with db.atomic_async() as tx:
        try:
            row_dicts = []
            for i in range(20000):
                row_dicts.append({'text': i})

            bulk_insert = TestModel.insert_many(row_dicts)
            await objects.execute(bulk_insert)
            print('insert db complete!')
        except peewee_async.IntegrityError:
            tx.rollback()

    return json({"ah": "oh"})

async def async_spammer_handler():
    async with db.atomic_async() as tx:
        try:
            row_dicts = []
            for i in range(50000):
                row_dicts.append({'text': i})

            bulk_insert = TestModel.insert_many(row_dicts)
            await objects.execute(bulk_insert)
            print('insert db complete!')
        except peewee_async.IntegrityError:
            tx.rollback()

@app.route("/spammerasync")
async def spammerasync(request):
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            async_spammer_handler()
        )
    finally:
        return json({"success": True})


@app.route("/asyncmethod")
async def asyncmethod(request):
    all_testmodels = await objects.execute(TestModel.select().limit(10))
    return json(all_testmodels)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, workers=5)
