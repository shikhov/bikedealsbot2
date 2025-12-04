from pathlib import Path

from aiohttp.web_fileresponse import FileResponse
from aiohttp.web_request import Request
from aiohttp.web_response import json_response

from pymongo import DeleteOne
import json

import logging

from aiogram import Bot
from aiogram.utils.web_app import safe_parse_webapp_init_data

async def list_handler(request: Request):
    return FileResponse(Path(__file__).parent.resolve() / 'html/list.html')


async def api_list_handler(request: Request):
    bot: Bot = request.app["bot"]
    jsondata = await request.json()
    
    try:
        webapp_data = safe_parse_webapp_init_data(token=bot.token, init_data=jsondata['_auth'])
    except ValueError:
        return json_response({"ok": False, "error": "Unauthorized"}, status=401)

    chat_id = str(webapp_data.user.id)
    items = []
    db = request.app['db']
    cursor = db.sku.find({'chat_id': chat_id})
    for doc in await cursor.to_list():
        items.append({
            'name': doc['name'],
            'variant': doc['variant'],
            'store': doc['store'],
            'code': doc['_id']
        })
    
    return json_response(data=items)


async def api_delete_handler(request: Request):
    bot: Bot = request.app["bot"]
    jsondata = await request.json()
    
    logging.info(f'json_data: {json.dumps(jsondata, indent=4, ensure_ascii=False)}')

    try:
        webapp_data = safe_parse_webapp_init_data(token=bot.token, init_data=jsondata['_auth'])
    except ValueError:
        return json_response({"ok": False, "error": "Unauthorized"}, status=401)

    db = request.app['db']
    chat_id = str(webapp_data.user.id)
    bulk_request = [DeleteOne({'_id': item, 'chat_id': chat_id}) for item in jsondata['items']]

    try:
        await db.sku.bulk_write(bulk_request, ordered=False)
    except Exception:
        return json_response({"ok": False, "err": "Delete error"}, status=400)

    return json_response({"ok": True})

