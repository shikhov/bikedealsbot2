from pathlib import Path

from aiohttp.web_fileresponse import FileResponse
from aiohttp.web_request import Request
from aiohttp.web_response import json_response

from aiogram import Bot
from aiogram.utils.web_app import safe_parse_webapp_init_data

# from app.src.repositories import TrackedSkuRepository

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
    sku_repository = request.app['sku_repository']
    async for sku in sku_repository.find({'chat_id': chat_id}):
        items.append({
            'name': sku.name,
            'variant': sku.variant,
            'store': sku.store,
            'code': sku.doc_id
        })
    
    return json_response(data=items)


async def api_delete_handler(request: Request):
    bot: Bot = request.app["bot"]
    jsondata = await request.json()

    try:
        webapp_data = safe_parse_webapp_init_data(token=bot.token, init_data=jsondata['_auth'])
    except ValueError:
        return json_response({"ok": False, "error": "Unauthorized"}, status=401)

    sku_repository = request.app['sku_repository']
    chat_id = str(webapp_data.user.id)

    try:
        await sku_repository.delete_by_ids(chat_id, jsondata['items'])
    except Exception:
        return json_response({"ok": False, "err": "Delete error"}, status=400)

    return json_response({"ok": True})

