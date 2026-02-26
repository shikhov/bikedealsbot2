import asyncio
import logging
import re
import os
from hashlib import md5
from datetime import datetime
from time import time
from collections import defaultdict
from typing import Callable, Dict, Any, Awaitable

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart, BaseFilter
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from pymongo import AsyncMongoClient, UpdateOne
from aiohttp import web
from webapp.routes import list_handler, api_list_handler, api_delete_handler

import parsing

from config import CONNSTRING, DBNAME

WEBAPP_PATH = os.getenv('PATH')
HOST = os.getenv('HOST')
APP_BASE_URL = f'https://{HOST}/{WEBAPP_PATH}'
PORT = 8000

STATUS_OK = 0
STATUS_TIMEOUTERROR = 1
STATUS_PARSINGERROR = 2

db = AsyncMongoClient(CONNSTRING)[DBNAME]


class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id == ADMINCHATID


class Product:
    id = None
    first_skuid = None
    name = None
    store = None
    storelc = None
    var_count = 0

    def __init__(self, data, source):
        self.variants = data
        self.source = source
        if data:
            first_sku = list(data.values())[0]
            self.id = first_sku['prodid']
            self.first_skuid = list(data.keys())[0]
            self.name = first_sku['name']
            self.store = first_sku['store']
            self.storelc = first_sku['store'].lower()
            self.var_count = len(data)

    def getSkuAddList(self):
        text_array = []
        text_array.append(self.name)
        for skuid, sku in self.variants.items():
            line = getSkuString(sku, ['icon', 'price']) + f'\n<i>Добавить: /add_{self.storelc}_{self.id}_{skuid}</i>'
            text_array.append(line)

        return text_array

    def hasSku(self, skuid):
        if not self.variants:
            return False
        if skuid not in self.variants:
            return False
        return True


TOKEN = None
ADMINCHATID = None
BESTDEALSCHATID = None
BESTDEALSMINPERCENTAGE = None
BESTDEALSWARNPERCENTAGE = None
BESTDEALSMINVALUE = None
CACHELIFETIME = None
ERRORMINTHRESHOLD = None
ERRORMAXDAYS = None
MAXITEMSPERUSER = None
CHECKINTERVAL = None
LOGCHATID = None
LOGFILTER = None
BANNERSTART = None
BANNERHELP = None
BANNERDONATE = None
STORES = None
DEBUG = None
HTTPTIMEOUT = None
REQUESTDELAY = None

async def loadSettings():
    global TOKEN, ADMINCHATID, BESTDEALSCHATID, BESTDEALSMINPERCENTAGE, BESTDEALSMINVALUE
    global BESTDEALSWARNPERCENTAGE, CACHELIFETIME, ERRORMINTHRESHOLD, ERRORMAXDAYS
    global MAXITEMSPERUSER, CHECKINTERVAL, LOGCHATID, BANNERSTART, BANNERHELP
    global BANNERDONATE, STORES, DEBUG, HTTPTIMEOUT, REQUESTDELAY, LOGFILTER

    settings = await db.settings.find_one({'_id': 'settings'})

    TOKEN = settings['TOKEN']
    ADMINCHATID = settings['ADMINCHATID']
    BESTDEALSCHATID = settings['BESTDEALSCHATID']
    BESTDEALSMINPERCENTAGE = settings['BESTDEALSMINPERCENTAGE']
    BESTDEALSWARNPERCENTAGE = settings['BESTDEALSWARNPERCENTAGE']
    BESTDEALSMINVALUE = settings['BESTDEALSMINVALUE']
    CACHELIFETIME = settings['CACHELIFETIME']
    ERRORMINTHRESHOLD = settings['ERRORMINTHRESHOLD']
    ERRORMAXDAYS = settings['ERRORMAXDAYS']
    MAXITEMSPERUSER = settings['MAXITEMSPERUSER']
    CHECKINTERVAL = settings['CHECKINTERVAL']
    LOGCHATID = settings['LOGCHATID']
    LOGFILTER = settings['LOGFILTER']
    BANNERSTART = settings['BANNERSTART']
    BANNERHELP = settings['BANNERHELP']
    BANNERDONATE = settings['BANNERDONATE']
    STORES = settings['STORES']
    DEBUG = settings['DEBUG']
    HTTPTIMEOUT = settings['HTTPTIMEOUT']
    REQUESTDELAY = settings['REQUESTDELAY']


class LoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any]
    ) -> Any:
        if isinstance(event, Message):
            if event.text != '/start' and event.chat.type == ChatType.PRIVATE:
                chat_id = str(event.from_user.id)
                if not await db.users.find_one({'_id': chat_id}):
                    user_data = {
                        '_id': chat_id,
                        'first_name': event.from_user.first_name,
                        'last_name': event.from_user.last_name,
                        'username': event.from_user.username,
                        'enable': True
                    }
                    await db.users.insert_one(user_data)
            
            result = await handler(event, data)
            await logMessage(event)
            return result
        return await handler(event, data)


# Configure logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("aiogram.event").setLevel(logging.WARNING) 

dp = Dispatcher()
dp.message.middleware(LoggingMiddleware())


async def processException(e: Exception, chat_id: str):
    error_codes = ['bot was blocked', 'user is deactivated']
    if e.message and any(code in e.message for code in error_codes):
        await disableUser(chat_id)


async def logMessage(message: Message):
    if not LOGCHATID: return
    if message.from_user.id == ADMINCHATID: return
    if message.text in LOGFILTER: return

    username = ' (' + message.from_user.username + ')' if message.from_user.username else ''
    logentry = '<b>' + message.from_user.full_name + username + ':</b> ' + message.text
    await bot.send_message(LOGCHATID, logentry)


def getStoreUrls():
    arr = []
    for store in STORES.values():
        status = '' if store['active'] else ' <i>(временно недоступен)</i>'
        arr.append(store['url'] + status)
    return arr


@dp.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
async def processCmdStart(message: Message):
    msg = substituteVars(BANNERSTART)
    await message.answer(msg)

    chat_id = str(message.from_user.id)
    data = {
        'first_name': message.from_user.first_name,
        'last_name': message.from_user.last_name,
        'username': message.from_user.username,
        'enable': True
    }
    await db.users.update_one({'_id' : chat_id }, {'$set': data}, upsert=True)
    await db.sku.update_many({'chat_id': chat_id}, {'$set': {'enable': True}})


async def broadcast(message: Message, text, docs):
    text_hash = md5(text.encode('utf-8')).hexdigest()
    await message.answer('🟢 Начало рассылки')

    count = 0
    async for doc in docs:
        count += 1
        if count % 100 == 0:
            await message.answer('Обработано: ' + str(count))
                
        if text_hash in doc.setdefault('broadcasts', []): continue

        try:
            await bot.send_message(chat_id=doc['_id'], text=text)
            doc['broadcasts'].append(text_hash)
            await db.users.update_one({'_id': doc['_id']}, {'$set': doc})
        except Exception as e:
            await processException(e, doc['_id'])
        await asyncio.sleep(0.1)

    await message.answer('🔴 Окончание рассылки')


@dp.message(Command('users'), IsAdmin())
async def processCmdUpdateUsers(message: Message):
    cursor = db.users.find({'enable': True})
    await message.answer('🟢 Начало обновления списка пользователей')

    count = 0
    async for doc in cursor:
        count += 1
        if count % 100 == 0:
            await message.answer('Обработано: ' + str(count))
        
        try:
            await bot.send_chat_action(chat_id=doc['_id'], action='typing')
        except Exception as e:
            await processException(e, doc['_id'])
        await asyncio.sleep(0.1)

    await message.answer('🔴 Окончание обновления списка пользователей')


@dp.message(Command('bc'), IsAdmin())
async def processCmdBroadcast(message: Message):
    text = message.html_text.replace('/bc', '', 1).strip()
    docs = db.users.find({'enable': True})
    await broadcast(message, text, docs)


@dp.message(F.text.regexp(r'^/bc_\w+'), IsAdmin())
async def processCmdBroadcastByStore(message: Message):
    text = message.get_args()
    params = message.get_command().split('_')
    store = params[1].upper()
    docs = db.users.aggregate([
        {
            '$lookup':
            {
                'from': 'sku',
                'localField': '_id',
                'foreignField': 'chat_id',
                'as': 'matched_skus'
            }
        },
        {
            '$match':
            {
                'matched_skus.store': store,
                'enable': True
            }
        }
    ])
    await broadcast(message, text, docs)


@dp.message(Command('reload'), IsAdmin())
async def processCmdReload(message: Message):
    await loadSettings()
    await message.answer('Settings successfully reloaded')


@dp.message(F.text.regexp(r'https?://', mode='search'), F.chat.type == ChatType.PRIVATE)
async def processURLMsg(message: Message):
    for store, attrs in STORES.items():
        if re.search(attrs['url_regex'], message.text):
            break
    else:
        await message.reply('⚠️ Этот сайт не поддерживается. Список поддерживаемых смотрите в /help')
        return

    if not attrs['active']:
        await message.reply('😔 К сожалению, отслеживание этого сайта временно недоступно')
        return

    url = processURL(store, message.text)
    if not url:
        await message.reply('🤷‍♂️ Не могу понять. Кажется, это не ссылка на товар')
        return

    await showVariants(store, url, message)


def processURL(store, text):
    if store == 'BD':
        rg = re.search(r'https://www\.bike-discount\.de/.+?/([^?&\s]+)', text)
        if rg:
            return 'https://www.bike-discount.de/en/' + rg.group(1)

    if store == 'B24':
        rg = re.search(r'(https://www\.bike24\.(com|de)/p[12](\d+)\.html)', text)
        if rg:
            return 'https://www.bike24.com/p2' + rg.group(3) + '.html'

    if store == 'TI':
        rg = re.search(r'(https://www\.tradeinn\.com/)(.+?)/(.+?)(/\S+/\d+/p)', text)
        if rg:
            return rg.group(1) + 'bikeinn/en' + rg.group(4)

    if store == 'SB':
        rg = re.search(r'(https://www\.starbike\.com/en/\S+?/)', text)
        if rg:
            return rg.group(1)

    if store == 'CRC':
        rg = re.search(r'(https://www\.chainreactioncycles\.com/)(\S+/)?(p/[^?&\s]+)', text)
        if rg:
            return rg.group(1) + 'int/' + rg.group(3)

    if store == 'BC':
        rg = re.search(r'(https://www\.bike-components\.de/)(.+?)(/\S+p(\d+)\/)', text)
        if rg:
            return rg.group(1) + 'en' + rg.group(3)

    if store == 'A4C':
        rg = re.search(r'https://www\.all4cycling\.com/(.+?/)?products/([^?]+)', text)
        if rg:
            return 'https://www.all4cycling.com/en/products/' + rg.group(2)

    if store == 'LG':
        rg = re.search(r'https://www\.lordgun\.com/([^ ?]+)', text)
        if rg:
            return 'https://www.lordgun.com/' + rg.group(1)

    return None


@dp.message(F.text.regexp(r'^/add_\w+_\w+_\w+$'), F.chat.type == ChatType.PRIVATE)
async def processCmdAdd(message: Message):
    params = message.text.split('_')
    store = params[1].upper()
    prodid = params[2]
    skuid = params[3]
    await addVariant(store, prodid, skuid, message)


@dp.message(F.text.regexp(r'^/del_\w+_\w+_\w+$'), F.chat.type == ChatType.PRIVATE)
async def processCmdDel(message: Message):
    chat_id = str(message.from_user.id)
    docid = message.text.replace('/del', chat_id).upper()
    query = {'_id': docid}
    result = await db.sku.delete_one(query)
    if result.deleted_count == 1:
        await message.answer('Удалено')
        return
    await message.answer('Какая-то ошибка 😧')


@dp.message(Command('help'), F.chat.type == ChatType.PRIVATE)
async def processCmdHelp(message: Message):
    msg = substituteVars(BANNERHELP)
    await message.answer(msg)


@dp.message(Command('donate'), F.chat.type == ChatType.PRIVATE)
async def processCmdDonate(message: Message):
    await message.answer(BANNERDONATE)


@dp.message(Command('list'), F.chat.type == ChatType.PRIVATE)
async def processCmdList(message: Message):
    text_array = []
    chat_id = str(message.from_user.id)
    query = {'chat_id': chat_id}
    async for doc in db.sku.find(query):
        key = doc['store'].lower() + '_' + doc['prodid'] + '_' + doc['skuid']
        line = getSkuString(doc, ['store', 'url', 'icon', 'price']) + f'\n<i>Удалить: /del_{key}</i>'
        text_array.append(line)

    if text_array:
        text_array = ['Отслеживаемые товары:'] + text_array
    else:
        text_array = ['Ваш список пуст']

    await paginatedTgMsg(text_array, chat_id)


@dp.message(Command('listw'), F.chat.type == ChatType.PRIVATE)
async def command_list_web(message: Message):
    kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text='Открыть', web_app=WebAppInfo(url=f'{APP_BASE_URL}/list/'))
                ]
            ]
    )
    await message.answer(
        'Нажмите кнопку ниже, чтобы открыть веб-интерфейс для управления отслеживаемыми товарами:',
        reply_markup=kb
    )


@dp.message(Command('stat'), IsAdmin())
async def processCmdStat(message: Message):
    sent_msg = await message.answer('Getting stat...')

    usersall = await db.users.count_documents({})
    usersactive = await db.users.count_documents({'enable': True})
    skuall = await db.sku.count_documents({})
    
    pipeline = [
        {
            '$lookup':
            {
                'from': "sku",
                'localField': "_id",
                'foreignField': "chat_id",
                'as': "sku_docs"
            }
        },
        {
            '$addFields': { 'sku_count': { '$size': "$sku_docs" } }
        },
        {
            '$match': { 'sku_count': { '$ne': 0 } }
        },
        {
            '$match': { 'enable': True }
        },
        {
            '$count': 'count'
        }
    ]
    
    cursor = await db.users.aggregate(pipeline)
    try:
        result_list = await cursor.to_list(length=1)
        userswsku = result_list[0]['count'] if result_list else 0
    except Exception:
        userswsku = 0
        
    skuactive = await db.sku.count_documents({'enable': True})

    msg = ''
    msg += f'<b>Total users:</b> {usersall}\n'
    msg += f'<b>Enabled users:</b> {usersactive}\n'
    msg += f'<b>Enabled users with SKU:</b> {userswsku}\n'
    msg += f'<b>Total SKU:</b> {skuall}\n'
    msg += f'<b>Active SKU:</b> {skuactive}\n'

    for key in STORES.keys():
        num = await db.sku.count_documents({'store': key})
        msg += f'<b>{key}:</b> {num}\n'

    TOPNUMBER = 10
    pipeline_top = [
        {
            '$lookup':
            {
                'from': "sku",
                'localField': "_id",
                'foreignField': "chat_id",
                'as': "sku_docs"
            }
        },
        {
            '$addFields': { 'sku_count': { '$size': "$sku_docs" } }
        },
        {
            '$match': { 'sku_count': { '$ne': 0 } }
        },
        {
            '$sort': { "sku_count": -1 }
        },
        {
            '$limit': TOPNUMBER
        }
    ]
    
    msg += f'\n<b>Top {TOPNUMBER} users:</b>\n'
    async for doc in await db.users.aggregate(pipeline_top):
        username = ' (' + doc['username'] + ')' if doc['username'] else ''
        full_name = doc['first_name'] + ' ' + doc['last_name'] if doc['last_name'] else doc['first_name']
        msg += f'{full_name}{username}: {doc["sku_count"]}\n'

    await sent_msg.edit_text(msg)


@dp.message(F.chat.type == ChatType.PRIVATE)
async def processSearch(message: Message):
    text = message.text
    if not text: return

    chat_id = str(message.from_user.id)
    query = {'chat_id': chat_id}
    if await db.sku.count_documents(query) == 0:
        await message.answer('⚠️ Ваш список пуст, поиск невозможен')
        return

    try:
        pattern = re.compile(text, re.I)
    except Exception:
        await message.reply('⚠️ Некорректное выражение')
        return

    query = {'chat_id': chat_id, 'name': {'$regex': pattern}}
    text_array = []
    async for doc in db.sku.find(query):
        key = doc['store'].lower() + '_' + doc['prodid'] + '_' + doc['skuid']
        line = getSkuString(doc, ['store', 'url', 'icon', 'price']) + f'\n<i>Удалить: /del_{key}</i>'
        text_array.append(line)

    header = f'Результаты поиска по строке <b>{text}</b>:'
    text_array = [header] + (text_array or ['Ничего не найдено'])
    await paginatedTgMsg(text_array, chat_id)


async def reply_or_edit_msg(text, message: Message):
    if message.from_user.id == bot.id:
        await message.edit_text(text)
    else:
        await message.reply(text)



async def showVariants(store, url, message: Message):
    sent_msg = await message.reply('🔎 Ищу информацию о товаре...')

    prod = await getProduct(store, url)
    if prod.var_count == 0:
        await sent_msg.edit_text('Не смог найти цену 😧')
    elif prod.var_count == 1:
        await addVariant(store, prod.id, prod.first_skuid, sent_msg)
    elif prod.var_count > 1:
        await paginatedTgMsg(prod.getSkuAddList(), message.chat.id, sent_msg.message_id)


async def addVariant(store, prodid, skuid, message: Message):
    chat_id = str(message.chat.id)
    user = await db.users.find_one({'_id': chat_id})
    if not user:
        await reply_or_edit_msg('Какая-то ошибка 😧', message)
        return

    maxitems = user.get('maxitems', MAXITEMSPERUSER)
    query = {'chat_id': chat_id}
    if await db.sku.count_documents(query) >= maxitems:
        await reply_or_edit_msg(f'⛔️ Увы, в данный момент добавить можно не более {maxitems} позиций', message)
        return

    docid = chat_id + '_' + store + '_' + prodid + '_' + skuid
    if await db.sku.find_one({'_id': docid}):
        await reply_or_edit_msg('️☝️ Товар уже есть в вашем списке', message)
        return

    url = await getURL(store, prodid)
    if not url:
        await reply_or_edit_msg('Какая-то ошибка 😧', message)
        return

    prod = await getProduct(store, url)
    if not prod.hasSku(skuid):
        await reply_or_edit_msg('Какая-то ошибка 😧', message)
        return

    data = prod.variants[skuid].copy()
    data['_id'] = docid
    data['store_prodid'] = data['store'] + '_' + data['prodid']
    data['chat_id'] = chat_id
    data['skuid'] = skuid
    data['errors'] = 0
    data['enable'] = True
    data['lastcheck'] = datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M')
    data['lastcheckts'] = int(time())
    data['lastgoodts'] = int(time())
    data['instock_prev'] = None
    data['price_prev'] = None
    await db.sku.insert_one(data)

    dispname = data['variant'] or data['name']
    await reply_or_edit_msg(dispname + '\n✔️ Добавлено к отслеживанию', message)


async def getURL(store, prodid):
    doc = await db.skucache.find_one({'_id': store + '_' + prodid})
    if doc:
        return doc['url']
    return None


def substituteVars(text):
    text = text.replace('%STOREURLS%', '\n'.join(getStoreUrls()))
    return text


async def paginatedTgMsg(text_array, chat_id, message_id=0, delimiter='\n\n'):
    async def sendOrEditMsg():
        if message_id != 0 and first_page:
            await bot.edit_message_text(text=msg, chat_id=chat_id, message_id=message_id)
        else:
            await bot.send_message(chat_id, msg)

    first_page = True
    msg = ''

    for paragraph in text_array:
        if len(msg + paragraph) > 4090:
            await sendOrEditMsg()
            msg = ''
            first_page = False
        msg += paragraph + delimiter

    if msg:
        await sendOrEditMsg()


async def getProduct(store, url):
    tsexpired = int(time()) - CACHELIFETIME * 60
    query = {'url': url, 'timestamp': {'$gt': tsexpired}}
    doc = await db.skucache.find_one(query)
    if doc:
        return Product(data=doc['variants'], source='cache')

    parseFunction = getattr(parsing, 'parse' + store)
    result = await parseFunction(url, HTTPTIMEOUT)
    await cacheVariants(url, result)
    return Product(data=result['variants'], source='web')


async def clearSKUCache():
    tsexpired = int(time()) - CACHELIFETIME * 60
    query = {'timestamp': {'$lt': tsexpired}}
    await db.skucache.delete_many(query)


async def removeInvalidSKU():
    banner = f'ℹ️ Следующие позиции были удалены из вашего списка в связи с недоступностью более {ERRORMAXDAYS} дней:'
    tsexpired = int(time()) - ERRORMAXDAYS * 24 * 3600
    query = {'lastgoodts': {'$lt': tsexpired}}
    messages = {}
    async for doc in db.sku.find(query):
        user = await db.users.find_one({'_id': doc['chat_id']})
        if not user['enable']: continue
        skustring = getSkuString(doc, ['store', 'url'])
        messages.setdefault(doc['chat_id'], [banner]).append(skustring)

    await db.sku.delete_many(query)

    for chat_id in messages:
        try:
            await paginatedTgMsg(messages[chat_id], chat_id)
        except Exception as e:
            await processException(e, chat_id)
        await asyncio.sleep(0.1)


def getSkuString(sku, options):
    instock = sku['instock']
    url = sku['url']
    name = sku['name']
    variant = sku['variant']
    price = sku['price']
    price_prev = sku.get('price_prev')
    currency = sku['currency']
    store = sku['store']
    errors = sku.get('errors', 0)

    storename = ''
    urlname = ''
    icon = ''
    pricetxt = ''
    pricetxt_prev = ''

    if 'url' in options:
        urlname = f'<a href="{url}">{name}</a>\n'
    if 'icon' in options:
        icon = '✅ ' if instock else '🚫 '
        if errors > ERRORMINTHRESHOLD: icon = '⚠️ '
        if not STORES[store]['active']: icon = '⏳ '
    if 'store' in options:
        storename = f'<code>[{store}]</code> '
    if 'price' in options:
        pricetxt = f' <b>{price} {currency}</b>'
    if 'price_prev' in options:
        pricetxt_prev = f' (было: {price_prev} {currency})'

    return storename + urlname + icon + (variant + pricetxt + pricetxt_prev).strip()


async def cacheVariants(url, result):
    if result['status'] == STATUS_TIMEOUTERROR:
        return

    variants = result['variants']
    if variants:
        first_sku = list(variants.values())[0]
        docid = first_sku['store'] + '_' + first_sku['prodid']
        query = {'_id': docid}
    else:
        query = {'url': url}

    data = {
            'variants': variants,
            'timestamp': int(time()),
            'url': url
    }
    await db.skucache.update_one(query, {'$set': data}, upsert=True)


async def notify():
    def addMsg(msg):
        messages.setdefault(doc['chat_id'], []).append(msg)

    def processBestDeals():
        price_prev = doc['price_prev']
        price = doc['price']
        if price_prev == 0: return
        percents = int((1 - price/float(price_prev))*100)
        value = price_prev - price
        minvalue = BESTDEALSMINVALUE.get(doc['currency'], 0)
        if percents >= BESTDEALSMINPERCENTAGE and value >= minvalue:
            bdkey = doc['store_prodid'] + '_' + doc['skuid']
            bestdeals[bdkey] = skustring + ' ' + str(percents) + '%'
            if percents >= BESTDEALSWARNPERCENTAGE:
                bestdeals[bdkey] += '‼️'

    messages = {}
    bestdeals = {}
    bulk_request = []

    query = {'$or': [{'price_prev': {'$ne': None}},{'instock_prev': {'$ne': None}}], 'enable': True}
    async for doc in db.sku.find(query):
        if doc['instock_prev'] is not None:
            skustring = getSkuString(doc, ['store', 'url', 'price'])
            if doc['instock']:
                addMsg('✅ Снова в наличии!\n' + skustring)
            if not doc['instock']:
                addMsg('🚫 Не в наличии\n' + skustring)
        elif doc['price_prev'] is not None and doc['instock']:
            skustring = getSkuString(doc, ['store', 'url', 'price', 'price_prev'])
            if doc['price'] < doc['price_prev']:
                addMsg('📉 Снижение цены!\n' + skustring)
                processBestDeals()
            if doc['price'] > doc['price_prev']:
                addMsg('📈 Повышение цены\n' + skustring)

        bulk_request.append(
            UpdateOne(
                { '_id': doc['_id'] },
                { '$set': {'price_prev': None, 'instock_prev': None} }
            )
        )

    for chat_id in messages:
        try:
            await paginatedTgMsg(messages[chat_id], chat_id)
        except Exception as e:
            await processException(e, chat_id)
        if DEBUG and LOGCHATID:
            await paginatedTgMsg(messages[chat_id], LOGCHATID)
        await asyncio.sleep(0.1)

    if BESTDEALSCHATID:
        await paginatedTgMsg(bestdeals.values(), BESTDEALSCHATID)

    if bulk_request:
        await db.sku.bulk_write(bulk_request)


async def disableUser(chat_id):
    await db.users.update_one({'_id': chat_id}, {'$set': {'enable': False}}, upsert=True)
    await db.sku.update_many({'chat_id': chat_id}, {'$set': {'enable': False}})


async def checkSKU():
    now = int(time())
    query = {'enable': True, 'lastcheckts': {'$lt': now - CHECKINTERVAL * 60}}
    prodlist = set()
    async for doc in db.sku.find(query):
        prodlist.add(doc['store_prodid'])
    
    prodlist = list(prodlist)
    if not prodlist:
        return

    cursor = db.sku.find({'store_prodid': {'$in': prodlist}, 'enable': True}).sort('store_prodid')
    async for doc in cursor:
        if not STORES[doc['store']]['active']: continue

        logging.info(doc['_id'] + ' [' + doc['name'] + '][' + doc['variant'] + ']')

        prod = await getProduct(doc['store'], doc['url'])
        if prod.hasSku(doc['skuid']):
            sku = prod.variants[doc['skuid']]
            if sku['instock'] != doc['instock']:
                doc['instock_prev'] = doc['instock']

            price_threshold = STORES[doc['store']]['price_threshold']
            if sku['currency'] == doc['currency']:
                if doc['price']*price_threshold < abs(sku['price'] - doc['price']):
                    doc['price_prev'] = doc['price']

            doc['instock'] = sku['instock']
            doc['currency'] = sku['currency']
            doc['price'] = sku['price']
            doc['variant'] = sku['variant']
            doc['errors'] = 0
            doc['lastgoodts'] = int(time())
        else:
            doc['errors'] += 1

        doc['lastcheck'] = datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M')
        doc['lastcheckts'] = int(time())
        try:
            await db.sku.update_one({'_id': doc['_id']}, {'$set': doc})
        except Exception:
            pass
        if prod.source == 'web':
            await asyncio.sleep(REQUESTDELAY)


async def errorsMonitor():
    bad = defaultdict(int)
    good = defaultdict(int)
    query = {'lastcheckts': {'$gt': int(time()) - CHECKINTERVAL*60}}
    
    async for doc in db.sku.find(query):
        if doc['errors'] == 0:
            good[doc['store']] += 1
        else:
            bad[doc['store']] += 1

    for store in set(list(good) + list(bad)):
        if not STORES[store]['active']: continue
        good_count = good[store]
        bad_count = bad[store]
        if good_count == 0 or bad_count/float(good_count) > 0.8:
            await bot.send_message(ADMINCHATID, f'Problem with {store}!\nGood: {good_count}\nBad: {bad_count}')


def create_webapp_server():
    app = web.Application()
    app.router.add_get('/list/', list_handler)
    app.router.add_post('/api/list', api_list_handler)
    app.router.add_post('/api/delete', api_delete_handler)
    # app.add_routes([web.static('/static', 'webapp/static')])
    
    return app


async def main():
    # settings
    await loadSettings()

    # Initialize bot and dispatcher
    global bot
    botProperties = DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True)
    bot = Bot(token=TOKEN, default=botProperties)

    web_app = create_webapp_server()
    web_app['bot'] = bot
    web_app['db'] = db
    web_app['ADMINCHATID'] = ADMINCHATID
    web_runner = web.AppRunner(web_app)
    await web_runner.setup()
    site = web.TCPSite(web_runner, '0.0.0.0', int(PORT))
    await site.start()

    scheduler = AsyncIOScheduler(job_defaults={'misfire_grace_time': None})
    scheduler.start()

    scheduler.add_job(checkSKU, 'interval', minutes=5)
    scheduler.add_job(notify, 'interval', minutes=5)
    scheduler.add_job(errorsMonitor, 'interval', minutes=CHECKINTERVAL)
    scheduler.add_job(clearSKUCache, 'cron', day_of_week='mon', hour=0, minute=0)
    scheduler.add_job(removeInvalidSKU, 'cron', day=1, hour=14, minute=0)

    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())