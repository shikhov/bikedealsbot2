import asyncio
import logging
import re
from hashlib import md5
from datetime import datetime
from time import time
from collections import defaultdict

from aiogram import Bot, Dispatcher, executor
from aiogram.types import Message
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from pymongo import MongoClient, UpdateOne

import parsing

from config import CONNSTRING, DBNAME
db = MongoClient(CONNSTRING).get_database(DBNAME)

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



def loadSettings():
    global TOKEN, ADMINCHATID, BESTDEALSCHATID, BESTDEALSMINPERCENTAGE, BESTDEALSMINVALUE
    global BESTDEALSWARNPERCENTAGE, CACHELIFETIME, ERRORMINTHRESHOLD, ERRORMAXDAYS
    global MAXITEMSPERUSER, CHECKINTERVAL, LOGCHATID, BANNERSTART, BANNERHELP
    global BANNERDONATE, STORES, DEBUG, HTTPTIMEOUT, REQUESTDELAY, LOGFILTER

    settings = db.settings.find_one({'_id': 'settings'})

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
    def __init__(self):
        super(LoggingMiddleware, self).__init__()

    async def on_pre_process_message(self, message: Message, data: dict):
        if message.text == '/start': return
        if message.chat.type != 'private': return

        chat_id = str(message.from_user.id)
        if not db.users.find_one({'_id': chat_id}):
            data = {
                '_id': chat_id,
                'first_name': message.from_user.first_name,
                'last_name': message.from_user.last_name,
                'username': message.from_user.username,
                'enable': True
            }
            db.users.insert_one(data)


    async def on_post_process_message(self, message: Message, results, data: dict):
        await logMessage(message)


# Configure logging
logging.basicConfig(level=logging.INFO)

# settings
loadSettings()

# Initialize bot and dispatcher
bot = Bot(token=TOKEN, parse_mode='HTML')
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())


async def logMessage(message: Message):
    if not LOGCHATID: return
    if message.from_user.id == ADMINCHATID: return
    if message.text in LOGFILTER: return

    username = ' (' + message.from_user.username + ')' if message.from_user.username else ''
    logentry = '<b>' + message.from_user.full_name + username + ':</b> ' + message.text
    await bot.send_message(LOGCHATID, logentry, disable_web_page_preview=True)


def getStoreUrls():
    arr = []
    for store in STORES.values():
        status = '' if store['active'] else ' <i>(временно недоступен)</i>'
        arr.append(store['url'] + status)
    return arr


@dp.message_handler(commands='start', chat_type='private')
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
    db.users.update_one({'_id' : chat_id }, {'$set': data}, upsert=True)
    db.sku.update_many({'chat_id': chat_id}, {'$set': {'enable': True}})


async def broadcast(message: Message, text, docs):
    text_hash = md5(text.encode('utf-8')).hexdigest()
    await message.answer('🟢 Начало рассылки')

    for count, doc in enumerate(docs, start=1):
        if count % 100 == 0:
            await message.answer('Обработано: ' + str(count))
        if text_hash in doc.setdefault('broadcasts', []): continue

        try:
            await bot.send_message(chat_id=doc['_id'], text=text)
            doc['broadcasts'].append(text_hash)
            db.users.update_one({'_id': doc['_id']}, {'$set': doc})
        except (exceptions.BotBlocked, exceptions.UserDeactivated):
            disableUser(doc['_id'])
        await asyncio.sleep(0.1)

    await message.answer('🔴 Окончание рассылки')


@dp.message_handler(commands='bc', chat_id=ADMINCHATID)
async def processCmdBroadcast(message: Message):
    text = message.get_args()
    docs = db.users.find({'enable': True})
    await broadcast(message, text, docs)


@dp.message_handler(regexp_commands=[r'^/bc_\w+'], chat_id=ADMINCHATID)
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


@dp.message_handler(commands='reload', chat_id=ADMINCHATID)
async def processCmdReload(message: Message):
    loadSettings()
    await message.answer('Settings successfully reloaded')


@dp.message_handler(regexp=r'https?://', chat_type='private')
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

    await showVariants(store, url, str(message.from_user.id), message.message_id)
    return


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

    return None


@dp.message_handler(regexp_commands=[r'^/add_\w+_\w+_\w+$'], chat_type='private')
async def processCmdAdd(message: Message):
    chat_id = str(message.from_user.id)
    params = message.text.split('_')
    store = params[1].upper()
    prodid = params[2]
    skuid = params[3]
    await addVariant(store, prodid, skuid, chat_id, message.message_id, 'reply')


@dp.message_handler(regexp_commands=[r'^/del_\w+_\w+_\w+$'], chat_type='private')
async def processCmdDel(message: Message):
    chat_id = str(message.from_user.id)
    docid = chat_id + '_' + message.text.replace('/del_', '').upper()
    query = {'_id': docid}
    if db.sku.delete_one(query).deleted_count == 1:
        await message.answer('Удалено')
        return
    await message.answer('Какая-то ошибка 😧')


@dp.message_handler(commands='help', chat_type='private')
async def processCmdHelp(message: Message):
    msg = substituteVars(BANNERHELP)
    await message.answer(msg)


@dp.message_handler(commands='donate', chat_type='private')
async def processCmdDonate(message: Message):
    await message.answer(BANNERDONATE)


@dp.message_handler(commands='list', chat_type='private')
async def processCmdList(message: Message):
    text_array = []
    chat_id = str(message.from_user.id)
    query = {'chat_id': chat_id}
    for doc in db.sku.find(query):
        key = doc['store'].lower() + '_' + doc['prodid'] + '_' + doc['skuid']
        line = getSkuString(doc, ['store', 'url', 'icon', 'price']) + f'\n<i>Удалить: /del_{key}</i>'
        text_array.append(line)

    if text_array:
        text_array = ['Отслеживаемые товары:'] + text_array
    else:
        text_array = ['Ваш список пуст']

    await paginatedTgMsg(text_array, chat_id)


@dp.message_handler(commands='stat', chat_id=ADMINCHATID)
async def processCmdStat(message: Message):
    sent_msg = await message.answer('Getting stat...')

    usersall = db.users.count_documents({})
    usersactive = db.users.count_documents({'enable': True})
    skuall = db.sku.count_documents({})
    docs = db.users.aggregate([
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
    ])
    userswsku = docs.next()['count']
    skuactive = db.sku.count_documents({'enable': True})

    msg = ''
    msg += f'<b>Total users:</b> {usersall}\n'
    msg += f'<b>Enabled users:</b> {usersactive}\n'
    msg += f'<b>Enabled users with SKU:</b> {userswsku}\n'
    msg += f'<b>Total SKU:</b> {skuall}\n'
    msg += f'<b>Active SKU:</b> {skuactive}\n'

    for key in STORES.keys():
        num = db.sku.count_documents({'store': key})
        msg += f'<b>{key}:</b> {num}\n'

    TOPNUMBER = 10
    docs = db.users.aggregate([
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
    ])
    msg += f'\n<b>Top {TOPNUMBER} users:</b>\n'
    for doc in docs:
        username = ' (' + doc['username'] + ')' if doc['username'] else ''
        full_name = doc['first_name'] + ' ' + doc['last_name'] if doc['last_name'] else doc['first_name']
        msg += f'{full_name}{username}: {doc["sku_count"]}\n'

    await bot.edit_message_text(text=msg, message_id=sent_msg.message_id, chat_id=message.from_user.id)


@dp.message_handler(chat_type='private')
async def processSearch(message: Message):
    text = message.text
    if not text: return

    chat_id = str(message.from_user.id)
    query = {'chat_id': chat_id}
    if db.sku.count_documents(query) == 0:
        await message.answer('⚠️ Ваш список пуст, поиск невозможен')
        return

    try:
        pattern = re.compile(text, re.I)
    except Exception:
        await message.reply('⚠️ Некорректное выражение')
        return

    query = {'chat_id': chat_id, 'name': {'$regex': pattern}}
    text_array = []
    for doc in db.sku.find(query):
        key = doc['store'].lower() + '_' + doc['prodid'] + '_' + doc['skuid']
        line = getSkuString(doc, ['store', 'url', 'icon', 'price']) + f'\n<i>Удалить: /del_{key}</i>'
        text_array.append(line)

    header = f'Результаты поиска по строке <b>{text}</b>:'
    text_array = [header] + (text_array or ['Ничего не найдено'])
    await paginatedTgMsg(text_array, chat_id)


async def sendOrEditMsg(text, chat_id, message_id, msgtype):
    if msgtype == 'reply':
        await bot.send_message(chat_id=chat_id, text=text, reply_to_message_id=message_id)
    if msgtype == 'edit':
        await bot.edit_message_text(text=text, chat_id=chat_id, message_id=message_id)


async def showVariants(store, url, chat_id, message_id):
    msg = await bot.send_message(chat_id, '🔎 Ищу информацию о товаре...', reply_to_message_id=message_id)

    prod = await getProduct(store, url)
    if prod.var_count == 0:
        await bot.edit_message_text('Не смог найти цену 😧', chat_id, msg.message_id)
    elif prod.var_count == 1:
        await addVariant(store, prod.id, prod.first_skuid, chat_id, msg.message_id, 'edit')
    elif prod.var_count > 1:
        await paginatedTgMsg(prod.getSkuAddList(), chat_id, msg.message_id)


async def addVariant(store, prodid, skuid, chat_id, message_id, msgtype):
    query = {'chat_id': chat_id}
    if db.sku.count_documents(query) >= MAXITEMSPERUSER:
        await sendOrEditMsg(f'⛔️ Увы, в данный момент добавить можно не более {MAXITEMSPERUSER} позиций', chat_id, message_id, msgtype)
        return

    docid = chat_id + '_' + store + '_' + prodid + '_' + skuid
    if db.sku.find_one({'_id': docid}):
        await sendOrEditMsg('️☝️ Товар уже есть в вашем списке', chat_id, message_id, msgtype)
        return

    url = getURL(store, prodid)
    if not url:
        await sendOrEditMsg('Какая-то ошибка 😧', chat_id, message_id, msgtype)
        return

    prod = await getProduct(store, url)
    if not prod.hasSku(skuid):
        await sendOrEditMsg('Какая-то ошибка 😧', chat_id, message_id, msgtype)
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
    db.sku.insert_one(data)

    dispname = data['variant'] or data['name']
    await sendOrEditMsg(dispname + '\n✔️ Добавлено к отслеживанию', chat_id, message_id, msgtype)


def getURL(store, prodid):
    doc = db.skucache.find_one({'_id': store + '_' + prodid})
    if doc:
        return doc['url']
    return None


def substituteVars(text):
    text = text.replace('%STOREURLS%', '\n'.join(getStoreUrls()))
    return text


async def paginatedTgMsg(text_array, chat_id, message_id=0, delimiter='\n\n'):
    async def sendOrEditMsg():
        if message_id != 0 and first_page:
            await bot.edit_message_text(msg, chat_id, message_id, disable_web_page_preview=True)
        else:
            await bot.send_message(chat_id, msg, disable_web_page_preview=True)

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
    doc = db.skucache.find_one(query)
    if doc:
        return Product(data=doc['variants'], source='cache')

    parseFunction = getattr(parsing, 'parse' + store)
    variants = await parseFunction(url, HTTPTIMEOUT)
    cacheVariants(url, variants)
    return Product(data=variants, source='web')


async def clearSKUCache():
    tsexpired = int(time()) - CACHELIFETIME * 60
    query = {'timestamp': {'$lt': tsexpired}}
    db.skucache.delete_many(query)


async def removeInvalidSKU():
    banner = f'ℹ️ Следующие позиции были удалены из вашего списка в связи с недоступностью более {ERRORMAXDAYS} дней:'
    tsexpired = int(time()) - ERRORMAXDAYS * 24 * 3600
    query = {'lastgoodts': {'$lt': tsexpired}}
    messages = {}
    for doc in db.sku.find(query):
        user = db.users.find_one({'_id': doc['chat_id']})
        if not user['enable']: continue
        skustring = getSkuString(doc, ['store', 'url'])
        messages.setdefault(doc['chat_id'], [banner]).append(skustring)

    db.sku.delete_many(query)

    for chat_id in messages:
        try:
            await paginatedTgMsg(messages[chat_id], chat_id)
        except (exceptions.BotBlocked, exceptions.UserDeactivated):
            disableUser(chat_id)
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


def cacheVariants(url, variants):
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
    db.skucache.update_one(query, {'$set': data}, upsert=True)


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
    for doc in db.sku.find(query):
        if doc['instock_prev'] is not None:
            skustring = getSkuString(doc, ['store', 'url', 'price'])
            if doc['instock']:
                addMsg('✅ Снова в наличии!\n' + skustring)
            if not doc['instock']:
                addMsg('🚫 Не в наличии\n' + skustring)

        if doc['price_prev'] is not None and doc['instock']:
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
        except (exceptions.BotBlocked, exceptions.UserDeactivated):
            disableUser(chat_id)
        if DEBUG and LOGCHATID:
            await paginatedTgMsg(messages[chat_id], LOGCHATID)
        await asyncio.sleep(0.1)

    if BESTDEALSCHATID:
        await paginatedTgMsg(bestdeals.values(), BESTDEALSCHATID)

    if bulk_request:
        db.sku.bulk_write(bulk_request)


def disableUser(chat_id):
    db.users.update_one({'_id': chat_id}, {'$set': {'enable': False}}, upsert=True)
    db.sku.update_many({'chat_id': chat_id}, {'$set': {'enable': False}})


async def checkSKU():
    now = int(time())
    query = {'enable': True, 'lastcheckts': {'$lt': now - CHECKINTERVAL * 60}}
    result = db.sku.find(query)
    prodlist = list(set([doc['store_prodid'] for doc in result]))
    docs = db.sku.find({'store_prodid': {'$in': prodlist}, 'enable': True}).sort('store_prodid')
    for doc in docs:
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
            db.sku.update_one({'_id': doc['_id']}, {'$set': doc})
        except Exception:
            pass
        if prod.source == 'web':
            await asyncio.sleep(REQUESTDELAY)


async def errorsMonitor():
    bad = defaultdict(int)
    good = defaultdict(int)
    query = {'lastcheckts': {'$gt': int(time()) - CHECKINTERVAL*60}}
    for doc in db.sku.find(query):
        if doc['errors'] == 0:
            good[doc['store']] += 1
        else:
            bad[doc['store']] += 1

    for store in good:
        if not STORES[store]['active']: continue
        good_count = good[store]
        bad_count = bad[store]
        if good_count == 0 or bad_count/float(good_count) > 0.8:
            await bot.send_message(ADMINCHATID, f'Problem with {store}!\nGood: {good_count}\nBad: {bad_count}')


if __name__ == '__main__':
    scheduler = AsyncIOScheduler(job_defaults={'misfire_grace_time': None})
    scheduler.start()

    scheduler.add_job(checkSKU, 'interval', minutes=5)
    scheduler.add_job(notify, 'interval', minutes=5)
    scheduler.add_job(errorsMonitor, 'interval', minutes=CHECKINTERVAL)
    scheduler.add_job(clearSKUCache, 'cron', day_of_week='mon', hour=0, minute=0)
    scheduler.add_job(removeInvalidSKU, 'cron', day=1, hour=14, minute=0)

    executor.start_polling(dp, skip_updates=True)