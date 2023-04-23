import ast
import asyncio
import json
import logging
import re
import zlib
from hashlib import md5
from datetime import datetime
from time import time
import urllib.parse
import requests
from curl_cffi import requests as curl
import crcmod.predefined

from bs4 import BeautifulSoup, Tag
from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import timezone
from pymongo import MongoClient

from config import CONNSTRING, DBNAME

def loadSettings():
    global TOKEN, ADMINCHATID, BESTDEALSCHATID, BESTDEALSMINPERCENTAGE
    global BESTDEALSWARNPERCENTAGE, CACHELIFETIME, ERRORMINTHRESHOLD, ERRORMAXDAYS
    global MAXITEMSPERUSER, CHECKINTERVAL, LOGCHATID, BANNERSTART, BANNERHELP
    global BANNERDONATE, BANNEROLDUSER, STORES, DEBUG, HTTPTIMEOUT

    db = MongoClient(CONNSTRING).get_database(DBNAME)
    settings = db.settings.find_one({'_id': 'settings'})

    TOKEN = settings['TOKEN']
    ADMINCHATID = settings['ADMINCHATID']
    BESTDEALSCHATID = settings['BESTDEALSCHATID']
    BESTDEALSMINPERCENTAGE = settings['BESTDEALSMINPERCENTAGE']
    BESTDEALSWARNPERCENTAGE = settings['BESTDEALSWARNPERCENTAGE']
    CACHELIFETIME = settings['CACHELIFETIME']
    ERRORMINTHRESHOLD = settings['ERRORMINTHRESHOLD']
    ERRORMAXDAYS = settings['ERRORMAXDAYS']
    MAXITEMSPERUSER = settings['MAXITEMSPERUSER']
    CHECKINTERVAL = settings['CHECKINTERVAL']
    LOGCHATID = settings['LOGCHATID']
    BANNERSTART = settings['BANNERSTART']
    BANNERHELP = settings['BANNERHELP']
    BANNERDONATE = settings['BANNERDONATE']
    BANNEROLDUSER = settings['BANNEROLDUSER']
    STORES = settings['STORES']
    DEBUG = settings['DEBUG']
    HTTPTIMEOUT = settings['HTTPTIMEOUT']


class LoggingMiddleware(BaseMiddleware):
    def __init__(self):
        super(LoggingMiddleware, self).__init__()

    async def on_pre_process_message(self, message: types.Message, data: dict):
        if message.text == '/start': return
        if message.chat.type != 'private': return

        db = MongoClient(CONNSTRING).get_database(DBNAME)
        chat_id = str(message.from_user.id)
        if not db.users.find_one({'_id': chat_id}):
            await message.answer(BANNEROLDUSER)
            data = {
                '_id': chat_id,
                'first_name': message.from_user.first_name,
                'last_name': message.from_user.last_name,
                'username': message.from_user.username,
                'enable': True
            }
            db.users.insert_one(data)


    async def on_post_process_message(self, message: types.Message, results, data: dict):
        await logMessage(message)


crc16 = crcmod.predefined.Crc('crc-16')

# Configure logging
logging.basicConfig(level=logging.INFO)

# settings
loadSettings()

# Initialize bot and dispatcher
bot = Bot(token=TOKEN, parse_mode='HTML')
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())


async def logMessage(message):
    if not LOGCHATID: return
    if message.from_user.id == ADMINCHATID: return

    username = ' (' + message.from_user.username + ')' if message.from_user.username else ''
    logentry = '<b>' + message.from_user.full_name + username + ':</b> ' + message.text
    await bot.send_message(LOGCHATID, logentry, disable_web_page_preview=True)


def getStoreUrls(activeonly):
    arr = []
    for key in STORES:
        if (activeonly and STORES[key]['active']) or not activeonly:
            arr.append(STORES[key]['url'])
    return arr


def getStoreKeys(activeonly):
    arr = []
    for key in STORES:
        if (activeonly and STORES[key]['active']) or not activeonly:
            arr.append(key)
    return arr



@dp.message_handler(commands='start', chat_type='private')
async def processCmdStart(message: types.Message):
    msg = substituteVars(BANNERSTART)
    await message.answer(msg)

    db = MongoClient(CONNSTRING).get_database(DBNAME)
    chat_id = str(message.from_user.id)
    data = {
        'first_name': message.from_user.first_name,
        'last_name': message.from_user.last_name,
        'username': message.from_user.username,
        'enable': True
    }
    db.users.update_one({'_id' : chat_id }, {'$set': data}, upsert=True)
    db.sku.update_many({'chat_id': chat_id}, {'$set': {'enable': True}})



@dp.message_handler(commands='bc', chat_id=ADMINCHATID)
async def processCmdBroadcast(message: types.Message):
    await message.answer('🟢 Начало рассылки')
    msg = message.get_args()
    msg_hash = md5(msg.encode('utf-8')).hexdigest()
    count = 0

    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'enable': True}
    for doc in db.users.find(query):
        count += 1
        if count % 100 == 0:
            await message.answer('Обработано: ' + str(count))

        await asyncio.sleep(0.1)
        if 'broadcasts' not in doc: doc['broadcasts'] = []
        if msg_hash in doc['broadcasts']: continue

        try:
            await bot.send_message(chat_id=doc['_id'], text=msg)
            doc['broadcasts'].append(msg_hash)
            db.users.update_one({'_id': doc['_id']}, {'$set': doc})
        except (exceptions.BotBlocked, exceptions.UserDeactivated):
            disableUser(doc['_id'])

    await message.answer('🔴 Окончание рассылки')



@dp.message_handler(commands='reload', chat_id=ADMINCHATID)
async def processCmdReload(message: types.Message):
    loadSettings()
    await message.answer('Settings sucessfully reloaded')


@dp.message_handler(regexp=r'(https://www\.bike-components\.de/\S+p(\d+)\/)', chat_type='private')
async def processBC(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'(https://www\.bike-components\.de/)(.+?)(/\S+p(\d+)\/)', message.text)
    if rg:
        url = rg.group(1) + 'en' + rg.group(3)
        await showVariants(store='BC', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp=r'(https?://www\.chainreactioncycles\.com/\S+/rp-prod(\d+))', chat_type='private')
async def processCRC(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'(https?://www\.chainreactioncycles\.com/\S+/rp-prod(\d+))', message.text)
    if rg:
        url = 'https://www.chainreactioncycles.com/en/rp-prod' + rg.group(2)
        await showVariants(store='CRC', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp=r'(https://www\.starbike\.com/en/\S+/)', chat_type='private')
async def processSB(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'(https://www\.starbike\.com/en/\S+)', message.text)
    if rg:
        url = rg.group(1)
        await showVariants(store='SB', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp=r'(https://www\.tradeinn\.com/\S+/\d+/p)', chat_type='private')
async def processTI(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'(https://www\.tradeinn\.com/.+?/)(.+?)(/\S+/\d+/p)', message.text)
    if rg:
        url = rg.group(1) + 'en' + rg.group(3)
        await showVariants(store='TI', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp=r'(https://www\.bike24\.com/p2(\d+)\.html)', chat_type='private')
async def processB24(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'(https://www\.bike24\.com/p2(\d+)\.html)', message.text)
    if rg:
        url = rg.group(1)
        await showVariants(store='B24', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp=r'https://www\.bike-discount\.de/.+?/[^?&\s]+', chat_type='private')
async def processBD(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'https://www\.bike-discount\.de/.+?/([^?&\s]+)', message.text)
    if rg:
        url = 'https://www.bike-discount.de/en/' + rg.group(1)
        await showVariants(store='BD', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp_commands=[r'^/add_\w+_\w+_\w+$'], chat_type='private')
async def processCmdAdd(message: types.Message):
    chat_id = str(message.from_user.id)
    params = message.text.split('_')
    store = params[1].upper()
    prodid = params[2]
    skuid = params[3]
    await addVariant(store, prodid, skuid, chat_id, message.message_id, 'reply')


@dp.message_handler(regexp_commands=[r'^/del_\w+_\w+_\w+$'], chat_type='private')
async def processCmdDel(message: types.Message):
    chat_id = str(message.from_user.id)
    docid = chat_id + '_' + message.text.replace('/del_', '').upper()
    query = {'_id': docid}
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    if db.sku.find_one(query):
        db.sku.delete_one(query)
        await message.answer('Удалено')
        return
    await message.answer('Какая-то ошибка 😧')


@dp.message_handler(commands='help', chat_type='private')
async def processCmdHelp(message: types.Message):
    msg = substituteVars(BANNERHELP)
    await message.answer(msg)


@dp.message_handler(commands='donate', chat_type='private')
async def processCmdDonate(message: types.Message):
    await message.answer(BANNERDONATE)


@dp.message_handler(commands='list', chat_type='private')
async def processCmdList(message: types.Message):
    chat_id = str(message.from_user.id)
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'chat_id': chat_id}
    if db.sku.count_documents(query) == 0:
        await message.answer('Ваш список пуст')
        return

    text_array = ['Отслеживаемые товары:']

    for doc in db.sku.find(query):
        key = doc['store'].lower() + '_' + doc['prodid'] + '_' + doc['skuid']
        line = getSkuString(doc, ['store', 'url', 'icon', 'price']) + '\n' + '<i>Удалить: /del_' + key + '</i>'
        text_array.append(line)

    await paginatedTgMsg(text_array, chat_id)


@dp.message_handler(commands='stat', chat_id=ADMINCHATID)
async def processCmdStat(message: types.Message):
    sent_msg = await message.answer('Getting stat...')

    db = MongoClient(CONNSTRING).get_database(DBNAME)

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
            '$count': 'count'
        }
    ])
    userswsku = docs.next()['count']
    skuactive = db.sku.count_documents({'enable': True})

    msg = ''
    msg += '<b>Total users:</b> ' + str(usersall) + '\n'
    msg += '<b>Enabled users:</b> ' + str(usersactive) + '\n'
    msg += '<b>Users with SKU:</b> ' + str(userswsku) + '\n'
    msg += '<b>Total SKU:</b> ' + str(skuall) + '\n'
    msg += '<b>Active SKU:</b> ' + str(skuactive) + '\n'

    for key in getStoreKeys(activeonly=False):
        num = db.sku.count_documents({'store': key})
        msg += '<b>' + key + ':</b> ' + str(num) + '\n'

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
    msg += '\n<b>Top ' + str(TOPNUMBER) + ' users:</b>\n'
    for doc in docs:
        username = ' (' + doc['username'] + ')' if doc['username'] else ''
        full_name = doc['first_name'] + ' ' + doc['last_name'] if doc['last_name'] else doc['first_name']
        msg += full_name + username + ': ' + str(doc['sku_count']) + '\n'

    await bot.edit_message_text(text=msg, message_id=sent_msg.message_id, chat_id=message.from_user.id)


async def sendOrEditMsg(msg, chat_id, message_id, msgtype):
    if msgtype == 'reply':
        await bot.send_message(chat_id=chat_id, text=msg, reply_to_message_id=message_id)
    if msgtype == 'edit':
        await bot.edit_message_text(text=msg, chat_id=chat_id, message_id=message_id)


async def showVariants(store, url, chat_id, message_id):
    msg = await bot.send_message(chat_id, '🔎 Ищу информацию о товаре...', reply_to_message_id=message_id)

    text_array = []
    variants = getVariants(store, url)
    if variants:
        first_skuid = list(variants)[0]
        if len(variants) == 1:
            prodid = variants[first_skuid]['prodid']
            await addVariant(store, prodid, first_skuid, chat_id, msg.message_id, 'edit')
            return

        text_array.append(variants[first_skuid]['name'])
        for skuid in sorted(variants):
            sku = variants[skuid]
            line = getSkuString(sku, ['icon', 'price']) + '\n<i>Добавить: /add_' + store.lower() + '_' +  sku['prodid'] + '_' + skuid + '</i>'
            text_array.append(line)
    else:
        text_array.append('Не смог найти цену 😧')

    await paginatedTgMsg(text_array, chat_id, msg.message_id)


async def addVariant(store, prodid, skuid, chat_id, message_id, msgtype):
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'chat_id': chat_id}
    if db.sku.count_documents(query) >= MAXITEMSPERUSER:
        await sendOrEditMsg('⛔️ Увы, в данный момент добавить можно не более ' + str(MAXITEMSPERUSER) + ' позиций', chat_id, message_id, msgtype)
        return

    docid = chat_id + '_' + store + '_' + prodid + '_' + skuid
    if db.sku.find_one({'_id': docid}):
        await sendOrEditMsg('️☝️ Товар уже есть в вашем списке', chat_id, message_id, msgtype)
        return

    url = getURL(store, prodid)
    if not url:
        await sendOrEditMsg('Какая-то ошибка 😧', chat_id, message_id, msgtype)
        return

    variants = getVariants(store, url)
    if not variants or skuid not in variants:
        await sendOrEditMsg('Какая-то ошибка 😧', chat_id, message_id, msgtype)
        return

    sku = variants[skuid]
    data = {
        '_id': docid,
        'chat_id': chat_id,
        'skuid': skuid,
        'prodid': sku['prodid'],
        'variant': sku['variant'],
        'url': sku['url'],
        'name': sku['name'],
        'price': sku['price'],
        'currency': sku['currency'],
        'store': sku['store'],
        'instock': sku['instock'],
        'errors': 0,
        'enable': True,
        'lastcheck': datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M'),
        'lastcheckts': int(time()),
        'lastgoodts': int(time()),
        'instock_prev': None,
        'price_prev': None
    }
    db.sku.insert_one(data)

    dispname = sku['variant']
    if not dispname: dispname = sku['name']

    await sendOrEditMsg(dispname + '\n✔️ Добавлено к отслеживанию', chat_id, message_id, msgtype)


def getURL(store, prodid):
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    doc = db.skucache.find_one({'_id': store + '_' + prodid})
    if doc:
        return doc['url']
    return None


def substituteVars(text):
    text = text.replace('%ACTIVESTOREURLS%', '\n'.join(getStoreUrls(activeonly=True)))
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

    if msg: await sendOrEditMsg()


def getVariants(store, url):
    tsexpired = int(time()) - CACHELIFETIME * 60
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'$and': [{'store': store},{'url': url},{'timestamp': {'$gt': tsexpired}}]}
    doc = db.skucache.find_one(query)
    if doc:
        return doc['variants']

    variants = globals()['parse' + store](url)
    if variants:
        cacheVariants(variants)
    return variants


async def clearSKUCache():
    tsexpired = int(time()) - CACHELIFETIME * 60
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'timestamp': {'$lt': tsexpired}}
    db.skucache.delete_many(query)


async def removeInvalidSKU():
    banner = 'ℹ️ Следующие позиции были удалены из вашего списка в связи с недоступностью более ' + str(ERRORMAXDAYS) + ' дней:'
    tsexpired = int(time()) - ERRORMAXDAYS * 24 * 3600
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'lastgoodts': {'$lt': tsexpired}}
    msgs = {}
    for doc in db.sku.find(query):
        user = db.users.find_one({'_id': doc['chat_id']})
        if not user['enable']: continue
        skustring = getSkuString(doc, ['store', 'url'])
        if doc['chat_id'] not in msgs:
            msgs[doc['chat_id']] = [banner]
        msgs[doc['chat_id']].append(skustring)

    db.sku.delete_many(query)

    for chatid in msgs:
        try:
            await paginatedTgMsg(msgs[chatid], chatid)
        except (exceptions.BotBlocked, exceptions.UserDeactivated):
            disableUser(chatid)
        await asyncio.sleep(0.1)


def parseB24(url):
    try:
        content = curl.get(url, impersonate='chrome110', timeout=HTTPTIMEOUT).text

        matches = re.search(r'window\.dataLayer\.push\(({\\"vpv.+?})\);', content, re.DOTALL)
        rawjson = matches.group(1)
        rawjson = rawjson.replace('\\"', '"')
        rawjson = rawjson.replace('\\\\"', '\\"')
        jsdata = json.loads(rawjson)
        instock = jsdata['isAvailable']
        availdict = {}
        for entry in jsdata['productOptionsAvailability']:
            arr = entry.replace('\/', '/').split('|')
            varname = arr[0].replace(':', '|')
            varcount = arr[1]
            availdict[varname] = varcount

        def findDataProps(tag):
            return tag.name == 'div' and tag.get('id') == 'add-to-cart'

        soup = BeautifulSoup(content, 'lxml')
        res = soup.find_all(findDataProps)
        jsdata = json.loads(res[0]['data-props'])
        price = int(float(jsdata['gtmData']['price']))
        prodid = str(jsdata['gtmData']['id'])
        name = jsdata['gtmData']['name'].replace('\/', '/')
        variant = jsdata['gtmData']['variant'].replace('\/', '/')
        currency = jsdata['productDetailPrice']['currencyCode']
        coeff = 1.191

        namesplit = name.split(' - ')
        if len(namesplit) > 1:
            name = namesplit[0]
            variant = ', '.join(namesplit[1:]) + (', ' + variant if variant else '')

        variants = {}

        if jsdata['productOptionList']:
            if len(jsdata['productOptionList']) == 1:
                for sku in jsdata['productOptionList'][0]['optionValueList']:
                    skuid = str(sku['id'])
                    variants[skuid] = {}
                    vartext = sku['name'].replace('not deliverable: ', '').replace(' - add {SURCHARGE}', '')
                    variants[skuid]['instock'] = False
                    if vartext in availdict:
                        variants[skuid]['instock'] = (availdict[vartext] != '0')
                    variants[skuid]['variant'] = ((variant + ', ' if variant else '') + vartext).replace('\/', '/').strip()
                    variants[skuid]['prodid'] = prodid
                    variants[skuid]['price'] = price + int(sku['surcharge']*coeff)
                    variants[skuid]['currency'] = currency
                    variants[skuid]['store'] = 'B24'
                    variants[skuid]['url'] = url
                    variants[skuid]['name'] = name
            if len(jsdata['productOptionList']) == 2:
                for sku1 in jsdata['productOptionList'][0]['optionValueList']:
                    for sku2 in jsdata['productOptionList'][1]['optionValueList']:
                        skuid = str(crc16.new((str(sku1['id']) + str(sku2['id'])).encode('utf-8')).crcValue)
                        variants[skuid] = {}
                        name1 = sku1['name'].replace('not deliverable: ', '').replace(' - add {SURCHARGE}', '')
                        name2 = sku2['name'].replace('not deliverable: ', '').replace(' - add {SURCHARGE}', '')
                        vartext = name1 + ' | ' + name2
                        variants[skuid]['instock'] = (availdict[name1] != '0' and availdict[name2] != '0')
                        variants[skuid]['variant'] = ((variant + ', ' if variant else '') + vartext).replace('\/', '/').strip()
                        variants[skuid]['prodid'] = prodid
                        variants[skuid]['price'] = price + int(sku1['surcharge']*coeff) + int(sku2['surcharge']*coeff)
                        variants[skuid]['currency'] = currency
                        variants[skuid]['store'] = 'B24'
                        variants[skuid]['url'] = url
                        variants[skuid]['name'] = name
            if len(jsdata['productOptionList']) > 2: return None # there are no examples yet
        else:
            variants['0'] = {}
            variants['0']['variant'] = variant
            variants['0']['prodid'] = prodid
            variants['0']['price'] = price
            variants['0']['currency'] = currency
            variants['0']['store'] = 'B24'
            variants['0']['url'] = url
            variants['0']['name'] = name
            variants['0']['instock'] = instock
    except Exception:
        return None

    return variants


def parseBD(url):
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    try:
        response = requests.get(url, headers=headers, timeout=HTTPTIMEOUT)
        url = response.url

        matches = re.search(r'dataLayer = \[(.+?)\]', response.text, re.DOTALL)
        jsdata = json.loads(matches.group(1))
        prodid = str(jsdata['productID'])
        currency = jsdata['productCurrency']

        matches = re.search(r'dataLayer.push \((.+?)\);', response.text, re.DOTALL)
        jsdata = json.loads(matches.group(1))['ecommerce']['detail']['products'][0]
        name = jsdata['brand'] + ' ' + jsdata['name']
        price = jsdata['price']

        def findVariants(tag):
            return tag.name == 'input' and tag.has_attr('class') and 'option--input' in tag['class']

        variants = {}
        soup = BeautifulSoup(response.text, 'lxml')
        res = soup.find_all(findVariants)
        if res:
            for x in res:
                skuid = x['value']
                variants[skuid] = {}
                variants[skuid]['variant'] = x['title']
                variants[skuid]['prodid'] = prodid
                variants[skuid]['price'] = int(float(x['price'])*0.841)
                variants[skuid]['currency'] = currency
                variants[skuid]['store'] = 'BD'
                variants[skuid]['url'] = url
                variants[skuid]['name'] = name
                variants[skuid]['instock'] = (x['stock-color'] in ['1', '6'])
        else:
            matches = re.search(r'<link itemprop="availability" href="https?://schema\.org/(.+?)"', response.text, re.DOTALL)
            instock = (matches.group(1) == 'InStock')

            variants['0'] = {}
            variants['0']['variant'] = ''
            variants['0']['prodid'] = prodid
            variants['0']['price'] = int(float(price)*0.841)
            variants['0']['currency'] = currency
            variants['0']['store'] = 'BD'
            variants['0']['url'] = url
            variants['0']['name'] = name
            variants['0']['instock'] = instock
    except Exception:
        return None

    return variants


def parseBC(url):
    headers = {}
    try:
        response = requests.get(url, headers=headers, timeout=HTTPTIMEOUT)
        url = response.url

        matches = re.search(r'({ \"@context\": \"https:\\/\\/schema\.org\", \"@type\": \"Product\".+?})</script>', response.text, re.DOTALL)
        variants = {}
        json = ast.literal_eval(matches.group(1))
        skus = json['offers']
        for sku in skus:
            skuid = sku['sku'].replace(str(json['sku']), '').replace('-', '')
            variants[skuid] = {}
            variants[skuid]['variant'] = sku['name'].replace('\/', '/')
            variants[skuid]['prodid'] = str(json['sku'])
            variants[skuid]['price'] = int(sku['priceSpecification']['price'])
            if 'True' in sku['priceSpecification']['valueAddedTaxIncluded']:
                variants[skuid]['price'] = int(sku['priceSpecification']['price']*0.84)
            variants[skuid]['currency'] = sku['priceSpecification']['priceCurrency']
            variants[skuid]['store'] = 'BC'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = (json['brand']['name'] + ' ' + json['name'].replace('\/', '/'))
            variants[skuid]['instock'] = 'InStock' in sku['availability']
    except Exception:
        return None

    return variants


def parseCRC(url):
    headers = {
        'Cookie': 'countryCode=KZ; languageCode=en; currencyCode=USD'
    }
    try:
        response = requests.get(url, headers=headers, timeout=HTTPTIMEOUT, verify=False)

        matches = re.search(r'"priceCurrency":\s+"(.+?)",', response.text, re.DOTALL)
        currency = matches.group(1)

        matches = re.search(r'window\.universal_variable\s+=\s+(.+?)</script>', response.text, re.DOTALL)
        universal = ast.literal_eval(matches.group(1))
        product = universal['product']
        prodid = product['id'].replace('prod', '')
        prodname = (product['manufacturer'] + ' ' + product['name']).replace('\\"', '"')

        matches = re.search(r'var\s+variantsAray\s+=\s+(\[.+?);', response.text, re.DOTALL)
        options = ast.literal_eval(matches.group(1))

        variants = {}
        matches = re.search(r'var\s+allVariants\s+=\s+({.+?);', response.text, re.DOTALL)
        skus = ast.literal_eval(matches.group(1))['variants']
        for sku in skus:
            skuid = sku['skuId'].replace('sku', '')
            variants[skuid] = {}
            varNameArray = []
            for option in options:
                if sku[option]: varNameArray.append(sku[option])
            variants[skuid]['variant'] = ', '.join(varNameArray)
            variants[skuid]['prodid'] = prodid
            variants[skuid]['price'] = int(re.sub(r'^\D*(\d+).*', r'\1', sku['RP']))
            variants[skuid]['currency'] = currency
            variants[skuid]['store'] = 'CRC'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = prodname
            variants[skuid]['instock'] = sku['isInStock'] == 'true'
    except Exception:
        return None

    return variants


def parseSB(url):
    headers = {
        'Cookie': 'country=KZ; currency_relaunch=EUR; vat=hide'
    }
    try:
        response = requests.get(url, headers=headers, timeout=HTTPTIMEOUT)

        skus = None
        name = None
        matches = re.search(r'<script type=\"application/ld\+json\">(.+?)</script>', response.text, re.DOTALL)
        jsdata = json.loads(matches.group(1))
        for x in jsdata:
            skus = x.get('offers')
            name = x.get('name')
            if skus and name: break
        if not skus: return None
        if not name: return None

        prodid = str(zlib.crc32(url.encode('utf-8')))

        variants = {}
        for sku in skus:
            skuid = sku['sku']
            if skuid is None: skuid = '0'
            variants[skuid] = {}
            variants[skuid]['variant'] = sku['name'].replace(name, '').strip()
            variants[skuid]['prodid'] = prodid
            tmp = sku['price'].split('.')
            if len(tmp) == 3: sku['price'] = tmp[0] + tmp[1]
            variants[skuid]['price'] = int(float(sku['price']))
            variants[skuid]['currency'] = sku['priceCurrency']
            variants[skuid]['store'] = 'SB'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = name
            variants[skuid]['instock'] = (sku['availability'] == 'InStock')
    except Exception:
        return None

    return variants


def parseTI(url):
    headers = {
        'Cookie': 'id_pais=164'
    }
    url = url.replace(chr(160), '')
    url = urllib.parse.quote(url, safe=':/')
    try:
        response = requests.get(url, headers=headers, timeout=HTTPTIMEOUT)
        url = response.url

        matches = re.search(r'https://www.tradeinn.com/.+/(\d+)/p', url, re.DOTALL)
        prodid = matches.group(1)

        soup = BeautifulSoup(response.text, 'lxml')
        res = soup.find_all('h1', {'class': 'productName'})
        name = res[0].string

        def findVariants(tag):
            return tag.parent.get('id') == 'tallas_detalle'

        res = soup.find_all(findVariants)
        if not res: return None

        varnames = {}
        for child in res:
            varid = child['value']
            varnames[varid] = child.string

        res = soup.find_all(itemtype='http://schema.org/Offer')
        if not res: return None

        variants = {}
        for x in res:
            skuid = None
            price = None
            instock = None
            currency = None

            for child in x.children:
                if not isinstance(child, Tag): continue
                if child.get('itemprop') == 'sku':
                    skuid = child['content']
                if child.get('itemprop') == 'price':
                    price = child['content']
                if child.get('itemprop') == 'availability':
                    instock = child['href'] == 'http://schema.org/InStock'
                if child.get('itemprop') == 'priceCurrency':
                    currency = child['content']

            if not (skuid and price and instock and currency): continue
            if skuid not in varnames: continue

            variants[skuid] = {}
            variants[skuid]['variant'] = varnames[skuid]
            variants[skuid]['prodid'] = prodid
            variants[skuid]['price'] = int(float(price))
            variants[skuid]['currency'] = currency
            variants[skuid]['store'] = 'TI'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = name
            variants[skuid]['instock'] = instock
    except Exception:
        return None

    return variants


def getSkuString(sku, options):
    instock = sku['instock']
    url = sku['url']
    name = sku['name']
    variant = sku['variant']
    price = str(sku['price'])
    currency = sku['currency']
    store = sku['store']
    errors = sku['errors'] if 'errors' in sku else 0

    storename = ''
    urlname = ''
    icon = ''
    pricetxt = ''

    if 'url' in options:
        urlname = '<a href="' + url + '">' + name + '</a>' + '\n'
    if 'icon' in options:
        icon = '✅ ' if instock else '🚫 '
        if errors > ERRORMINTHRESHOLD: icon = '⚠️ '
    if 'store' in options:
        storename = '<code>[' + store + ']</code> '
    if 'price' in options:
        pricetxt = ' <b>' + price + ' ' + currency + '</b>'

    return storename + urlname + icon + (variant + pricetxt).strip()


def cacheVariants(variants):
    first_sku = variants[list(variants)[0]]
    docid = first_sku['store'] + '_' + first_sku['prodid']
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    data = {
        'variants': variants,
        'timestamp': int(time()),
        'url': first_sku['url'],
        'store': first_sku['store']
    }
    db.skucache.update_one({'_id': docid}, {'$set': data}, upsert=True)


async def notify():
    def addMsg(msg):
        if doc['chat_id'] in msgs:
            msgs[doc['chat_id']].append(msg)
        else:
            msgs[doc['chat_id']] = [msg]

    msgs = {}
    bestdeals = {}

    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'$or': [{'price_prev': {'$ne': None}},{'instock_prev': {'$ne': None}}], 'enable': True}
    for doc in db.sku.find(query):
        await asyncio.sleep(0.1)
        if not db.sku.find_one({'_id': doc['_id']}): continue
        skustring = getSkuString(doc, ['store', 'url', 'price'])

        if not doc['instock_prev'] is None:
            if doc['instock']:
                addMsg('✅ Снова в наличии!\n' + skustring)
            if not doc['instock']:
                addMsg('🚫 Не в наличии\n' + skustring)

        if not doc['price_prev'] is None and doc['instock']:
            if doc['price'] < doc['price_prev']:
                addMsg('📉 Снижение цены!\n' + skustring + ' (было: ' + str(doc['price_prev']) + ' ' + doc['currency'] + ')')
                if doc['price_prev'] != 0:
                    percents = int((1 - doc['price']/float(doc['price_prev']))*100)
                    if percents >= BESTDEALSMINPERCENTAGE:
                        bdkey = doc['store'] + '_' + doc['prodid'] + '_' + doc['skuid']
                        if bdkey not in bestdeals:
                            bestdeals[bdkey] = skustring + ' (было: ' + str(doc['price_prev']) + ' ' + doc['currency'] + ') ' + str(percents) + '%'
                            if percents >= BESTDEALSWARNPERCENTAGE: bestdeals[bdkey] = bestdeals[bdkey] + '‼️'
            if doc['price'] > doc['price_prev']:
                addMsg('📈 Повышение цены\n' + skustring + ' (было: ' + str(doc['price_prev']) + ' ' + doc['currency'] + ')')

        doc['price_prev'] = None
        doc['instock_prev'] = None
        db.sku.update_one({'_id': doc['_id']}, {'$set': doc})

    for chatid in msgs:
        try:
            await paginatedTgMsg(msgs[chatid], chatid)
        except (exceptions.BotBlocked, exceptions.UserDeactivated):
            disableUser(chatid)
        if DEBUG and LOGCHATID: await paginatedTgMsg(msgs[chatid], LOGCHATID)
        await asyncio.sleep(0.1)

    if BESTDEALSCHATID: await paginatedTgMsg(bestdeals.values(), BESTDEALSCHATID)


def disableUser(chat_id):
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    db.users.update_one({'_id': chat_id}, {'$set': {'enable': False}}, upsert=True)
    db.sku.update_many({'chat_id': chat_id}, {'$set': {'enable': False}})


async def checkSKU():
    now = int(time())

    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'$and': [{'enable': True},{'lastcheckts': {'$lt': now - CHECKINTERVAL * 60}}]}
    for doc in db.sku.find(query):
        await asyncio.sleep(0.1)
        if not db.sku.find_one({'_id': doc['_id']}): continue

        # increase check interval for inactive SKU
        days_inactive = (now - doc['lastgoodts'])/86400
        if days_inactive >= 1 and doc['lastcheckts'] >= now - (CHECKINTERVAL * 60 + days_inactive * 3600):
            continue

        logging.info(doc['_id'] + ' [' + doc['name'] + '][' + doc['variant'] + ']...')

        variants = getVariants(doc['store'], doc['url'])
        if variants and doc['skuid'] in variants:
            sku = variants[doc['skuid']]
            if sku['instock'] != doc['instock']:
                doc['instock_prev'] = doc['instock']

            price_threshold = STORES[doc['store']]['price_threshold']
            if sku['currency'] == doc['currency']:
                if doc['price']*price_threshold < abs(sku['price'] - doc['price']):
                    doc['price_prev'] = doc['price']

            doc['instock'] = sku['instock']
            doc['currency'] = sku['currency']
            doc['price'] = sku['price']
            doc['errors'] = 0
            doc['lastgoodts'] = int(time())
        else:
            doc['errors'] += 1

        doc['lastcheck'] = datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M')
        doc['lastcheckts'] = int(time())
        db.sku.update_one({'_id': doc['_id']}, {'$set': doc})


async def errorsMonitor():
    bad = {}
    good = {}
    db = MongoClient(CONNSTRING).get_database(DBNAME)
    query = {'lastcheckts': {'$gt': int(time()) - CHECKINTERVAL*60}}
    for doc in db.sku.find(query):
        store = doc['store']
        errors = doc['errors']
        if not store in good: good[store] = 0
        if not store in bad: bad[store] = 0
        if errors == 0:
            good[store] += 1
        else:
            bad[store] += 1

    for store in good:
        if not STORES[store]['active']: continue
        if good[store] == 0 or bad[store]/float(good[store]) > 0.8:
            await bot.send_message(ADMINCHATID, 'Problem with ' + store + '!\nGood: ' + str(good[store]) + '\nBad: ' + str(bad[store]))


if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    scheduler.start()

    scheduler.add_job(checkSKU, 'interval', minutes=5)
    scheduler.add_job(notify, 'interval', minutes=5)
    scheduler.add_job(errorsMonitor, 'interval', minutes=CHECKINTERVAL)
    scheduler.add_job(clearSKUCache, 'cron', day_of_week='mon', hour=0, minute=0)
    scheduler.add_job(removeInvalidSKU, 'cron', day=1, hour=14, minute=0)

    executor.start_polling(dp, skip_updates=True)