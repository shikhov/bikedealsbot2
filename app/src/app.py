import ast
import asyncio
import json
import logging
import os
import re
from datetime import datetime
from time import time
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup

from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.utils import exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from cloudant.adapters import Replay429Adapter
from cloudant.client import Cloudant
from cloudant.query import Query
from logdna import LogDNAHandler
from pytz import timezone
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from fake_useragent import UserAgent

from pyppeteer import launch

from config import DBSETTINGS, DBSKU, DBSKUCACHE, DBUSERS

curdir = os.path.dirname(os.path.abspath(__file__))
credsfile = os.path.join(curdir, 'creds.json')
creds = json.load(open(credsfile))
DB_APIKEY = creds['apikey']
DB_URL = creds['url']

def getDb(dbname):
    return Cloudant.iam(None, DB_APIKEY, url=DB_URL, connect=True, adapter=Replay429Adapter(retries=10, initialBackoff=0.01))[dbname]

def loadSettings():
    global TOKEN, ADMINCHATID, BESTDEALSCHATID, BESTDEALSMINPERCENTAGE
    global BESTDEALSWARNPERCENTAGE, CACHELIFETIME, ERRORMINTHRESHOLD, ERRORMAXDAYS
    global MAXITEMSPERUSER, CHECKINTERVAL, APPNAME, LOGDNAKEY

    db = getDb(DBSETTINGS)
    settings = db['settings']

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
    APPNAME = settings['APPNAME']
    LOGDNAKEY = settings['LOGDNAKEY']

class LoggingMiddleware(BaseMiddleware):
    def __init__(self):
        super(LoggingMiddleware, self).__init__()

    async def on_post_process_message(self, message: types.Message, results, data: dict):
        logMessage(message)


# Configure logging
logging.basicConfig(level=logging.INFO)

# settings
loadSettings()

# Initialize bot and dispatcher
bot = Bot(token=TOKEN, parse_mode='HTML')
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

# LogDNA
options = {
    'app': APPNAME,
    'index_meta': True
}
logger = logging.getLogger('logdna')
logger.setLevel(logging.INFO)
logger.addHandler(LogDNAHandler(LOGDNAKEY, options))

# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument("--headless")
# chrome_options.add_argument(f'user-agent={UserAgent().random}')
# chrome_options.add_argument("--window-size=1920x1080")

# driver = webdriver.Remote(
#     command_executor='http://jolly_elion:4444/wd/hub',
#     options=chrome_options)


def logMessage(message):
    meta = {
        'chat_id': message.from_user.id
    }
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name
    username = ' (' + message.from_user.username + ')' if message.from_user.username else ''
    dispname = first_name + ' ' + last_name if last_name else first_name
    logger.info(dispname + username + ': ' + message.text, {'meta': meta})


@dp.message_handler(commands='start', chat_type='private')
async def processCmdStart(message: types.Message):
    msg = '️Присылайте мне ссылки на товары из веломагазинов, а я буду отслеживать их цены и наличие 😉 '
    msg += 'Поддерживаются:\nchainreactioncycles.com\nbike-components.de'
    await message.answer(msg)

    chat_id = str(message.from_user.id)

    db = getDb(DBUSERS)
    doc = get_or_create(db, chat_id)
    doc['first_name'] = message.from_user.first_name
    doc['last_name'] = message.from_user.last_name
    doc['username'] = message.from_user.username
    doc['enable'] = True
    doc.save()

    db = getDb(DBSKU)
    selector = {'chatid': chat_id}
    docs = Query(db, selector=selector)()['docs']
    for entry in docs:
        doc = get_or_create(db, entry['_id'])
        doc['enable'] = True
        doc.save()


@dp.message_handler(commands='reload', chat_id=ADMINCHATID)
async def processCmdReload(message: types.Message):
    loadSettings()
    await message.answer('Settings sucessfully reloaded')


@dp.message_handler(regexp=r'(https://www\.bike-components\.de/\S+p(\d+)\/)', chat_type='private')
async def processBC(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'(https://www\.bike-components\.de/\S+p(\d+)\/)', message.text)
    if rg:
        url = rg.group(1)
        await showVariants(store='BC', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp=r'(https?://www\.chainreactioncycles\.com/\S+/rp-prod(\d+))', chat_type='private')
async def processCRC(message: types.Message):
    chat_id = str(message.from_user.id)

    rg = re.search(r'(https?://www\.chainreactioncycles\.com/\S+/rp-prod(\d+))', message.text)
    if rg:
        url = 'https://www.chainreactioncycles.com/en/rp-prod' + rg.group(2)
        await showVariants(store='CRC', url=url, chat_id=chat_id, message_id=message.message_id)


@dp.message_handler(regexp=r'(https://www\.bike24\.com/p2(\d+)\.html)', chat_type='private')
async def processB24(message: types.Message):
    # await message.answer('К сожалению, Bike24 в настоящее время не поддерживается')
    # return

    chat_id = str(message.from_user.id)

    rg = re.search(r'(https://www\.bike24\.com/p2(\d+)\.html)', message.text)
    if rg:
        url = rg.group(1)
        await showVariants(store='B24', url=url, chat_id=chat_id, message_id=message.message_id)




@dp.message_handler(regexp=r'(https://www\.bike-discount\.de/.+?/[^?&]+)', chat_type='private')
async def processBD(message: types.Message):
    await message.answer('К сожалению, bike-discount в настоящее время не поддерживается')
    return

    chat_id = str(message.from_user.id)

    rg = re.search(r'(https://www\.bike-discount\.de/.+?/[^?&]+)', message.text)
    if rg:
        url = rg.group(1)
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
    db = getDb(DBSKU)
    if docid in db:
        db[docid].delete()
        await message.answer('Удалено')
        return
    await message.answer('Какая-то ошибка 😧')


@dp.message_handler(commands='list', chat_type='private')
async def processCmdList(message: types.Message):
    chat_id = str(message.from_user.id)
    db = getDb(DBSKU)
    selector = {'chatid': chat_id}
    docs = Query(db, selector=selector)()['docs']
    if len(docs) == 0:
        await message.answer('Ваш список пуст')
        return

    text_array = ['Отслеживаемые товары:']

    for doc in docs:
        key = doc['store'].lower() + '_' + doc['prodid'] + '_' + doc['skuid']
        line = getSkuString(doc, ['store', 'url', 'icon', 'price']) + '\n' + '<i>Удалить: /del_' + key + '</i>'
        text_array.append(line)

    await paginatedTgMsg(text_array, chat_id)


@dp.message_handler(commands='stat', chat_id=ADMINCHATID)
async def processCmdStat(message: types.Message):
    sent_msg = await message.answer('Getting stat...')

    db = getDb(DBUSERS)
    usersall = len(Query(db, selector={'_id': {'$gt': '0'}})()['docs'])
    users = len(Query(db, selector={'enable': True})()['docs'])

    db = getDb(DBSKU)
    docs = Query(db, selector={'_id': {'$gt': '0'}})()['docs']
    sku = len(docs)
    skubyusers = {}
    for doc in docs: skubyusers[doc['chatid']] = 'foo'
    userswsku = len(skubyusers.keys())
    skuactive = len(Query(db, selector={'enable': True})()['docs'])
    crc = len(Query(db, selector={'store': 'CRC'})()['docs'])
    bc = len(Query(db, selector={'store': 'BC'})()['docs'])
    b24 = len(Query(db, selector={'store': 'B24'})()['docs'])
    bd = len(Query(db, selector={'store': 'BD'})()['docs'])

    msg = ''
    msg += '<b>Total users:</b> ' + str(usersall) + '\n'
    msg += '<b>Enabled users:</b> ' + str(users) + '\n'
    msg += '<b>Users with SKU:</b> ' + str(userswsku) + '\n'
    msg += '<b>Total SKU:</b> ' + str(sku) + '\n'
    msg += '<b>Active SKU:</b> ' + str(skuactive) + '\n'
    msg += '<b>CRC:</b> ' + str(crc) + '\n'
    msg += '<b>BC:</b> ' + str(bc) + '\n'
    msg += '<b>B24:</b> ' + str(b24) + '\n'
    msg += '<b>BD:</b> ' + str(bd) + '\n'

    await bot.edit_message_text(text=msg, message_id=sent_msg.message_id, chat_id=message.from_user.id)


async def sendOrEditMsg(msg, chat_id, message_id, msgtype):
    if msgtype == 'reply':
        await bot.send_message(chat_id=chat_id, text=msg, reply_to_message_id=message_id)
    if msgtype == 'edit':
        await bot.edit_message_text(text=msg, chat_id=chat_id, message_id=message_id)


def get_or_create(db, docid):
    if docid in db:
        return db[docid]
    return db.create_document({'_id': docid})


async def showVariants(store, url, chat_id, message_id):
    msg = await bot.send_message(chat_id, '🔎 Ищу информацию о товаре...', reply_to_message_id=message_id)

    text_array = []
    variants = await getVariants(store, url)
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
    db = getDb(DBSKU)

    selector = {'chatid': chat_id}
    docs = Query(db, selector=selector)()['docs']
    if len(docs) >= MAXITEMSPERUSER:
        await sendOrEditMsg('⛔️ Увы, в данный момент добавить можно не более ' + str(MAXITEMSPERUSER) + ' позиций', chat_id, message_id, msgtype)
        return

    docid = chat_id + '_' + store + '_' + prodid + '_' + skuid
    if docid in db:
        await sendOrEditMsg('️☝️ Товар уже есть в вашем списке', chat_id, message_id, msgtype)
        return

    url = getURL(store, prodid)
    if not url:
        await sendOrEditMsg('Какая-то ошибка 😧', chat_id, message_id, msgtype)
        return

    variants = await getVariants(store, url)
    if not variants or skuid not in variants:
        await sendOrEditMsg('Какая-то ошибка 😧', chat_id, message_id, msgtype)
        return

    sku = variants[skuid]
    dbsku = get_or_create(db, docid)
    dbsku['chatid'] = chat_id
    dbsku['skuid'] = skuid
    dbsku['prodid'] = sku['prodid']
    dbsku['variant'] = sku['variant']
    dbsku['url'] = sku['url']
    dbsku['name'] = sku['name']
    dbsku['price'] = sku['price']
    dbsku['currency'] = sku['currency']
    dbsku['store'] = sku['store']
    dbsku['instock'] = sku['instock']
    dbsku['errors'] = 0
    dbsku['enable'] = True
    dbsku['lastcheck'] = datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M')
    dbsku['lastgoodts'] = int(time())
    dbsku['lastcheckts'] = int(time())
    dbsku['instock_prev'] = None
    dbsku['price_prev'] = None
    dbsku.save()

    dispname = sku['variant']
    if not dispname: dispname = sku['name']

    await sendOrEditMsg(dispname + '\n✔️ Добавлено к отслеживанию', chat_id, message_id, msgtype)


def getURL(store, prodid):
    db = getDb(DBSKUCACHE)
    docid = store + '_' + prodid
    if docid in db:
        return db[docid]['url']
    return None


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


async def getVariants(store, url):
    tsexpired = int(time()) - CACHELIFETIME * 60
    db = getDb(DBSKUCACHE)
    selector = {'$and': [{'store': store},{'url': url},{'timestamp': {'$gt': tsexpired}}]}
    docs = Query(db, selector=selector)(limit=1)['docs']
    if len(docs) > 0:
        return docs[0]['variants']

    # if store == 'B24':
    #     return await parseB24(url)

    return await globals()['parse' + store](url)


async def parseB24(url):


    # try:
    #     chrome_options = webdriver.ChromeOptions()
    #     chrome_options.add_argument("--headless")
    #     chrome_options.add_argument(f'user-agent={UserAgent().random}')
    #     chrome_options.add_argument("--window-size=1920x1080")

    #     driver = webdriver.Remote(
    #         command_executor='http://jolly_elion:4444/wd/hub',
    #         options=chrome_options)
    #     driver.get(url)
    # except Exception:
    #     return None
    # finally:    
    #     driver.quit()

    start_parm = {
            # "executablePath": '/usr/bin/chromium',
            "headless": True,
            "args": [
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
                '--no-sandbox',
            ],
    }

    browser = await launch(**start_parm)
    page = await browser.newPage()
    await page.goto(url)

    for i in range(6):
        content = await page.content()
        matches = re.search(r'dataLayer =\s+\[(.+?)\];', content, re.DOTALL)
        if matches: break
        asyncio.sleep(1)
        logging.info(str(i) + ' second...')

    if not matches: return None

    # jsdata = json.loads(matches.group(1).decode('unicode-escape'))
    jsdata = json.loads(matches.group(1).replace(r'\"', '"'))
    if 'productOptionsAvailability' not in jsdata: return None

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
    if not res: return None
    if not res[0].get('data-props'): return None

    jsdata = json.loads(res[0]['data-props'])
    price = int(float(jsdata['gtmData']['price']))
    prodid = str(jsdata['gtmData']['id'])
    name = jsdata['gtmData']['name'].replace('\/', '/')
    variant = jsdata['gtmData']['variant'].replace('\/', '/')
    currency = jsdata['productDetailPrice']['currencyCode']

    namesplit = name.split(' - ')
    if len(namesplit) > 1:
        name = namesplit[0]
        variant = ', '.join(namesplit[1:]) + (', ' + variant if variant else '')

    variants = {}

    if jsdata['productOptionList']:
        for sku in jsdata['productOptionList'][0]['optionValueList']:
            skuid = str(sku['id'])
            variants[skuid] = {}
            vartext = sku['name'].replace('not deliverable: ', '').replace(' - add {SURCHARGE}', '')
            variants[skuid]['instock'] = False
            if vartext in availdict:
                variants[skuid]['instock'] = (availdict[vartext] != '0')
            variants[skuid]['variant'] = ((variant + ', ' if variant else '') + vartext).replace('\/', '/').strip()
            variants[skuid]['prodid'] = prodid
            variants[skuid]['price'] = price + int(sku['surcharge'])
            variants[skuid]['currency'] = currency
            variants[skuid]['store'] = 'B24'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = name
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

    cacheVariants(variants)
    return variants


def parseBD(url):
    return None


async def parseBC(url):
    try:
        content = urlopen(url).read().decode('utf-8')
    except Exception:
        return None

    matches = re.search(r'({ \"@context\": \"https:\\/\\/schema\.org\", \"@type\": \"Product\".+?})</script>', content, re.DOTALL)
    if not matches: return None

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

    cacheVariants(variants)
    return variants


async def parseCRC(url):
    headerslist = {
        'RUB': {'User-Agent': 'Mozilla/5.0', 'Cookie': 'countryCode=RU; languageCode=en; currencyCode=RUB'},
        'GBP': {'User-Agent': 'Mozilla/5.0', 'Cookie': 'countryCode=GB; languageCode=en; currencyCode=GBP'}}

    for currency in headerslist:
        req = Request(url)
        headers = headerslist[currency]
        for header in headers:
            req.add_header(header, headers[header])
        try:
            content = urlopen(req).read().decode('utf-8')
        except Exception:
            return None

        matches = re.search(r'window\.universal_variable\s+=\s+(.+?)</script>', content, re.DOTALL)
        if not matches: continue

        universal = ast.literal_eval(matches.group(1))
        if not ('product' in universal and universal['product']['price']): continue

        product = universal['product']
        prodid = product['id'].replace('prod', '')
        prodname = product['manufacturer'] + ' ' + product['name']

        matches = re.search(r'var\s+variantsAray\s+=\s+(\[.+?);', content, re.DOTALL)
        if not matches: continue

        options = ast.literal_eval(matches.group(1))

        matches = re.search(r'var\s+allVariants\s+=\s+({.+?);', content, re.DOTALL)
        if not matches: continue

        variants = {}
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

        cacheVariants(variants)
        return variants
    return None


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

    return storename + urlname + icon + variant + pricetxt


def cacheVariants(variants):
    first_sku = variants[list(variants)[0]]
    db = getDb(DBSKUCACHE)
    dbsku = get_or_create(db, first_sku['store'] + '_' + first_sku['prodid'])
    dbsku['variants'] = variants
    dbsku['timestamp'] = int(time())
    dbsku['url'] = first_sku['url']
    dbsku['store'] = first_sku['store']
    dbsku.save()


async def notify():
    def addMsg(msg):
        if doc['chatid'] in msgs:
            msgs[doc['chatid']].append(msg)
        else:
            msgs[doc['chatid']] = [msg]

    msgs = {}
    bestdeals = {}

    db = getDb(DBSKU)
    selector = {'$or': [{'price_prev': {'$ne': None}},{'instock_prev': {'$ne': None}}], 'enable': True}
    docs = Query(db, selector=selector)()['docs']
    for entry in docs:
        await asyncio.sleep(0.1)
        doc = get_or_create(db, entry['_id'])
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
        doc.save()

    for chatid in msgs:
        try:
            await paginatedTgMsg(msgs[chatid], chatid)
        except (exceptions.BotBlocked, exceptions.UserDeactivated):
            disableUser(chatid)
        await asyncio.sleep(0.1)

    if BESTDEALSCHATID: await paginatedTgMsg(bestdeals.values(), BESTDEALSCHATID)


def disableUser(chat_id):
    db = getDb(DBUSERS)
    user = get_or_create(db, str(chat_id))
    user['enable'] = False
    user.save()

    db = getDb(DBSKU)
    selector = {'chatid': chat_id}
    docs = Query(db, selector=selector)()['docs']
    for entry in docs:
        doc = get_or_create(db, entry['_id'])
        doc['enable'] = False
        doc.save()


async def checkSKU():
    now = int(time())

    db = getDb(DBSKU)
    selector = {'$and': [{'enable': True},{'lastcheckts': {'$lt': now - CHECKINTERVAL * 60}}]}
    docs = Query(db, selector=selector)()['docs']
    for entry in docs:
        await asyncio.sleep(0.1)
        doc = get_or_create(db, entry['_id'])

        # increase check interval for inactive SKU
        days_inactive = (now - doc['lastgoodts'])/86400
        if days_inactive >= 1 and doc['lastcheckts'] >= now - (CHECKINTERVAL * 60 + days_inactive * 3600):
            continue

        logging.info(doc['_id'] + ' [' + doc['name'] + '][' + doc['variant'] + ']...')

        variants = await getVariants(doc['store'], doc['url'])
        if variants and doc['skuid'] in variants:
            sku = variants[doc['skuid']]
            if sku['instock'] != doc['instock']:
                doc['instock_prev'] = doc['instock']
            else:
                doc['instock_prev'] = None
            if sku['price'] != doc['price']:
                doc['price_prev'] = doc['price']
            else:
                doc['price_prev'] = None

            doc['instock'] = sku['instock']
            doc['price'] = sku['price']
            doc['errors'] = 0
            doc['lastgoodts'] = int(time())
        else:
            doc['errors'] += 1

        doc['lastcheck'] = datetime.now(timezone('Asia/Yekaterinburg')).strftime('%d.%m.%Y %H:%M')
        doc['lastcheckts'] = int(time())
        doc.save()


async def errorsMonitor():
    bad = {}
    good = {}
    db = getDb(DBSKU)
    selector = {'lastcheckts': {'$gt': int(time()) - CHECKINTERVAL*60}}
    docs = Query(db, selector=selector)()['docs']
    for doc in docs:
        store = doc['store']
        errors = doc['errors']
        if not store in good: good[store] = 0
        if not store in bad: bad[store] = 0
        if errors == 0:
            good[store] += 1
        else:
            bad[store] += 1

    for store in good:
        if good[store] == 0 or bad[store]/float(good[store]) > 0.8:
            await bot.send_message(ADMINCHATID, 'Problem with ' + store + '!\nGood: ' + str(good[store]) + '\nBad: ' + str(bad[store]))


if __name__ == '__main__':
    scheduler = AsyncIOScheduler()
    scheduler.start()

    scheduler.add_job(checkSKU, 'interval', seconds=300)
    scheduler.add_job(notify, 'interval', seconds=300)
    scheduler.add_job(errorsMonitor, 'interval', seconds=CHECKINTERVAL*60)

    executor.start_polling(dp, skip_updates=True)