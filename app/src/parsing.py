import json
import re
import urllib.parse

import crcmod.predefined
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup, Tag
from curl_cffi import requests as curl

from app import STATUS_OK, STATUS_TIMEOUTERROR, STATUS_PARSINGERROR

crc16 = crcmod.predefined.Crc('crc-16')
crc32 = crcmod.predefined.Crc('crc-32')


async def parseSB(url, httptimeout):
    headers = {
        'Cookie': 'country=KZ; currency_relaunch=EUR; vat=hide'
    }
    timeout = ClientTimeout(total=httptimeout)
    try:
        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                content = await response.text()
                url = str(response.url)

        soup = BeautifulSoup(content, 'lxml')
        prodid = str(crc32.new(url.encode('utf-8')).crcValue)
        name = soup.find('title').text

        def findVarnames(tag):
            return tag.name == 'a' and 'meta-id' in tag.attrs

        varnames = {}
        for x in soup.find_all(findVarnames):
            varnames[x['meta-id']] = x.text.strip()

        instock = {}
        for x in soup.find_all('span', {'class': 'dropdownbox-eta'}):
            instock[x['meta-id']] = False if 'uk-text-danger' in x['class'] else True

        variants = {}
        for x in soup.find_all('span', {'class': 'dropdownbox-price'}):
            if len(varnames) == 1:
                skuid = '0'
                variant = ''
            else:
                skuid = x['meta-id']
                variant = varnames[x['meta-id']]
            variants[skuid] = {}
            variants[skuid]['variant'] = variant
            variants[skuid]['prodid'] = prodid
            pricetxt = re.sub(r'[^0-9.]', '', x.text)
            variants[skuid]['price'] = int(float(pricetxt))
            variants[skuid]['currency'] = 'EUR'
            variants[skuid]['store'] = 'SB'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = name
            variants[skuid]['instock'] = instock[x['meta-id']]

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}


async def parseB24(url, httptimeout):
    try:
        async with curl.AsyncSession() as session:
            response = await session.get(url, impersonate='safari15_5', timeout=httptimeout)
            content = response.text

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
            if len(jsdata['productOptionList']) > 2:
                raise Exception
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

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}


async def parseTI(url, httptimeout):
    id_pais = 164
    headers = {
        'Cookie': f'id_pais={id_pais}',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
        'Host': 'www.tradeinn.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.8,ru;q=0.5,ru-RU;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br'
    }
    timeout = ClientTimeout(total=httptimeout)
    url = url.replace(chr(160), '')
    url = urllib.parse.quote(url, safe=':/')

    try:
        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                url = str(response.url)

        rg = re.search(r'(https://www\.tradeinn\.com/)(.+?)/(.+?)(/\S+/)(\d+)/p', url)
        url = rg.group(1) + 'bikeinn/en' + rg.group(4) + rg.group(5) + '/p'
        prodid = rg.group(5)
        jsurl = f'https://dc.tradeinn.com/{prodid}'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.8,ru;q=0.5,ru-RU;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': url,
            'Origin': 'https://www.tradeinn.com'
        }

        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(jsurl) as response:
                jscontent = await response.text()

        jsdata = json.loads(jscontent)['_source']
        name = jsdata['marca'] + ' ' + jsdata['model']['eng']
        variants = {}
        for var in jsdata['productes']:
            if not var['sellers']: continue
            prices = {x['id_pais']: x['precio'] for s in var['sellers'] for x in s['precios_paises']}
            if id_pais not in prices: continue

            skuid = var['id_producte']
            variants[skuid] = {}
            varname = filter(None, [var['talla'], var['talla2'], var['color']])
            variants[skuid]['variant'] = ' '.join(varname)
            variants[skuid]['prodid'] = prodid
            variants[skuid]['price'] = int(prices[id_pais])
            variants[skuid]['currency'] = 'RUB'
            variants[skuid]['store'] = 'TI'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = name
            variants[skuid]['instock'] = True

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}


async def parseBC(url, httptimeout):
    headers = {}
    timeout = ClientTimeout(total=httptimeout)
    try:
        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                content = await response.text()
                url = str(response.url)

        def findVariants(tag):
            return tag.name == 'script' and tag.get('type') == 'application/ld+json'

        soup = BeautifulSoup(content, 'lxml')
        res = soup.find_all(findVariants)
        jsdata = {}
        variants = {}
        for x in res:
            jsdata = json.loads(x.text)
            if isinstance(jsdata, list):
                break

        for x in jsdata:
            if x['@type'] == 'Product':
                skus = x['offers']
                for sku in skus:
                    skuid = sku['sku'].replace(str(x['sku']), '').replace('-', '')
                    variants[skuid] = {}
                    variants[skuid]['variant'] = sku['name'].replace('\/', '/')
                    variants[skuid]['prodid'] = str(x['sku'])
                    ps = sku['priceSpecification'][0]
                    variants[skuid]['price'] = int(ps['price'])
                    if 'True' in ps['valueAddedTaxIncluded']:
                        variants[skuid]['price'] = int(ps['price']*0.84)
                    variants[skuid]['currency'] = ps['priceCurrency']
                    variants[skuid]['store'] = 'BC'
                    variants[skuid]['url'] = url
                    variants[skuid]['name'] = (x['brand']['name'] + ' ' + x['name'].replace('\/', '/'))
                    variants[skuid]['instock'] = 'InStock' in sku['availability']
                break

            if x['@type'] == 'ProductGroup':
                skus = x['hasVariant']
                for sku in skus:
                    skuid = sku['sku'].replace(str(x['productGroupID']), '').replace('-', '')
                    variants[skuid] = {}
                    variants[skuid]['variant'] = sku['name'].replace('\/', '/')
                    variants[skuid]['prodid'] = str(x['productGroupID'])
                    ps = sku['offers']['priceSpecification'][0]
                    variants[skuid]['price'] = int(ps['price'])
                    if 'True' in ps['valueAddedTaxIncluded']:
                        variants[skuid]['price'] = int(ps['price']*0.84)
                    variants[skuid]['currency'] = ps['priceCurrency']
                    variants[skuid]['store'] = 'BC'
                    variants[skuid]['url'] = url
                    variants[skuid]['name'] = (x['brand']['name'] + ' ' + x['name'].replace('\/', '/'))
                    variants[skuid]['instock'] = 'InStock' in sku['offers']['availability']
                break

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}


async def parseBD(url, httptimeout):
    try:
        async with curl.AsyncSession() as session:
            response = await session.get(url, impersonate='safari15_5', timeout=httptimeout)
            content = response.text
            url = response.url

        matches = re.search(r'dataLayer = \[(.+?)\]', content, re.DOTALL)
        if not matches:
            matches = re.search(r'dataLayer.push\((.+?)\);', content, re.DOTALL)
        jsdata = json.loads(matches.group(1))
        prodid = str(jsdata['productID'])
        currency = jsdata['productCurrency']

        matches = re.search(r'dataLayer.push \((.+?)\);', content, re.DOTALL)
        jsdata = json.loads(matches.group(1))['ecommerce']['detail']['products'][0]
        name = jsdata['brand'] + ' ' + jsdata['name']
        price = jsdata['price']

        def findVariants(tag):
            return tag.name == 'input' and tag.has_attr('class') and 'option--input' in tag['class']

        variants = {}
        soup = BeautifulSoup(content, 'lxml')
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
            matches = re.search(r'<link itemprop="availability" href="https?://schema\.org/(.+?)"', content, re.DOTALL)
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

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}


async def parseCRC(url, httptimeout):
    def getVarName(variant):
        tmp = []
        attrs = {x['name']: x for x in variant['attributes']}
        for filterableAttribute in jsbody['filterableAttributes']:
            key = filterableAttribute['name']
            if not attrs.get(key): continue
            varAttValue = attrs.get(key)['value']
            if isinstance(varAttValue, dict):
                tmp.append(varAttValue['label'])
            else:
                tmp.append(varAttValue)

        return ', '.join(tmp)

    headers = {
        'Cookie': 'countryCode=KZ; languageCode=en; currencyCode=USD'
    }
    timeout = ClientTimeout(total=httptimeout)
    try:
        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                content = await response.text()
                url = str(response.url)

        matches = re.search(r'type="application/json">(.+)</script>', content, re.DOTALL)
        jsdata = json.loads(matches.group(1))
        jsbody = jsdata['props']['pageProps']['renderGraph']['page']['components']['body'][0]
        jsvariants = jsbody['variants']

        variants = {}
        for variant in jsvariants:
            skuid = str(crc16.new((variant['sku']).encode('utf-8')).crcValue)
            variants[skuid] = {}
            variants[skuid]['variant'] = getVarName(variant)
            variants[skuid]['prodid'] = jsbody['key']
            variants[skuid]['price'] = int(variant['price']['current']['centAmount']/100)
            variants[skuid]['currency'] = variant['price']['current']['currencyCode']
            variants[skuid]['store'] = 'CRC'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = jsbody['name']
            variants[skuid]['instock'] = variant['stockLevel']['inStock']

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}