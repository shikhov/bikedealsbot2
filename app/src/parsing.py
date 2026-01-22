import json
import re
import urllib.parse
from itertools import product

import crcmod.predefined
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
from curl_cffi import requests as curl

from app import STATUS_OK, STATUS_TIMEOUTERROR, STATUS_PARSINGERROR

crc16 = crcmod.predefined.Crc('crc-16')
crc32 = crcmod.predefined.Crc('crc-32')


async def parseSB(url, httptimeout):
    headers = {
        'Cookie': 'country=RU; currency_relaunch=EUR; vat=hide'
    }
    try:
        async with curl.AsyncSession() as session:
            response = await session.get(url, impersonate='safari15_5', timeout=httptimeout, headers=headers)
            content = response.text
            url = response.url

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
            response = await session.get(url, impersonate='firefox', timeout=httptimeout)
            content = response.text

        soup = BeautifulSoup(content, 'lxml')
        res = soup.find('div', {'id': 'add-to-cart'})
        jsdata = json.loads(res['data-props'])

        price = int(float(jsdata['ga4GtmData']['price']))
        prodid = str(jsdata['ga4GtmData']['item_id'])
        name = jsdata['ga4GtmData']['item_name']
        currency = jsdata['productDetailPrice']['currencyCode']
        coeff = 1.191

        jsurl = f'https://www.bike24.com/api/product/{prodid}/availability?deliveryCountryId=4&zipCode='
        async with curl.AsyncSession() as s:            
            response = await s.get(jsurl, impersonate='firefox', timeout=httptimeout)        
            availdata = response.text

        availdict = {}
        availjson = json.loads(availdata)
        for key, value in availjson['availabilityVariantsList'].items():
            if ',' in key:
                skuid_parts = key.split(',')
                tmp = []
                for part in skuid_parts:                    
                    tmp.append(part.split('=')[-1])
                s = '_'.join(sorted(tmp)).encode('utf-8')
                skuid = str(crc16.new(s).crcValue)
            elif '=' in key:
                skuid = key.split('=')[-1]
            else:
                skuid = key
            availdict[skuid] = value['availability']['currentStock'] > 0

        variants = {}

        if jsdata['productOptionList']:            
            options = jsdata['productOptionList']
            lists = [opt['optionValueList'] for opt in options]
            combos = {}
            for combo in product(*lists):
                ids = []
                optnames = []
                surcharge = 0
                for x in combo:
                    optname = x['name'].replace('not deliverable: ', '').replace(' - add {SURCHARGE}', '')
                    optnames.append(optname)
                    ids.append(str(x['id']))                    
                    surcharge += x['surcharge']

                if len(ids) > 1:                      
                    s = "_".join(sorted(ids)).encode('utf-8')
                    key = str(crc16.new(s).crcValue)
                else:
                    key = ids[0]                    

                combos[key] = {
                    "variant": ', '.join(optnames),
                    "surcharge": surcharge
                }

            for skuid, sku in combos.items():
                variants[skuid] = {}
                variants[skuid]['instock'] = availdict[skuid]
                variants[skuid]['variant'] = sku['variant']
                variants[skuid]['prodid'] = prodid
                variants[skuid]['price'] = price + int(sku['surcharge']*coeff)
                variants[skuid]['currency'] = currency
                variants[skuid]['store'] = 'B24'
                variants[skuid]['url'] = url
                variants[skuid]['name'] = name
        else:
            variants['0'] = {}
            variants['0']['variant'] = ""
            variants['0']['prodid'] = prodid
            variants['0']['price'] = price
            variants['0']['currency'] = currency
            variants['0']['store'] = 'B24'
            variants['0']['url'] = url
            variants['0']['name'] = name
            variants['0']['instock'] = availdict[prodid]

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
                    psdict = {p['priceType']: p for p in sku['priceSpecification']}
                    ps = psdict.get('https://schema.org/SalePrice') or psdict.get('https://schema.org/ListPrice')
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
                    psdict = {p['priceType']: p for p in sku['offers']['priceSpecification']}
                    ps = psdict.get('https://schema.org/SalePrice') or psdict.get('https://schema.org/ListPrice')
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

        matches = re.search(r'dataLayer.push\((\{"event":.+?)\);', content, re.DOTALL)
        jsdata = json.loads(matches.group(1))['ecommerce']['items'][0]
        name = jsdata['item_brand'] + ' ' + jsdata['item_name']
        prodid = str(crc32.new(url.encode('utf-8')).crcValue)
        
        variants = {}
        soup = BeautifulSoup(content, 'lxml')
        res = soup.find('form', {'data-nele-variant-data': True})

        if res:
            jsdata = json.loads(res.get('data-nele-variant-data'))
            varnames = {x['id']: x['translated']['name'] for x in jsdata['configuratorSettings'][0]['options']}
            for offer in jsdata['siblings']:
                option_id = offer['optionIds'][0]
                varname = varnames[option_id]
                skuid = str(crc16.new(varname.encode('utf-8')).crcValue)
                variants[skuid] = {}
                variants[skuid]['variant'] = varname
                variants[skuid]['prodid'] = prodid
                variants[skuid]['price'] = int(offer['calculatedPrice']['unitPrice'])
                variants[skuid]['currency'] = 'EUR'
                variants[skuid]['store'] = 'BD'
                variants[skuid]['url'] = url
                variants[skuid]['name'] = name
                variants[skuid]['instock'] = offer['available']
        else:
            res = soup.find('script', {'type': 'application/ld+json'})
            jsdata = json.loads(res.string)[0]
            offer = jsdata['offers'][0]
            variants['0'] = {}
            variants['0']['variant'] = ''
            variants['0']['prodid'] = prodid
            variants['0']['price'] = int(offer['price'])
            variants['0']['currency'] = offer['priceCurrency']
            variants['0']['store'] = 'BD'
            variants['0']['url'] = url
            variants['0']['name'] = jsdata['brand']['name'] + ' ' + jsdata['name']
            variants['0']['instock'] = (offer['availability'] != 'https://schema.org/OutOfStock')

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


async def parseA4C(url, httptimeout):
    headers = {}
    timeout = ClientTimeout(total=httptimeout)
    try:
        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                content = await response.text()
                url = str(response.url)

        prodid = str(crc32.new(url.encode('utf-8')).crcValue)
        matches = re.search(r'variants: (.+?)};(.*)?</script>', content, re.DOTALL)
        jsraw = matches.group(1).rstrip().rstrip(',')
        jsraw = '{"variants": ' + jsraw + '}'
        jsdata = json.loads(jsraw)
        variants = {}
        for x in jsdata['variants']:
            skuid = str(crc16.new(str(x['id']).encode('utf-8')).crcValue)
            variants[skuid] = {}
            variants[skuid]['variant'] = x['title']
            variants[skuid]['prodid'] = prodid
            variants[skuid]['price'] = int(x['price']/100)
            variants[skuid]['currency'] = 'EUR'
            variants[skuid]['store'] = 'A4C'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = x['name'].split(' - ')[0]
            variants[skuid]['instock'] = x['available']

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}


async def parseLG(url, httptimeout):
    headers = {}
    timeout = ClientTimeout(total=httptimeout)
    try:
        async with ClientSession(headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                content = await response.text()
                url = str(response.url)

        def findData(tag):
            return tag.name == 'article' and tag.get('id') == 'product-new'

        soup = BeautifulSoup(content, 'lxml')
        res = soup.find_all(findData)
        jsdata = json.loads(res[0]['data-json'])
        variants = {}
        for x in jsdata['options']:
            skuid = str(x['sourceId'])
            variants[skuid] = {}
            variants[skuid]['variant'] = (', ').join(sorted(x['attributes'].values()))
            variants[skuid]['prodid'] = str(jsdata['originId'])
            variants[skuid]['price'] = int(x['price']['price0'])
            variants[skuid]['currency'] = 'USD'
            variants[skuid]['store'] = 'LG'
            variants[skuid]['url'] = url
            variants[skuid]['name'] = jsdata['title']
            variants[skuid]['instock'] = x['quantity'] > 0

        return {'status': STATUS_OK, 'variants': variants}
    except TimeoutError:
        return {'status': STATUS_TIMEOUTERROR, 'variants': None}
    except Exception:
        return {'status': STATUS_PARSINGERROR, 'variants': None}