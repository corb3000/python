# From this blog:
# https://scrapfly.io/blog/how-to-scrape-rightmove/#full-rightmove-scraper-code


import asyncio
import json
from typing import List, TypedDict
from urllib.parse import urlencode
from httpx import AsyncClient, Response
import jmespath
from typing_extensions import TypedDict
import sqlite3
import time

# 1. establish HTTP client with browser-like headers to avoid being blocked
client = AsyncClient(
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,lt;q=0.8,et;q=0.7,de;q=0.6",
    },
    follow_redirects=True,
    http2=True,  # enable http2 to reduce block chance
    timeout=30,
)


async def find_locations(query: str) -> List[str]:
    """use rightmove's typeahead api to find location IDs. Returns list of location IDs in most likely order"""
    # rightmove uses two character long tokens so "cornwall" becomes "CO/RN/WA/LL"
    tokenize_query = "".join(c + ("/" if i % 2 == 0 else "") for i, c in enumerate(query.upper(), start=1))
    url = f"https://www.rightmove.co.uk/typeAhead/uknostreet/{tokenize_query.strip('/')}/"
    response = await client.get(url)
    data = json.loads(response.text)
    return [prediction["locationIdentifier"] for prediction in data["typeAheadLocations"]]


async def scrape_search(location_id: str) -> dict:
    RESULTS_PER_PAGE = 24

    def make_url(offset: int) -> str:
        url = "https://www.rightmove.co.uk/api/_search?"
        params = {
            "areaSizeUnit": "sqft",
            "channel": "BUY",  # BUY or RENT
            "currencyCode": "GBP",
            "includeSSTC": "false",
            "index": offset,  # page offset
            "isFetching": "false",
            "locationIdentifier": location_id, #e.g.: "REGION^61294", 
            "numberOfPropertiesPerPage": RESULTS_PER_PAGE,
            "radius": "0.0",
            "sortType": "6",
            "viewType": "LIST",
            "minPrice": "500000",
            "maxPrice": "800000",
            "propertyTypes": ["detached", "land"]
        }
        url_param =(url + urlencode(params , doseq=True))
        # print(url_param)
        return url_param
    start = 0
    first_page = await client.get(make_url(0))
    first_page_data = json.loads(first_page.content)
    total_results = int(first_page_data['resultCount'].replace(',', ''))
    print(f"total results= {total_results}")
    print(first_page.status_code)
    results = first_page_data['properties']
    
    other_pages = []

    for offset in range(start + RESULTS_PER_PAGE, total_results, RESULTS_PER_PAGE):
        next_page = await client.get(make_url(offset))
        print(next_page.status_code, offset)
        if next_page.status_code == 200:
            data = json.loads(next_page.text)
            results.extend(data['properties'])

        # else:
        #     time.sleep(10)
        #     next_page = await client.get(make_url(offset))
        #     if next_page.status_code == 200:
        #         data = json.loads(next_page.text)
        #         results.extend(data['properties'])
        #     else:
        #         print("failed second time")
    return results

# Example run:
async def run():
    county = "somerset"
    cornwall_id = (await find_locations(county))[0]
    print(cornwall_id)
    # cornwall_id = "REGION^61322" # somerset as find location dose not seem to work
    cornwall_results = await scrape_search(cornwall_id)

    parse_map = {
        # from top area of the page: basic info, videos and photos
        "id": "id",
        "bedrooms": "bedrooms",
        "bathrooms": "bathrooms",
        "type": "transactionType",
        "property_type": "propertySubType",
        "description": "summary",
        "price": "price.amount",
        "address": "displayAddress",
        "listingUpdateDate": "listingUpdate.listingUpdateDate",
        "listingUpdateReason": "listingUpdate.listingUpdateReason",
        "latitude": "location.latitude",
        "longitude": "location.longitude",
        "url": "propertyUrl",
        "propertyImages": "propertyImages.images"
    }
    conn = sqlite3.connect('houses.db')
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS house(
        id INT PRIMARY KEY,
        bedrooms INT,
        bathrooms INT,
        type TEXT,
        property_type TEXT,
        description TEXT,
        price REAL,
        address TEXT,
        latitude REAL,
        longitude REAL,
        url TEXT,
        listingUpdateDate TEXT,
        listingUpdateReason TEXT,
        propertyImages,
        county,
        density_1k REAL,
        density_3k REAL,
        density_5k REAL);
    """)
    conn.commit()
    results = {}
    for property in cornwall_results:
        for key, path in parse_map.items():
            value = jmespath.search(path, property)
            if type(value) is list:
                value = json.dumps(value)
        
            results[key] = value     
        results["county"] = county
        query = 'INSERT or REPLACE INTO house ({}) VALUES ({})'.format(
            ','.join(results.keys()),
            ','.join(['?']*len(results)))
        # print(results)

        cur.execute(query, tuple(results.values()))
        conn.commit()
    conn.close()


if __name__ == "__main__":
    asyncio.run(run())