import pandas as pd
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from lxml import html
import os
from tqdm import tqdm

from typing import List, Dict, Optional


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
]

def get_description(tree) -> str:

    p_text = tree.xpath("//section[contains(@id,'postingbody')]//text()")
    clean_text = [t.strip() for t in p_text if t.strip()]
    description = "\n".join(clean_text)

    return description


def get_zip_code(tree) -> str:
    p_text = tree.xpath("//h2[@class='street-address']")
    if not p_text: return None

    clean_text = p_text[0].text_content().strip().split(" ")

    if not clean_text: return None

    zip_code = clean_text[-1]

    return zip_code


def get_bathroom(tree) -> Optional[int]:
    text = tree.xpath("//span[@class='attr important']")
    if not text: return None

    clean = text[0].text_content().strip()

    if not clean: return None

    ba_index = clean.find("Ba")
    if ba_index != -1:
        bathrooms = clean[ba_index-1]
        if bathrooms.isdigit():
            return bathrooms

    return None



def process_listing(url, session):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    response = session.get(url=url, headers=headers)
    tree = html.fromstring(response.text)
    
    zip_code = get_zip_code(tree)
    desc = get_description(tree)
    bathrooms = get_bathroom(tree)

    result = {
        "url": url,
        "zip code": zip_code,
        "description": desc,
        "bathrooms": bathrooms
    }

    print(result)
    return result

def download_all_sites(sites: list):
    session = requests.Session()
    records: List[Dict] = []
    with ThreadPoolExecutor(max_workers=min(32, os.cpu_count()+4)) as executor:
        futures = [executor.submit(process_listing, url, session) for url in sites[1453:]]

        for fut in tqdm(as_completed(futures), total=len(futures), desc="Processing"):
            record = fut.result()
            records.append(record)

    return records

def task():
    df = pd.read_csv('samples.csv')
    x = df['url'].to_list()

    start_time = time.time()
    records = download_all_sites(x)
    print(f"Elapsed time: {time.time()-start_time}, scraped {len(records)} records")

    details_df = pd.DataFrame(records)
    
    merged = df.merge(details_df, on="url", how="left")
    merged.to_csv("merged.csv", index=False)



if __name__ == '__main__':
    task()