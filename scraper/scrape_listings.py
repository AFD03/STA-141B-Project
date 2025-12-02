#diag.py
import time
from typing import Dict, List

import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

BASE_URL = "https://sfbay.craigslist.org/search/san-francisco-ca/apa"

START_PRICE = 600
BUCKET_WIDTH = 200
MAX_SEARCH_PRICE = 9000


def get_scroll_root(driver):
    """
    Try to find the real scrollable element.
    Prefer #search-results, fall back to .scrolling-container, else window.
    Returns (root_element_or_None, mode) where mode is "element" or "window".
    """
    try:
        root = driver.find_element(By.XPATH, "//*[@id='search-results']")
        print("  Using #search-results as scroll root")
        return root, "element"
    except Exception:
        pass

    try:
        root = driver.find_element(By.XPATH, "//*[@class='scrolling-container']")
        print("  Using .scrolling-container as scroll root")
        return root, "element"
    except Exception:
        pass

    print("  Falling back to window scroll")
    return None, "window"


def scrape_bucket(driver, wait, min_price: int, max_price: int) -> Dict[str, Dict]:
    """
    Scrape one price bucket (min_price to max_price) using the virtualized
    scrolling logic. Returns a dict: pid -> listing_info.
    """
    url = f"{BASE_URL}?min_price={min_price}&max_price={max_price}"
    print(f"\n=== Bucket ${min_price} to ${max_price} ===")
    print(f"Loading: {url}")
    driver.get(url)


    try:
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@data-pid]")
            )
        )
    except TimeoutException:
        print("  No visible results for this bucket (timeout waiting for results).")
        return {}

    print("  First batch of results detected for this bucket.")

    scroll_root, scroll_mode = get_scroll_root(driver)

    listings_by_pid: Dict[str, Dict] = {}
    no_new_pids_rounds = 0

    # Virtualized list scan: move the viewport and capture PIDs repeatedly
    for step in range(300):  # hard cap so it can't loop forever
        results = driver.find_elements(By.XPATH, "//div[@data-pid]")
        pages = driver.find_elements(By.XPATH, "//div[@class='cl-scroll-page']")

        new_pids_this_round = 0
        for el in results:
            pid = el.get_attribute("data-pid")
            if not pid or pid in listings_by_pid:
                continue

            # title + url
            anchors = el.find_elements(By.XPATH, ".//a[contains(@class, 'cl-app-anchor')]")
            if anchors:
                anchor = anchors[0]
            else:
                anchor = el.find_element(By.XPATH, ".//a")

            url = anchor.get_attribute("href")

            # price: something like "3250"
            price_els = el.find_elements(
                By.XPATH,
                ".//span[contains(@class, 'price') or contains(@class, 'result-price')]"
            )
            price = price_els[0].text.strip().replace(",","")[1:]
            price = int(price) if price_els else None

            # sqft: gives square foot
            sqft_els = el.find_elements(By.XPATH, ".//span[contains(@class, 'post-sqft')]")
            if sqft_els: sqft = int(sqft_els[0].text.strip().replace("ft2",""))
            else: sqft=None


            # beds: gives integer
            beds_els = el.find_elements(By.XPATH, ".//span[contains(@class, 'post-bedrooms')]")
            if beds_els: 
                beds = beds_els[0].text.strip()
                beds = int(beds[0])
            else: beds = None

            # neighborhood: Gives hood
            hood_els = el.find_elements(By.XPATH, ".//div[contains(@class,'meta')]")
            if hood_els: 
                meta_text = hood_els[0].text.strip()
                
                lines = [ln.strip() for ln in meta_text.split("\n") if ln.strip()]

                hood = lines[-1] if lines else None



            listings_by_pid[pid] = {
                "pid": pid,
                "url": url,
                "price": price,
                "beds": beds,
                "sqft": sqft,
                "hood": hood,
            }

            new_pids_this_round += 1


        print(
            f"  Step {step:03d} -> "
            f"DOM results: {len(results)}, cl-scroll-page: {len(pages)}, "
            f"new pids this round: {new_pids_this_round}, "
            f"bucket unique pids: {len(listings_by_pid)}"
        )

        if new_pids_this_round == 0:
            no_new_pids_rounds += 1
        else:
            no_new_pids_rounds = 0

        # If we've scrolled many times without seeing new IDs, assume we've covered this bucket
        if no_new_pids_rounds >= 3:
            print("  No new PIDs for many scrolls in this bucket; stopping.")
            break

        # Scroll further
        if scroll_mode == "element" and scroll_root is not None:
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollTop + 1200;",
                scroll_root,
            )
        else:
            driver.execute_script(
                "window.scrollTo(0, window.pageYOffset + 1200);"
            )

        time.sleep(1.2)

    print(f"  Bucket ${min_price}-{max_price}: total unique PIDs found: {len(listings_by_pid)}")
    return listings_by_pid


def main():
    options = Options()
    # comment this out if you want to watch it scroll
    # options.add_argument("--headless=new")
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    all_listings_by_pid: Dict[str, Dict] = {}
    global_pids = set()

    min_price = START_PRICE
    empty_buckets_in_a_row = 0

    try:
        while min_price < MAX_SEARCH_PRICE:
            max_price = min_price + BUCKET_WIDTH

            bucket_listings = scrape_bucket(driver, wait, min_price, max_price)

            # Count new vs duplicate in this bucket (relative to global set)
            new_from_bucket = 0
            for pid, listing in bucket_listings.items():
                if pid not in global_pids:
                    global_pids.add(pid)
                    all_listings_by_pid[pid] = listing
                    new_from_bucket += 1

            print(
                f"=== Bucket summary ${min_price}-{max_price}: "
                f"{len(bucket_listings)} unique in bucket, "
                f"{new_from_bucket} new globally, "
                f"{len(all_listings_by_pid)} total unique so far ==="
            )

            # Heuristic: if a bucket has zero results, count it and possibly stop
            if len(bucket_listings) == 0:
                empty_buckets_in_a_row += 1
            else:
                empty_buckets_in_a_row = 0

            # After a few empty buckets in a row, assume we've gone past the useful price range
            if empty_buckets_in_a_row >= 2:
                print("Hit 3 empty buckets in a row; assuming no more listings at higher prices.")
                break

            min_price = max_price  # move to next price range

        print("\n=== GLOBAL SUMMARY ===")
        print(f"Total unique listings across all buckets: {len(all_listings_by_pid)}")

        print("\nSample listings:")
        print(len(list(all_listings_by_pid.values())))
        for item in list(all_listings_by_pid.values())[:5]:
            print(item)
        
        samples = [item for item in list(all_listings_by_pid.values())]
        df = pd.DataFrame(samples)
        df.to_csv('samples.csv')

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
