from dotenv import load_dotenv
load_dotenv()

import asyncio
import requests
from typing import Literal, Union
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup, Tag
from db import Database


class PennCrawler():
    def __init__(self) -> None:
        self.url = "https://www.pals.pa.gov/api/Search/SearchForPersonOrFacilty"

    async def start(self):
        insert_values = []
        page = 0
        res = self.pennsylvania_crawler(page)
        while len(res) > 0:
            vals = [{'name': self.full_name(p.get("FirstName"), p.get("MiddleName"), p.get("LastName")), 'license_number': p.get("LicenseNumber"), 'city': p.get("City").upper(), 'state': p.get("State").upper(), 'is_license_active': p.get("Status") == "Active"} for p in res]
            insert_values.extend(vals)
            page += 1
            res = self.pennsylvania_crawler(page)
        return insert_values

    def pennsylvania_crawler(self, page = 1):
        payload = {"OptPersonFacility":"Person","ProfessionID":37,"LicenseTypeId":84,"State":"","Country":"ALL","County":None,"IsFacility":0,"PersonId":None,"PageNo":page}
        r = requests.post("https://www.pals.pa.gov/api/Search/SearchForPersonOrFacilty", json = payload)
        return r.json()

    def full_name(self, firstname: str, middlename: str, lastname: str):
        name = firstname
        name += f" {middlename}" if middlename else ""
        name += f" {lastname}" if lastname else ""
        return name


class FloridaCrawler:
    def __init__(self):
        self.base_url = "https://mqa-internet.doh.state.fl.us"
        self.home_url = self.base_url + "/MQASearchServices/healthcareproviders"
        self.pages = []

    def handle_list_data(self, data, ret = []):
        soup = BeautifulSoup(data, features="html.parser")
        tbody = soup.find("tbody")
        if tbody and isinstance(tbody, Tag):
            for row in tbody.find_all("tr"):
                cells = [cell.text.strip() for cell in row.find_all("td")]
                # ['ME85753', 'ABDEL-HALIM, JAMAL MOHAMMAD', 'Medical Doctor', 'WELLINGTON', 'CLEAR/Active']
                ret.append({ 'license_number': cells[0], 'name': cells[1], 'city': cells[3], 'is_license_active': 'active' in cells[-1].lower() })
                

    async def start(self):
        async with async_playwright() as p:
            self.browser = await p.chromium.launch()
            self.ctx = await self.browser.new_context()
            page = await self.ctx.new_page()
            self.pages.append(page)
            await page.goto(self.home_url)
            el = page.locator("select#BoardDD")
            await el.select_option("15")
            el = page.locator("select#ProfessionDD")
            await el.select_option("1501")
            await page.locator("[type=submit]").click()
            await self.fetch_next_pages()
            data = await self.get_data()
            await self.browser.close()
            return data

    async def open_link_new_tab(self, path):
        new_page = await self.ctx.new_page()
        self.pages.append(new_page)
        await new_page.goto(self.base_url + path)

    async def get_data(self):
        all_data = []
        for p in self.pages:
            item = await p.locator("table").inner_html()
            self.handle_list_data(item, all_data)
        return all_data

    async def fetch_next_pages(self, page_idx = 0):
        items = await self.pages[page_idx].locator("ul.pagination > li > a").all()
        if len(items) == 0:
            await asyncio.sleep(0.15)
            await self.fetch_next_pages(page_idx)
            return
        paths = [await p.get_attribute("href") for p in items]
        res = []
        add_status = 0
        for p in paths:
            if p == None:
                if add_status == 0:
                    add_status = 1
                else:
                    add_status = 0
                    break
            else:
                if add_status == 1:
                    res.append(p)
        await asyncio.gather(*[self.open_link_new_tab(p) for p in res if p])


async def crawl_data(crawler: Union[Literal['penn'], Literal['florida']]):
    db = Database()
    insert_values = []
    c = PennCrawler() if crawler == "penn" else FloridaCrawler()
    data = await c.start()
    print("Data crawled.")
    for p in data:
        insert_values.append((p['name'], p['license_number'], p['city'].upper(), None, p['is_license_active']))
    print("Fetched all data")
    print("Inserting...")
    db.insert_data(insert_values)
    print("Added to db ✅")





if __name__ == "__main__":
    asyncio.run(crawl_data("penn"))
    # print(json.dumps(pennsylvania_crawler(344953)))


