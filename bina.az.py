import pdb
import time

import requests
from bs4 import BeautifulSoup
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from pprint import pprint as pp

months_to_num_dict = {
    'yanvar': 1,
    'fevral': 2,
    'mart': 3,
    'aprel': 4,
    'may': 5,
    'iyun': 6,
    'iyul': 7,
    'avqust': 8,
    'sentyabr': 9,
    'oktyabr': 10,
    'noyabr': 11,
    'dekabr': 12

}

base_url = 'https://bina.az'


def insert_into_table(conn, values_dict):
    placeholder = '%s'
    placeholders = ', '.join([placeholder] * len(values_dict))
    values = values_dict.values()
    keys = tuple(values_dict.keys())
    col_names = str(keys).replace("'", "")

    sql = f"INSERT INTO bina {col_names} VALUES({placeholders})"
    val = tuple(values)
    cur = conn.cursor()
    cur.execute(sql, val)
    conn.commit()
    cur.close()

def get_all_item(conn):
    sql = 'select * from bina'
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    return result

def connect():
    conn = psycopg2.connect(
        host="localhost",
        database="bina",
        user="postgres",
        password="postgres")
    return conn


def create_table(conn):
    sql = """create table IF NOT EXISTS bina(
    id bigserial primary key,
    unique_id bigint unique,
    city varchar (200),
    location varchar (300),
    price bigint,
    currency varchar (10),
    img varchar (400),
    url varchar (300),
    date date,
    category varchar(100),
    floor varchar(20),
    area varchar(30),
    yard_area varchar(20),
    number_of_rooms smallint,
    title_deed varchar(20),
    mortgage varchar(10),
    is_agency smallint
    )  
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()


def insert_partial(obj, conn):
    sql = "insert into bina (unique_id, is_parsed, city, location, price, currency, img, url, date)" \
          " values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    values = (obj['unique_id'], obj['is_parsed'], obj['city'], obj['location'], obj['price'],
              obj['currency'], obj['img'], obj['url'], obj['date'])
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    cur.close()


def parse_time(dt):
    leng = len(dt.split())
    if leng == 3:
        day, month, year = dt.split()
        month = months_to_num_dict.get(month)
        if not month:
            month = datetime.now().month
        return datetime.strptime(f'{day}.{month}.{year}', '%d.%m.%Y')
    else:
        if 'bugün' in dt:
            return datetime.now()
        else:
            return datetime.now() - timedelta(days=1)


def get_bs(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Error to get {url}')
    bs = BeautifulSoup(response.content, 'lxml')
    return bs


def parse_inner_page(url):
    print('Parsing: ', url)
    bs = get_bs(url)
    in_info = dict()
    table = bs.find('table', class_='parameters')
    if table:
        trs = table.find_all('tr')
        for tr in trs:
            key, value = tr.find_all('td')
            if key and value:
                key = key.get_text().strip()
                value = value.get_text().strip()
                in_info[key] = value
        agency = bs.find('div', class_='agency-container')
        if agency:
            in_info['is_agency'] = 1
        else:
            in_info['is_agency'] = 0
    return in_info


def modify_inner_info(inif):
    inif['category'] = inif.pop('Kateqoriya', None)
    inif['area'] = inif.pop('Sahə', None)
    inif['yard_area'] = inif.pop('Torpaq sahəsi', None)
    inif['number_of_rooms'] = inif.pop('Otaq sayı', None)
    inif['title_deed'] = inif.pop('Kupça', None)
    inif['floor'] = inif.pop('Mərtəbə', None)
    inif['mortgage'] = inif.pop('İpoteka', None)
    return inif


def parse_outer_page():
    conn = connect()
    # try:
    create_table(conn)
    # except:
    #     pass

    # pdb.set_trace()
    for i in range(704, 10001):
        print(i)
        time.sleep(2)
        url = f'https://bina.az/alqi-satqi?page={i}'
        page = get_bs(url)
        container = page.find('div', class_='items_list')
        items = container.find_all('div', class_='items-i')

        for item in items:
            obj = dict()
            a_ = item.find('a', class_='item_link')
            if a_ and a_.has_attr('href'):
                # pdb.set_trace()
                obj['unique_id'] = int(a_['href'].split('/')[-1].replace('.html', ''))
                obj['url'] = base_url + a_['href']

            img_ = item.find('img')
            if img_ and img_.has_attr('data-src'):
                obj['img'] = img_['data-src']

            price = item.find('span', class_='price-val')
            if price:
                obj['price'] = int(price.get_text().strip().replace(' ', ''))

            currency = item.find('span', class_='price-cur')
            if currency:
                obj['currency'] = currency.get_text().strip()

            location = item.find('div', class_='location')
            if location:
                obj['location'] = location.get_text().strip()

            city_when = item.find('div', class_='city_when')
            if city_when:
                city_when = city_when.get_text().strip()
                # Bakı, 22 dekabr 2021
                city, when = city_when.split(',')
                obj['city'] = city.strip()
                when = when.strip()

                date = parse_time(when)
                obj['date'] = date
                # obj['is_parsed'] = 0
            inner_info = parse_inner_page(obj['url'])
            inner_info = modify_inner_info(inner_info)
            obj.update(inner_info)
            try:
                insert_into_table(conn, obj)
                print('inserted')
            except Exception as e:
                conn.rollback()
                print(e)


if __name__ == '__main__':
    #parse_outer_page()
    con = connect()
    result = get_all_item(con)
    print(result)


df = pd.DataFrame(result)
df.to_excel('data_bina.xlsx', index=False)

