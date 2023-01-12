"""INVESTING closing prices, highs and lows"""

from datetime import date, timedelta
from file_clean import clean
import json
import os
from pandas import DataFrame
import pyodbc
from time import sleep
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sys
from webdriver_manager.chrome import ChromeDriverManager

df_dic = {}
ticker_to_name = {}
hrefs = {}
pclose_col = []
popen_col = []
ticker_col = []
name_col = []
vol_col = []
high_col = []
low_col = []
exchange_col = []
retries = []
get_avoid = ''
get_must = ''

date = date.today() 
print(date)
i = 1

with open(r'work files\avoid.txt', 'r') as file:
    avoid = file.read()
    avoid = avoid.split()

with open(r'work files\must.txt', 'r') as file:
    must = file.read()
    must = must.split()

nr_vars = json.load(open(r'work files\nr_vars.txt'))
url = nr_vars['web'][0]['url']
nameticker_x = nr_vars['web'][0]['nameticker_x']
popen_x = nr_vars['web'][0]['popen_x']
pclose_x = nr_vars['web'][0]['pclose_x']
volume_x = nr_vars['web'][0]['volume_x']
day_range_x = nr_vars['web'][0]['day_range_x']
filter_id = nr_vars['web'][0]['filter_id']
table_id = nr_vars['web'][0]['table_id']
try_n = int(nr_vars['try_n'])
sectors_filter = nr_vars['web'][1]

sql_sp = nr_vars['sql'][0]['sql_sp']
sql_truncate = nr_vars['sql'][0]['sql_truncate']
sql_driver = nr_vars['sql'][0]['sql_driver']
sql_server = nr_vars['sql'][0]['sql_server']
sql_database = nr_vars['sql'][0]['sql_database']
trust = nr_vars['sql'][0]['trust']
conn_str = f'Driver={sql_driver};Server={sql_server};Database={sql_database};Trusted_Connection={trust}'
prefs = {"download.prompt_for_download": False}

options = Options()
options.page_load_strategy = 'eager' # none, eager or normal (default)
options.add_argument("--headless")
options.add_experimental_option('prefs', prefs)  # TEST
driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
driver.get(url)
driver.minimize_window()


def get_elements(stock_url='')-> None:
    """Scrape stock data and append to column"""

    driver.get(ref)
    tickname_element = WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.XPATH, nameticker_x)))
    tickname_element = tickname_element.text
    end_name = tickname_element.find("(")
    end_ticker = tickname_element.find(")")
    name = tickname_element[:end_name]
    ticker = tickname_element[end_name+1:end_ticker]
    popen_element = WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.XPATH, popen_x)))
    popen = popen_element.text.replace(',','')
    pclose_element = WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.XPATH, pclose_x)))                
    pclose = pclose_element.text.replace(',','')
    volume_element = WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.XPATH, volume_x)))                
    volume = volume_element.text.replace(',','')
    range_element = WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.XPATH, day_range_x)))                
    day_range = range_element.text
    sep = day_range.find("-")
    low = day_range[:sep].strip().replace(',','')
    high = day_range[sep + 1:].strip().replace(',','')
    popen_col.append(float(popen))
    pclose_col.append(float(pclose))
    ticker_col.append(ticker)
    name_col.append(name)
    high_col.append(float(high))
    low_col.append(float(low))
    vol_col.append(float(volume))
    exchange_col.append(sector)
    
    return None

while try_n <= 5:
    try:
        for sector in sectors_filter:
            hrefs[sector] = []
            filter_sec = sectors_filter[sector]
            WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.ID, filter_id))).click()
            WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.ID, filter_sec))).click()
            table = WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.ID, table_id)))
            df_txt = table.text
            df_dic[sector] = df_txt.split()
            web_elements = driver.find_elements('xpath', '//*[@href]')
            for elem in web_elements:
                href = elem.get_attribute('href')
                if href not in hrefs[sector]:
                    hrefs[sector].append(href)
        try_n = 100
    except Exception as toe:
        print(str(toe))
        print(f"Page id - {try_n}")
        sleep(30)
        if try_n == 5:
            driver.close()
#             warning('CRITICAL ERROR', 'No data was scraped.\nPlease check connectivity')
            sys.exit("Couldn`t make the first conncetion")
    try_n += 1

for sector in hrefs:
    sec_refs = hrefs[sector]
    for ref in sec_refs:
        if ref not in avoid and 'equities' in ref:
            try:
                get_elements(stock_url=ref)
                get_must = ' '.join([get_must, ref])
                i+=1
            except Exception as e:
                get_avoid = ' '.join([get_avoid, ref])
                with open(r'work files\avoid.txt', 'a') as file: # NOT A FIXED SOLUTION
                    file.write(ref + ' ')  
                if ref in must:
                    retries.append(ref)

if len(retries) > 0:
    options = Options()
    options.page_load_strategy = 'eager' # none, eager or normal (default)
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    for retry in retries:
        try:
            get_elements(stock_url=retry)
        except Exception as e:
            print(retry)  # write to log
            
driver.close()

if not os.path.exists(r'work files\avoid.txt'):
    print(f'Avoid file was not found. Creating file at {os.getcwd}')

with open(r'work files\avoid.txt', 'a') as file:
    file.write(get_avoid)
    clean(r'work files\avoid.txt')

with open(r'work files\must.txt', 'a') as file:
    file.write(get_must)
    clean(r'work files\must.txt')

df = DataFrame({"Company_name": name_col,'Symbol': ticker_col, 'Exchange': exchange_col, 
                "Close_price":pclose_col,'Open_price': popen_col, 'High': high_col, 'Low': low_col,
                "Volume":vol_col, "Date": date, 'sp': None, 'dj': None, 'nsdq': None})
df.loc[df['Exchange'] == 'sp', 'sp'] = 1
df.loc[df['Exchange'] == 'nsdq', 'nsdq'] = 1
df.loc[df['Exchange'] == 'dj', 'dj'] = 1
df.drop(axis=1, labels='Exchange', inplace=True)

agg_act = {'Company_name': 'first','Close_price': 'first','Open_price': 'first',
        'High': 'first','Low': 'first','Volume': 'first','Date': 'first','sp': 'sum','dj': 'sum','nsdq': 'sum'}
df = df.groupby(df['Symbol']).aggregate(agg_act).reset_index()

def close_conn(cursor):
    cursor.commit()
    cursor.close()
    del cursor
    return None


try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
except pyodbc.InterfaceError as  e:
#     logger.log('Connection string failure')
    sys.exit('Connection string failure?')
    
cursor.execute(sql_truncate)
try:
    for index, row in df.iterrows():
        cursor.execute("""insert into tmp 
            (Symbol, [Company name], [Open price], Low, High, [Close price], Volume, [Date], sp, dj, nsdq)
                values(?,?,?,?,?,?,?,?,?,?,?)""",
                row.Symbol, row.Company_name, row.Open_price, row.Low, row.High, row.Close_price,
                       row.Volume, row.Date, row.sp, row.dj, row.nsdq)
except pyodbc.InterfaceError as  e:
#     logger.log('Connection string failure')
    close_conn(cursor)
    sys.exit()
except pyodbc.ProgrammingError:
    print("Command string error")
    close_conn(cursor)
    sys.exit()
except AttributeError as ae:
    print("probably wrong input co name")
    close_conn(cursor)
    sys.exit()

try:
    cursor.execute(sql_sp)
    print("DONE")
except pyodbc.InterfaceError as er:
    print(er)
finally:
    close_conn(cursor)

df.to_excel(f"prices_{date}.xlsx")