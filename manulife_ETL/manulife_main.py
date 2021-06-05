from manulife_fund_info import FundInfoToDb
from manulife_percent_price import CodeDataToDb
from utils import pgfunctions as pg
"""
Step 1 : import Manulife funds name,issue, inception, codes, 7 years returns to the database.
Manually executed downloading 3 html files and importing to database  every quarter.
Read the funds name and code from fund_info.html.
Read the funds name and inception from fund_inception.html
Read the funds name and 7 years from fund_year.html
"""
file = open('fund_info.html', mode='r', encoding='UTF-8')
url_file_code = file.read()
file = open('fund_inception.html', mode='r', encoding='UTF-8')
url_file_inception = file.read()
file = open('fund_return.html', mode='r', encoding='UTF-8')
url_file_return = file.read()
file.close()
url_file_list = [url_file_code, url_file_inception, url_file_return]
fund_info = FundInfoToDb(url_file_list)
fund_info.execute()
print("Import Manulife funds name,issue, inception, codes, 7 years returns completely.")

"""
Step 2 : import code's Protfolio and daily price to database.
Auto executed importing to database every day.
Attention: very cost time.
"""
table_name = "fund_code"
column_name = "fund_code"
code_list = pg.column(table_name, column_name)
for code in code_list:
    data_db = CodeDataToDb(code[0])
    data_db.execute()
print("Import Manulife funds daily price completely.")
