"""
This program is used to import allocation_class info and daily data to database.
allocation_class info include four blocks:
 (1)Asset_Allocation; (2)Geographic_Allocation;(3)Sector_Allocation;(4)'Underlying Fund Holdings' or 'Top 10 Holdings'.
Attention: some funds have 'Underlying_Fund_Holdings' such as MGE968, some  have  the 'Top 10 Holdings'(MCB3208)
Some funds have not Sector_Allocation such as MCB6501
"""
import re
import requests
import pandas as pd
from utils import pgfunctions as pg
from utils import other_functions as oth


class CodeDataToDb():
    def __init__(self, fund_code):
        self.code = fund_code
        self.match = ""
        self.matches_date = ""
        self.allocation_class = ""
        self.url_file = ""

    def execute(self):
        """
        Main program. Grab data of fund_code from internet, then import into database.
        :return: None
        """
        url = 'https://funds.manulife.ca/en-US/profiles/' + self.code
        self.url_file = oth.grab(url)
        # get report date in the HTML
        date_p = r'<h1 id="allocation-heading" class="section-title">\n\s+Portfolio Allocation\n\s+'\
                 r'<span class="title-note">as of\s(?P<report_date>.+?)</span>'
        pattern = re.compile(date_p, re.S)
        self.matches_date = pd.to_datetime(pattern.search(self.url_file).groupdict()["report_date"])
        # query whether has duplicate data by fund_code and report_date
        wh = f"fund_code='{self.code}' and report_date='{self.matches_date}' "
        if not pg.whether_data("fund_composition", wh):
            # Step 1: Import allocation_class data (3 or more  blocks and multi lines) into database
            block_p = r'<h2 class="content-title">\n(.+?)</div>'
            pattern = re.compile(block_p, re.S)
            matches = pattern.findall(self.url_file)
            for self.match in matches:    # every block
                items_p = r'(?P<block_name>.+?)<span class="title-note">'   # get the title
                pattern = re.compile(items_p, re.S)
                result = pattern.search(self.match)
                a_dict = result.groupdict()
                self.allocation_class = a_dict["block_name"].strip()
                search_obj = re.search(r'Hold', self.allocation_class, re.I)
                if search_obj:
                    self.code_protfolio_etl(2)
                else:
                    self.code_protfolio_etl(1)
            # Step 2: Import daily data to database, but only import new daily data.
            self.code_price_etl()
        else:
            print(f"{self.code} already has detail records.")

    def code_protfolio_etl(self, block_type):
        """
        Step 1 :
        Import every allocation ( one Protfolio has some allocations) to database,
        include percentage data and daily data.
        :block_type: int
            1-first line, include Asset_Allocation,Geographic_Allocation,Sector_Allocation;
            2-second line, include Holding etc.
        :return: None
        """
        if block_type == 1:
            # Asset_Allocation, Geographic_Allocation, Sector_Allocation etc show in first line.
            p_p1 = r'<td><span>(.+?)</span></td>'
            p_p2 = r'<td class="col-number">(.+?)</td>'
        if block_type == 2:
            # Top 10 Holdings, Holding etc show in second line.
            p_p1 = r'<tr>\n\s+<td>(.+?)</td>'
            p_p2 = r'</td>\n\s+<td class="col-number">(.+?)</td>'
        p_pattern = re.compile(p_p1, re.S)
        item_list = p_pattern.findall(self.match)
        p_pattern = re.compile(p_p2, re.S)
        percent_list = p_pattern.findall(self.match)
        p_df = pd.DataFrame({"item": item_list, "percent": percent_list})
        p_df["fund_code"] = self.code
        p_df["allocation_class"] = self.allocation_class
        p_df["report_date"] = self.matches_date
        wh = f"fund_code='{self.code}' "
        if not pg.whether_data("fund_composition", wh):
            sql = f"DELETE FROM fund_composition WHERE {wh} "
            pg.execute_sql_not_return(sql)
        pg.df_columns_db("fund_composition", p_df,
                             ["item", "percent", "fund_code", "allocation_class", "report_date"])

    def code_price_etl(self):
        """
        Step 2 :
        Import daily data of fund_code into database.
        Only import new data because maybe has duplex data of last time.
        :return: None
        """
        daily_df = pd.DataFrame(columns=["fund_code", "date", "price"])
        daily_p1 = r'<td>\d{4}/\d{2}/\d{2}</td>\n\s+<td\sclass="col-number">\$\d+.\d+</td>'
        d_pattern = re.compile(daily_p1)
        d_matches = d_pattern.findall(self.url_file)
        daily_p2 = r'(?P<date>\d{4}/\d{2}/\d{2})</td>\n\s+<td\sclass="col-number">\$(?P<price>\d+.\d+)</td>'
        for j in d_matches:
            res = re.search(daily_p2, j)
            url_dict = res.groupdict()
            url_dict["fund_code"] = self.code
            df_temp = pd.DataFrame([url_dict])
            daily_df = pd.concat([daily_df, df_temp])
        daily_df["date"] = pd.to_datetime(daily_df["date"])
        increase_daily_df = pg.get_new_df("fund_price", daily_df, ["fund_code", "date"])
        pg.df_columns_db("fund_price", increase_daily_df, ["fund_code", "date", "price"])


if __name__ == '__main__':
    table_name = "fund_code"
    column_name = "fund_code"
    code_list = pg.column(table_name, column_name)
    for code in code_list:
        data_db = CodeDataToDb(code[0])
        data_db.execute()


