# -*- coding: utf-8 -*-
"""
@author: Songbin.Zhang
This program is used to get info from the HTML file in your local hard disk
(1) Import data into fund table.
(2) Import data into fund_code table.
WEB:  https://funds.manulife.ca/en-us/profiles
Store as tree HTML files，then read and get the all fund name and code, then import into database.
(1) The fund_info.html include the fund's name and code.
(2) The fund_inception.html include the fund's name and inception.
(3) The fund_return.html include the fund's name and 7 years returns.
"""
import re
import pandas as pd
from utils import pgfunctions as pg
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)


class FundInfoToDb():
    def __init__(self, url_file_list):
        self.url_file_code = url_file_list[0]
        self.url_file_inception = url_file_list[1]
        self.url_file_return = url_file_list[2]

    def execute(self):
        """
        Main program
        :return: None
        """
        re_p = r'class="col-name colgroup-last-col">(?P<fund_name>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_frontEnd" class="">(?P<frontEnd>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_noLoadWithCb" class="">(?P<noLoadWithCb>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_noLoad" class="">(?P<noLoad>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_lowLoad" class="">(?P<lowLoad>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_backEnd" class="">(?P<backEnd>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_fClass" class="">(?P<fClass>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_psf" class="">(?P<psf>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_eliteFrontEnd" class="">(?P<eliteFrontEnd>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_eliteBackEnd" class="">(?P<eliteBackEnd>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_eliteLowLoad" class="">(?P<eliteLowLoad>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_platinumNoLoad" class="">(?P<platinumNoLoad>.+?)</td>'\
                r'<td headers="assetClass\d+ fundServ_noLoadWithReset" class="">(?P<noLoadWithReset>.+?)</td>.+'
        cols = ["fund_name", "frontEnd", "noLoadWithCb", "noLoad", "lowLoad", \
                                   "backEnd", "fClass", "psf", "eliteFrontEnd", "eliteBackEnd", \
                                   "eliteLowLoad", "platinumNoLoad", "noLoadWithReset"]
        df_code = self.read_re(self.url_file_code, re_p, cols)
        re_p = r'<tr class="body-row">'\
                r'<td headers="assetClass\d+ entityName" class="col-name colgroup-last-col">(?P<fund_name>.+?)</td>'\
                r'<td headers="assetClass\d+ incepDate" class="">(?P<month>.+?)</td>.+'
        cols = ["fund_name", "month"]
        df_inception = self.read_re(self.url_file_inception, re_p, cols)
        df_inception["inception"] = pd.to_datetime(df_inception["month"])
        df_inception.drop('month', axis=1, inplace=True)
        df_merge = pd.merge(df_code, df_inception, on="fund_name")
        self.fund_name_inception_etl(df_merge)

        re_p = r'class="col-name colgroup-last-col">(?P<fund_name>.+?)</td>'\
                r'<td headers="assetClass\d+ annRet2020" class="col-num">(?P<Y2020>.+?)</td>'\
                r'<td headers="assetClass\d+ annRet2019" class="col-num">(?P<Y2019>.+?)</td>'\
                r'<td headers="assetClass\d+ annRet2018" class="col-num">(?P<Y2018>.+?)</td>'\
                r'<td headers="assetClass\d+ annRet2017" class="col-num">(?P<Y2017>.+?)</td>'\
                r'<td headers="assetClass\d+ annRet2016" class="col-num">(?P<Y2016>.+?)</td>'\
                r'<td headers="assetClass\d+ annRet2015" class="col-num">(?P<Y2015>.+?)</td>'\
                r'<td headers="assetClass\d+ annRet2014" class="col-num colgroup-last-col">(?P<Y2014>.+?)</td>.+'
        cols = ["fund_name", "Y2020", "Y2019", "Y2018", "Y2017", "Y2016", "Y2015", "Y2014"]
        df_return = self.read_re(self.url_file_return, re_p, cols)
        self.fund_return_etl(df_return)

    @staticmethod
    def read_re(url_file, re_p, df_cols):
        """
        Read the url_file and get the valuable data from the HTML file by RE pattern.
        :param url_file: str
        :param re_p: str  pattern using RE
        :param df_cols: list    it is the columns of df
        :return: dataframe
        """
        p1 = r'<tr class="body-row">.+\n'
        pattern = re.compile(p1, re.M)
        matches = pattern.search(url_file)
        text1 = matches.group()
        p2 = r'(<tr class="body-row">.+?</tr>)'
        pattern = re.compile(p2)
        result2 = pattern.findall(text1)
        df = pd.DataFrame(columns=df_cols)
        for i in result2:
            pattern = re.compile(re_p)
            result = pattern.search(i)
            if result != None:
                df_temp = pd.DataFrame([result.groupdict(0)])
                df = pd.concat([df, df_temp])
        return df

    def fund_name_inception_etl(self, df) -> None:
        """
        Import fund's name, issue and inception to database.
        Only import created data.
        :param df: dataframe
        :return: None
        """
        increase_df = pg.get_new_df("fund", df, ["fund_name"])
        increase_df['issue'] = 'manulife'
        pg.df_columns_db("fund", increase_df, ['fund_name', 'issue', 'inception'])
        print("fund_name_inception_etl complete.")
        self.fund_code_etl(increase_df)

    def fund_code_etl(self, df) -> None:
        """
        Import code to database.
        :param df: dataframe
        :return: None
        """
        for index, rows in df.iterrows():
            df2 = pd.DataFrame({'fund_code': rows[1:-2]})
            df3 = df2.reset_index()
            df3.rename(columns={"index": 'load_type'}, inplace=True)
            df3['fund_id'] = self.get_id(rows)
            df3['fund_code'] = df3['fund_code'].str.strip()
            df3.drop(index=df3[df3['fund_code'] == '—'].index, inplace=True)
            pg.df_columns_db("fund_code", df3, ['load_type', 'fund_code', 'fund_id'])
        print("fund_code_etl complete.")

    def fund_return_etl(self, df) -> None:
        """
        Import 7 years returns to database.
        :param df: dataframe
        :return: None
        """
        increase_df = pg.get_new_df("fund", df, ["fund_name"])
        for index, rows in increase_df.iterrows():
            df2 = pd.DataFrame({'year': rows.index, 'return': rows.values})
            df2['fund_id'] = self.get_id(rows)
            df2.drop(0, axis=0, inplace=True)  # delete first line because it is title
            df2.drop(index=df2[df2['return'] == '—'].index, inplace=True)
            df2["year"] = df2["year"].apply(lambda x: x[1:])  # delete the "Y" to number
            pg.df_columns_db("fund_return", df2, ['year', 'return', 'fund_id'])
        print("fund_return_etl complete.")

    @staticmethod
    def get_id(rows):
        """
        Get one fund id by SQL query
        :param rows: series
        :return result: str
        """
        a = rows["fund_name"]
        sql = f"""SELECT id FROM fund WHERE fund_name='{a}' """
        fund_id = pg.execute_sql(sql)
        result = fund_id[0][0]
        return result


if __name__ == '__main__':
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



