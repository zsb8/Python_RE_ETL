# RE_ETL
Use Match Regular Expressions in ETL.
This is a sample code which ETL funds data from Manulife.

~~~python
    MANULIFE_GET_EFFECTIVE_PATTERN = r'<h1 id="allocation-heading" class="section-title">\n\s+Portfolio Allocation\n\s+' \
                                     r'<span class="title-note">as of\s(?P<effective_date>.+?)</span>'
    MANULIFE_PERFORMANCE_PATTERN = r'<p class="not-enough-data">Performance cannot be shown'
    MANULIFE_CONTENT_TITLE_PATTERN = r'<h2 class="content-title">\n(.+?)</div>'
    MANULIFE_TITLE_PATTERN = r'(?P<block_name>.+?)<span class="title-note">'
    MANULIFE_DAILY_PRICE_BLOCK_PATTERN = r'<td>\d{4}/\d{2}/\d{2}</td>\n\s+<td\sclass="col-number">\$\d+.\d+</td>'
    MANULIFE_DAILY_PRICE_PATTERN = r'(?P<date>\d{4}/\d{2}/\d{2})</td>\n\s+<td\sclass="col-number">\$(?P<price>\d+.\d+)</td>'
~~~

in manulife_price.py
~~~pyton
    def code_portfolio_df(self, block_type):
        """
        Step 1 - 2:
        Import every allocation ( one Portfolio has some allocations) to database,
        include percentage data .
        :block_type: str
            1-first line, include Asset_Allocation,Geographic_Allocation,Sector_Allocation;
            2-second line, include Holding etc.
        :return: df
        """
        if block_type == "first line in porfolio area":
            # Asset_Allocation, Geographic_Allocation, Sector_Allocation etc show in first line.
            p_p1 = r'<td><span>(.+?)</span></td>'
            p_p2 = r'<td class="col-number">(.+?)</td>'
        if block_type == "second line in porfolio area":
            # Top 10 Holdings, Holding etc show in second line.
            p_p1 = r'<tr>\n\s+<td>(.+?)</td>'
            p_p2 = r'</td>\n\s+<td class="col-number">(.+?)</td>'
        p_pattern = re.compile(p_p1, re.S)
        item_list = p_pattern.findall(self.match)
        p_pattern = re.compile(p_p2, re.S)
        percent_list = p_pattern.findall(self.match)
        fund_component_df = pd.DataFrame({"item": item_list, "percent": percent_list})
        fund_component_df["allocation_class"] = self.allocation_class
        fund_component_df["effective_date"] = self.matches_date
        fund_component_df["fund_id"] = self.fund_id
        return fund_component_df
~~~
