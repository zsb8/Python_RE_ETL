# RE_ETL
Use Match Regular Expressions to ETL.
This is a sample code which ETL funds data from Manulife.

~~~
    MANULIFE_GET_EFFECTIVE_PATTERN = r'<h1 id="allocation-heading" class="section-title">\n\s+Portfolio Allocation\n\s+' \
                                     r'<span class="title-note">as of\s(?P<effective_date>.+?)</span>'
    MANULIFE_PERFORMANCE_PATTERN = r'<p class="not-enough-data">Performance cannot be shown'
    MANULIFE_CONTENT_TITLE_PATTERN = r'<h2 class="content-title">\n(.+?)</div>'
    MANULIFE_TITLE_PATTERN = r'(?P<block_name>.+?)<span class="title-note">'
    MANULIFE_DAILY_PRICE_BLOCK_PATTERN = r'<td>\d{4}/\d{2}/\d{2}</td>\n\s+<td\sclass="col-number">\$\d+.\d+</td>'
    MANULIFE_DAILY_PRICE_PATTERN = r'(?P<date>\d{4}/\d{2}/\d{2})</td>\n\s+<td\sclass="col-number">\$(?P<price>\d+.\d+)</td>'
~~~
