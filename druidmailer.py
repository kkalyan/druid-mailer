import json
import re
import pandas as pd
from datetime import date, timedelta
import datetime
from six.moves import urllib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from smtplib import SMTPException
import sys
from email.charset import Charset, QP
import globalconfig



def fetch_and_email(row):
    queries = row.query.split(',')
    html=''
    for query in queries:
    	query, query_obj = get_query(query,row.days)
    	result = fetch_json(query,globalconfig.druid)
    	df = as_df(result,query_obj)
    	html += to_html(df,query_obj)
    email(row, html)

def get_query(query_file,days):
    query = file(query_file,'r').read()
    query_obj = json.loads(query);
    start_date = datetime.date.today() - timedelta(days)
    date = start_date.strftime('%Y-%m-%d')
    query_obj['intervals']=start_date.strftime('%Y-%m-%d/P'+str(days)+'D')
    return (json.dumps(query_obj),query_obj)

def fetch_json(query, druid_url):
    headers = {'Content-Type': 'application/json'}
    req = urllib.request.Request(druid_url, query, headers)
    res = urllib.request.urlopen(req)
    data = res.read().decode("utf-8")
    res.close()
    return json.loads(data)

def to_html(df,query_obj):
    if 'dimension' in query_obj:
        dims = [query_obj['dimension']]
    elif 'dimensions' in query_obj:
        dims = query_obj['dimensions']
    else:
        dims=[]

    dimensions = []
    for dim in dims:
        if(isinstance(dim,unicode)):
            dimensions.append(dim)
        if(isinstance(dim,dict)):
            dimensions.append(dim['outputName'])
    metrics = []
    percent_metrics = []
    if 'percent_metrics' in query_obj:
        percent_metrics = query_obj['percent_metrics']
    if 'email_metrics' in query_obj:
        metrics = query_obj['email_metrics']
    else:
	for col in df.columns:
        	if(col=='timestamp' or col in dimensions):
            		continue
        	metrics.append(col)
    try:
        sort_column = query_obj['limitSpec']['columns'][0]['dimension']
    except:
        sort_column = ''
    table_css = 'style="border-collapse: collapse;font-family:\'Helvetica Neue\',Helvetica,Arial,sans-serif;margin:16px;"';
    thead_th_css = 'style="background-color: #DDEFEF;border: solid 1px #DDEEEE;padding: 10px;text-align: left;"';
    tbody_td_css = 'style="border: solid 1px #DDEEEE;padding: 10px;text-shadow: 1px 1px 1px #fff;"';
    if 'email_heading' in query_obj:
	html='<h2 style=\'font-family:Lato,"Helvetica Neue",Helvetica,Arial,sans-serif;margin:16px;color:rgb(57,67,64)\'>%s</h2>' % query_obj['email_heading']
    else:
	html=''
    html+='<table '+table_css+'><thead><tr style="text-align: right;">';
    for dimension in dimensions:
        if dimension=='creative_url': continue;
        html+='<th '+thead_th_css+'>'+dimension+'</th>'
    for metric in metrics:
        suffix = ' &#9660;' if(sort_column==metric) else '';
        html+='<th '+thead_th_css+'>'+metric+suffix+'</th>'
    html+='</tr> </thead> <tbody>'
    for i in range(df.shape[0]):
        row = df.iloc[i]
        html+='<tr>'
        for dimension in dimensions:
            if dimension=='creative_url': continue;
            if row[dimension] is None:
                val=''
            else:
                val = row[dimension]
            html+='<td '+tbody_td_css+'>'+val+'</td>'
        for metric in metrics:
            html+='<td '+tbody_td_css+'>'+human_format(row[metric],metric in percent_metrics)+'</td>'
        html+='</tr>'
    html+='</tbody></table>'
    return html

def as_df(result,query_obj):
    query_type=query_obj['queryType']
    nres = []
    if query_type == "timeseries":
        nres = [list(v['result'].items()) + [('timestamp', v['timestamp'])]
            for v in result]
        nres = [dict(v) for v in nres]
    elif query_type == "topN":
        nres = []
        for item in result:
            timestamp = item['timestamp']
            results = item['result']
            tres = [dict(list(res.items()) + [('timestamp', timestamp)])
            for res in results]
            nres += tres
    elif query_type == "groupBy":
        nres = [list(v['event'].items()) + [('timestamp', v['timestamp'])]
        for v in result]
        nres = [dict(v) for v in nres]
    return pd.DataFrame(nres)

def human_format(num,percent=False):
    try:
        if num is None:
            return '0'
	if percent:
		return "{:.1%}".format(num)
        num = float('{:.3g}'.format(num))
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        out = '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])
        return out
    except:
        return num



def email(row,df_html):
    server = smtplib.SMTP(globalconfig.smtp, 25)
    link_html = "<br/><a href='%s'>link</a>" % row.link 
    message = MIMEMultipart("multipart", None,
                            [MIMEText(df_html.encode('utf-8'),'html'),
                             MIMEText(link_html,'html')])

    date = datetime.date.today() - timedelta(row.days)
    message['Subject'] = row.subject+ ' '+date.strftime('%m/%d')
    message['From'] = row.from_email
    message['To'] = row.to  
    server.sendmail(message['From'], message['To'].split(','), message.as_string())
    server.quit()

config = pd.read_table('config.tsv',comment='#')
config.apply(fetch_and_email,axis=1)
