import http.client
import requests
import json
import pandas as pd
import numpy as np
from pandas import ExcelWriter

import time
import datetime as dt


def login():
    conn = http.client.HTTPSConnection("primaryventures.auth0.com")

    payload = "{\"client_id\":\"dxtYo5a2tGhzYBXiNpMXwQTTnmbVR1lA\",\"client_secret\":\"2uclM1UEkwcfzO278ZL3sA4MXvfv2ci1Y8v6WEmpyVR73s-Ofu1N9phxOLYHXdOs\",\"audience\":\"https://pmy-data-api.com\",\"grant_type\":\"client_credentials\"}"

    headers = {'content-type': "application/json"}

    conn.request("POST", "/oauth/token", payload, headers)

    res = conn.getresponse()
    data = res.read()

    token = {'access_token': json.loads(data.decode('utf-8'))['access_token']}
    return token


def create_reports(token, from_date, to_date, type):
    reports_id = []
    mmk = get_market_maker(token)[0]
    #metric = requests.get(url='http://api.surveillance.pmydata.com/api/v1/metric_type_data/', headers=token)
    #mmk_type = requests.get(url='http://api.surveillance.pmydata.com/api/v1/market_maker_type/', headers=token)
    mmk_data = pd.DataFrame(
        [(m['id'], mm1['market_member'], mm1['product'], mm1['metric_type_data'][0]['market_maker_type']) for mm1 in mmk
         for m in mm1['accounts']], columns=['id_account', 'market_member', 'product', 'market_maker_type'])

    if type == 'volume':
        baseUrl = 'https://api.surveillance.pmydata.com/api/v1/reports/volume/report/'
    elif type == 'time':
        baseUrl = 'https://api.surveillance.pmydata.com/api/v1/reports/time/report/'
        mmk_data = mmk_data[mmk_data['market_maker_type'] == 1] # solo ['market_maker_type'] == 1 tiene metrica de tiempo

    for mm in mmk_data['market_member'].unique().tolist():
        product = mmk_data[mmk_data['market_member'] == mm]['product'].unique().tolist()
        id_account = mmk_data[mmk_data['market_member'] == mm]['id_account'].unique().tolist()
        json_string = {"from_date": from_date, "to_date": to_date, "market_member": mm, "accounts": id_account,
                       "products": product}
        create_report = requests.post(baseUrl, json=json_string, headers=token)
        if type == 'time':
            time.sleep(30)
        try:
            reports_id.append(create_report.json()['result'])
        except:
            print(create_report.status_code)
            print(create_report.content)
            print(json_string)

    return reports_id


def get_reports(token, ids):
    volume_excel = ExcelWriter('volume.xlsx')
    all_reports = pd.DataFrame()
    for r in ids:
        url = 'https://api.surveillance.pmydata.com/api/v1/reports/volume/report/' + r
        report = requests.get(url=url, headers=token)
        if report.json()[0]['status'] == 'SUCCESS':
            #reports = requests.get(url=baseUrl_volume, headers=token) # todos los reportes
            df_report = pd.DataFrame(report.json()[report_name])
            df_report['market_member'] = report.json()['market_member']
            df_report['from_date'] = report.json()['from_date'].__str__()
            df_report['to_date'] = report.json()['to_date'].__str__()
            all_reports.append(df_report)
            df_report.to_excel(volume_excel, sheet_name=report.json()['market_member'].__str__())
        else:
            print(r.ljust(25), report.json()[0]['status'].ljust(15))
            print()
    return all_reports.reset_index()


def get_all_reports(token, type):
    if type == 'volume':
        url = 'https://api.surveillance.pmydata.com/api/v1/reports/volume/report/'
    elif type == 'time':
        url = 'https://api.surveillance.pmydata.com/api/v1/reports/time/report/'

    reports = requests.get(url=url, headers=token)
    reports_data = [(r['from_date'], r['to_date'], r['report_type'], r['market_member'], r['state'], r['task_id'], r['created']) for r in reports.json()]
    return reports_data, reports


def get_market_member(token):
    url = 'https://api.surveillance.pmydata.com/api/v1/market_member/'
    market_members = requests.get(url=url, headers=token)
    return market_members.json()


def get_product(token):
    url = 'https://api.surveillance.pmydata.com/api/v1/product/?config=true'
    products = requests.get(url=url, headers=token)
    return products.json()


def get_market_maker(token):
    # ['id', 'metric_type_data', 'accounts', 'emails', 'market_member', 'product']
    url = 'https://api.surveillance.pmydata.com/api/v1/market_maker/'
    market_maker = requests.get(url=url, headers=token)
    df_mmk = pd.DataFrame(market_maker.json())[['id', 'emails', 'market_member', 'product']]
    #df_mmk = pd.DataFrame(
    #    [dict((k, d[k]) for k in ['id', 'emails', 'market_member', 'product']) for d in market_maker.json()])
    market_maker_type = pd.json_normalize(market_maker.json(), 'metric_type_data')[['market_maker_type']]
    df_mmk = df_mmk.join(market_maker_type)
    return market_maker.json(), df_mmk


def post_market_maker_mail(token, mail, mmk):
    url = 'https://api.surveillance.pmydata.com/api/v1/market_maker_email/'
    response = []
    for m in mail:
        postMail = {"email": m, "market_maker": mmk}
        print(postMail)
        response.append(requests.post(url=url, json=postMail, headers=token).json())
    return response


def post_list_mail(token):
    mmk_mails = pd.read_excel('mmk_mmember.xlsx', sheet_name='Sheet1', engine='openpyxl')
    l_mails = list(map(lambda x: str(x).split(';'), mmk_mails['mails'].to_list()))
    for mmk, mails in zip(mmk_mails['id'].to_list(), l_mails):
        if not mails[0] == 'nan':
            post_market_maker_mail(token, map(lambda x: x.replace(' ',''), mails), mmk)


def get_mmk_emails(token):
    mm = pd.DataFrame(get_market_member(token))
    _, df_mmk = get_market_maker(token)
    mmk_mm = df_mmk.set_index(['market_member']).join(mm.set_index(['id']))
    mmk_mm.to_excel('mmk_mmember.xlsx')
    return mmk_mm


def get_volume_total(token, df_all_reports, trading_sessions):
    totals = df_all_reports[df_all_reports['is_total']]['instrument_symbol'].str.split('-', expand=True).join(
        df_all_reports, how='left')
    totals.rename(columns={0: 'type', 1: 'product'}, inplace=True)
    fields = ['volumen', 'volumen_agresor', 'volumen_agresor_a_clientes',
              'volumen_agresor_a_mmk', 'volumen_agredido', 'volumen_agredido_a_clientes', 'volumen_agredido_a_mmk']
    totals[fields] = totals[fields].astype('float')
    totals['market_member'] = totals['market_member'].astype('int64')
    #totals.loc[totals[totals['type'] == 'FUTURES_SPREAD '].index, fields] = totals[totals[
    #                                                                                   'type'] == 'FUTURES_SPREAD '] * 2  # duplico los pases
    market_members = get_market_member(token)
    totals = totals.join(pd.DataFrame(market_members)[['id', 'name']].set_index('id'), on='market_member', how='left')
    # totals['RatioAgresor'] = totals.volumen_agresor/totals.volumen_agredido
    # totals['ADV'] = totals.volumen/trading_sessions

    total_prod_member = totals.groupby(['name', 'product'])[fields].sum()
    total_prod_member['ADV'] = total_prod_member.volumen / trading_sessions
    total_prod_member['RatioAgresor'] = total_prod_member.volumen_agresor / total_prod_member.volumen_agredido

    return total_prod_member


def json_performance(token):
    json_mmk, _ = get_market_maker(token)
    mmk_df = pd.DataFrame(json_mmk)
    mmk_df['id_account'] = mmk_df.accounts.apply(lambda x:[f['id'] for f in x])
    mmk_df = mmk_df.join(
        pd.json_normalize(json_mmk, 'metric_type_data'), rsuffix="_metric")
    mmk_df.sort_values(['product', 'market_maker'], inplace=True)
    metrics = dict(zip(["average_daily_volume", "ratio", "frequency", "quantity"], [True, True, True, False]))
    market_makers = ["uid", "product_name", "product", "market_maker", "accounts", "percentage", "metrics", "metrics_information"]
    #uid: numero incremental desde 1 para cada producto que se mide para las mismas cuentas (un agente que se mide en varios productos)
    for i in mmk_df.index:
        mmk_df.iloc[i][["product_name", "product", "market_maker", "id_account"]].to_list()


def performance_report(token, ddf):
    url = 'https://api.surveillance.pmydata.com/api/v1/reports/performance/report/'
    if ddf:
        body = """{"from_date":"2021-09-01","to_date":"2021-09-30","market_makers":[{"uid":1,"product_name":"Dólar USA A3500","product":1,"market_maker":13,"accounts":[55743,71542,96836,96870],"percentage":null,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":false},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":true}},{"uid":2,"product_name":"Indice ROFEX 20","product":413,"market_maker":30,"accounts":[55743,71542,96836,96870,99667],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":1,"product_name":"Dólar USA A3500","product":1,"market_maker":8,"accounts":[59284,60578],"percentage":null,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":false},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":true}},{"uid":1,"product_name":"Dólar USA A3500","product":1,"market_maker":1,"accounts":[3444,3452],"percentage":null,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":false},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":true}},{"uid":1,"product_name":"Dólar USA A3500","product":1,"market_maker":7,"accounts":[6727,53663],"percentage":null,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":false},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":true}},{"uid":1,"product_name":"Dólar USA A3500","product":1,"market_maker":11,"accounts":[9666,68211,68213,68214,68215,68394,68395,68396,71511],"percentage":null,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":false},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":true}},{"uid":1,"product_name":"Petróleo Crudo WTI","product":6,"market_maker":22,"accounts":[89385,103227,103228,103229,103230],"percentage":true,"metrics":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":2,"product_name":"Oro Fino","product":5,"market_maker":70,"accounts":[89385,103227,103228,103229,103230],"percentage":true,"metrics":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":3,"product_name":"Indice ROFEX 20","product":413,"market_maker":20,"accounts":[34367,35209,35210,35213,35214,47061,89385],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":4,"product_name":"GGAL ROFEX","product":463,"market_maker":31,"accounts":[34367,35209,35210,35213,35214,47061],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":5,"product_name":"PAMP MTR","product":678,"market_maker":32,"accounts":[34367,35209,35210,35213,35214,47061],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":6,"product_name":"YPFD MTR","product":679,"market_maker":33,"accounts":[34367,35209,35210,35213,35214,47061],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":1,"product_name":"Indice ROFEX 20","product":413,"market_maker":40,"accounts":[36875,100144],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":true,"frequency":true,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":2,"product_name":"GGAL ROFEX","product":463,"market_maker":34,"accounts":[36875,100144],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":3,"product_name":"PAMP MTR","product":678,"market_maker":35,"accounts":[36875,100144],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}},{"uid":4,"product_name":"YPFD MTR","product":679,"market_maker":36,"accounts":[36875,100144],"percentage":true,"metrics":{"average_daily_volume":true,"ratio":false,"frequency":false,"quantity":true},"metrics_information":{"average_daily_volume":false,"ratio":false,"frequency":false,"quantity":false}}]}"""
    else:
        body = ""

    data = json.loads(body)
    reports = requests.post(url=url, json=data, headers=token)
    return reports


def get_performance_report(token):
    url = 'https://api.surveillance.pmydata.com/api/v1/reports/performance/report/'
    reports = requests.get(url=url, headers=token)
    return reports


def hist_performance_report(token, months_list):
    reports = get_performance_report(token)
    performance = pd.DataFrame([])
    for r in reports.json():
        if r['products_rows_task']:
            p = pd.DataFrame(r['products_rows_task'])
            p['from_date'] = r['from_date']
            p['to_date'] = r['to_date']
            p['best_quote'] = p['contract_rows_product'].apply(lambda x: pd.json_normalize(x[0]).spread_quantity.max())
            if performance.empty:
                performance = p
            else:
                performance = performance.append(p)
        else:
            continue

    df_market_member = pd.DataFrame(get_market_member(token))[['id', 'name']].set_index(['id'])
    mmk, df_market_maker = get_market_maker(token)
    df_market_maker = df_market_maker[['id', 'market_member', 'market_maker_type']].set_index(['id'])
    df_market_maker = df_market_maker.join(df_market_member, on='market_member')[['name', 'market_maker_type']]
    df_market_maker.rename(columns={'name': 'mmk_name'}, inplace=True)
    products = pd.DataFrame(get_product(token))[['id', 'name']].set_index(['id'])
    products.rename(columns={'name': 'prod_name'}, inplace=True)
    performance = performance.join(products, on='product', how='left').join(df_market_maker, on='market_maker', how='left')
    performance.set_index(['id'], drop=True, inplace=True)
    performance.sort_index(ascending=False, inplace=True)
    more_than_days = pd.to_datetime(performance.to_date)-pd.to_datetime(performance.from_date) >= dt.timedelta(days=29)
    months = pd.to_datetime(performance.to_date).dt.month.isin(months_list)
    performance = performance[(more_than_days & months)]
    performance.to_excel('hist_performance.xlsx')
    return performance


def get_xls(token, type, task_id):
    url = 'https://api.surveillance.pmydata.com/api/v1/reports/{0}/report/{1}/download_csv/'.format(type, task_id)
    r = requests.get(url=url, headers=token)
    return r


def convert(x):
    try:
        return np.float64(x)
    except:
        return x
