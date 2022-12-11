from binance.spot import Spot
import requests
import os
from google.cloud import bigquery
import schedule

toTerahash = 1000000000000
CREDS_BIGQUERY = '/creds/bigsurmining-14baacf42c48.json'
KEYBINANCE = os.environ['KEYBINANCE']
SECRETBINANCE = os.environ['SECRETBINANCE']
client = Spot(key=KEYBINANCE, secret=SECRETBINANCE)

def bigQueryUpdate(query):
    client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS_BIGQUERY)
    bq_response = client.query(query=f'{query}').to_dataframe()
    return bq_response

def bigQueryRead(query):
    client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS_BIGQUERY)
    bq_response = client.query(query=f'{query}').to_dataframe()
    return bq_response


def loadUsersBQ():
    usuariosPoolList = []
    usuariosDF = bigQueryRead("SELECT usuariosPool FROM BD1.usuarios ORDER BY id ASC")
    for usuario in usuariosDF["usuariosPool"]:
        usuariosPoolList.append(usuario)
        print(f"Cargado usuario {usuario}")
    return usuariosPoolList

def job():
    usuariosPoolList = loadUsersBQ()
    for usuariosPool in usuariosPoolList:
        try:
            json1 = (client.mining_statistics_list(algo="sha256", userName=usuariosPool))
            hashrate = (str(float(json1['data']['dayHashRate'])/toTerahash))
            workers_active = (json1['data']['validNum'])
            workers_inactive = (json1['data']['invalidNum'])
            paidTodayEstimate = json1['data']['profitToday']['BTC']
        except Exception as e:
            hashrate = 0
            workers_active = 0
            workers_inactive = 0
            paidTodayEstimate = 0
        print(f"User: {usuariosPool} Hashrate: {hashrate}, Active Workers: {workers_active}, Offline Workers: {workers_inactive}, Paid Today Estimate: {paidTodayEstimate}")
        bigQueryUpdate(f"UPDATE BD1.usuarios SET actualHashrate={hashrate}, activeWorkers={workers_active}, inactiveWorkers={workers_inactive}, paidTodayEstimate={paidTodayEstimate} WHERE usuariosPool='{usuariosPool}'")

schedule.every(5).minutes.do(job)

while True:
    schedule.run_pending()
