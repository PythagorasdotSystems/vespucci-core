import requests

def run_cron_func(func, cron_url):
    try:
        func()
    except:
        requests.get(cron_url + '/fail')
        raise
    else:
        requests.get(cron_url)
