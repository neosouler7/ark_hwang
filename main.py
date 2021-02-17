from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
from dateutil.parser import parse

from manager.tg_manager import TgManager
from manager.db_manager import DbManager
from utils.common import get_current_time


BASE_URL = 'https://cathiesark.com/{etf}/trades'
ARK_ETFS = ['arkk', 'arkq', 'arkf', 'arkg', 'arkw']
# ARK_ETFS = ['arkk']


class ArkTracker:
    def __init__(self):
        self.parsed_date = get_current_time('%Y%m%d')
        self.yesterday = get_current_time('%Y%m%d', -1)
        self.db_manager = DbManager()
        self.tg_manager = TgManager()

    def get_empty_trade(self, _etf, v):
        trade_date = parse(v.get('trade_date')).strftime('%Y%m%d')
        trade = {
            'ark_id': f'{_etf}_{v.get("ticker")}_{trade_date}',
            'etf': _etf,
            'ticker': v.get('ticker'),
            'trade_date': trade_date,
            'direction': v.get('direction'),
            'shares': str(v.get('shares')),
            'fund_weight': str(v.get('fund_weight').replace('%', '')),
            'parsed_date': self.parsed_date
        }
        return trade

    def get_parsed_today(self, _etf):
        print(f'requesting {self.parsed_date} / {_etf} ...')
        res = requests.get(BASE_URL.format(etf=_etf))
        tbody = BeautifulSoup(res.content.decode('utf-8'), 'html.parser').find('div', {'class': 'ant-table-container'})
        df = pd.read_html(str(tbody))[0]
        df = df.dropna(axis=0)  # 결측값 제거

        df = df[['Ticker', 'Date', 'Direction', 'Shares', 'Fund Weight']]
        df.columns = ['ticker', 'trade_date', 'direction', 'shares', 'fund_weight']
        df['ark_id'] = pd.factorize(df['ticker'] + df['trade_date'])[0]
        df.set_index('ark_id')

        parsed_info = []
        for i, (k, v) in enumerate(df.to_dict('index').items()):
            parsed_info.append(self.get_empty_trade(_etf, v))

        return parsed_info

    def run(self):
        send_target = []
        for etf in ARK_ETFS:
            time.sleep(0.5)

            # 오늘 전체 풀 리스트를 파싱해서
            parsed_today = self.get_parsed_today(etf)

            # 과거 데이터 불러와서
            parsed_info = {x.get('ark_id'): x for x in self.db_manager.get_parsed_info()}

            # 차이 계산하고, 전체 대상에 넣고
            parsed_diff = []
            for t in parsed_today:
                if not parsed_info.get(t.get('ark_id')):
                    parsed_diff.append(t)  # etf별 초기화
                    send_target.append(t)  # 전체 etf 대상

            # 차이 insert
            if parsed_diff:
                if not self.db_manager.insert_bulk_row('ark', parsed_diff):
                    print('db error!')
                    return

        # tg 보낸다.
        tg_msg = f''
        if not send_target:
            tg_msg += f'NO TRADE from ARK\n'
        else:
            for t in send_target:
                tg_msg += f'{t.get("etf").upper()} / {t.get("ticker")} / {t.get("trade_date")} / {t.get("direction")} / {t.get("shares")}({t.get("fund_weight")}%)\n'

        # tg_msg = f'hello ALL from JH (sorry JJ)\n'
        print(tg_msg)
        self.tg_manager.send_message(tg_msg)


if __name__ == "__main__":
    a = ArkTracker()
    a.run()
