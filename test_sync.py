import baostock as bs
import pandas as pd

lg = bs.login()
print(f'登录: {lg.error_code}')

rs = bs.query_history_k_data_plus(
    code='sh.600000',
    fields='date,open,high,low,close,volume,amount,pctChg',
    start_date='2024-01-01',
    end_date='2024-01-10',
    frequency='d',
    adjustflag='2'
)

data_list = []
while rs.next():
    data_list.append(rs.get_row_data())

df = pd.DataFrame(data_list, columns=rs.fields)
print(df)
bs.logout()