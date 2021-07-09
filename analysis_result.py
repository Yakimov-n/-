import pandas as pd
import bs4
import matplotlib.pyplot as plt
import numpy as np
from datetime import timedelta, datetime
plt.style.use('ggplot')  # Красивые графики
plt.rcParams['figure.figsize'] = (15, 5)  # Размер картинок
"""читаем блокчейн"""
dg = pd.read_csv('github.com/Yakimov-n/transaction-analysis\
/blob/215aadd0551a8300b2ee4d82fc59cb8f84ccc39a/btc-tx.csv', sep = ',', encoding='latin1',parse_dates=['time'], dayfirst=True, error_bad_lines=False)
"""находим и оставляем строки, которые содержат числа, а так же меняем значение, если строка съехала"""
def safe_float_convert(x):
    try:
        float(x)
        return True                                     # numeric, success!
    except ValueError:
        return False                                    # not numeric
    except TypeError:
        return False                                    # null type
mask = dg['Sender'].map(safe_float_convert) # сопоставляем новую функцию со столбцом B dataframe, поэлементно, и создаем логическую маску
numeric_dg = dg.loc[mask]
numeric_dg = numeric_dg[numeric_dg['Sender'].notna()]
mask_id = numeric_dg.index[numeric_dg['Sender'].notna()]
dg['Transaction_amount_BTC'][mask_id-1] = dg['Sender'][mask_id]
dg = (dg.drop('Transaction_amount_BTC', axis=1)
         .join(dg['Transaction_amount_BTC'].apply(pd.to_numeric, errors='coerce')))
dg = dg[dg['Transaction_amount_BTC'].notna()]
dg['Transaction_amount_BTC'] = dg['Transaction_amount_BTC'].astype(float)
dg['time'] = pd.to_datetime(dg['time'], errors = 'coerce')
dg['time'] = dg['time'].ffill()                                                     # заполнение столбца time по предыдущему значению
dg = dg.sort_values('Hash', kind = 'mergesort')
dg_h = dg.groupby('Hash').sum()   # объединяем операции с одинаковым хешем
dg = dg[['time','Hash','Transaction_amount_BTC']]
dg = dg.drop_duplicates(subset='Hash').set_index('Hash')
dg['Transaction_amount_BTC'] = dg_h['Transaction_amount_BTC']
dg = dg.reset_index()
dg = dg[dg['Transaction_amount_BTC'].notna()].sort_values('time', kind='mergesort').set_index('time')
db = pd.read_csv(https://github.com/Yakimov-n/transaction-analysis/blob/3cac418fb175b12f5fc50bdb581c\
6a9a5e217f86/%D0%9F%D1%80%D0%BE%D1%88%D0%BB%D1%8B%D0%B5%20%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D0%B5%20-%20B\
TC_USD%20Bitfinex.csv', sep = ',', parse_dates=['Дата'], dayfirst=True) # читаем статистику по стоимости биткоина
db = db.sort_values("Дата", kind = 'mergesort')
db['Цена'] = db['Цена'].str.replace('[.]','').str.replace('[,]','.').astype(float)
"""выбираем интервал на основе данных блокчейна"""
dt_1 = datetime(2015, 1, 2)
dt_2 = datetime(2017,11,18)
db = db[db['Дата']>=dt_1] 
db = db[db['Дата']<=dt_2]
db = db.rename(columns = {'Дата':'time'}).set_index('time')
dg = dg.join(db['Цена']).drop(['Hash'], axis=1) # добавляем столбец с ценой
dg = pd.concat([dg,(dg['Transaction_amount_BTC'].multiply(dg['Цена'], axis=0))],axis=1) # получем суммы транзакций в $
dg = dg.set_axis(['Transaction_amount_BTC','Цена','total'],axis=1)
"""читаем статистику банковских транзакций"""
df1 = pd.read_csv('https://github.com/Yakimov-n/transaction-analysis/blob\
/3cac418fb175b12f5fc50bdb581c6a9a5e217f86/download_transactions_map.csv', sep = ',', parse_dates=['begin_date', 'end_date'],dayfirst=True)
df1 = df1.sort_values("end_date", kind = 'mergesort')
dg = dg.reset_index()
df_mask = dg['total'].values
flt = df1['amount_transactions'].isin(df_mask) # находим в банковских транзакциях суммы одинаковые с суммами блокчейна
df1 = df1.loc[flt]
"""выбираем суммы близкие к млн и смотрим, что происходило за одни сутки и через одни сутки после транзакции"""
df1 = df1.loc[df1['amount_transactions']>=0.99e+06]
dg_t = dg['time'].unique()
dg_t1 = dg_t+np.timedelta64(1,'D')
dg_t2 = dg_t-np.timedelta64(1,'D')
dg_tt = np.r_[dg_t, dg_t1, dg_t2]
dg_tt = set(dg_tt)
dg_tt = pd.DataFrame(dg_tt)
dg_tt = dg_tt.set_axis(['time'], axis=1)
df1 = df1.loc[df1['end_date'].isin(dg_tt['time'].values)]
df1

