import pandas as pd
import investpy
import yfinance as yf
import string
import random

def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str
    
def normalize(df):
    df = df.dropna()
    return (df / df.iloc[0]) * 100

def merge_time_series(df_1, df_2, how='outer'):
    df = df_1.merge(df_2, how=how, left_index=True, right_index=True)
    return df
    
def compute_growth_index(dataframe, initial_value=100, initial_cost=0, ending_cost=0):
    initial_cost = initial_cost / 100
    ending_cost  = ending_cost / 100
    
    GR = ((1 + dataframe.pct_change()).cumprod()) * (initial_value * (1 - initial_cost))
    GR.iloc[0]  = initial_value * (1 - initial_cost)
    GR.iloc[-1] = GR.iloc[-1] * (1 * (1 - ending_cost))
    return GR 
    
def download_eod_data(tickers, key):
    Begin = '2000-03-10'
    ETFs = pd.DataFrame()
    
    # Download
    for ticker in tickers:
        try:
            url = "https://eodhistoricaldata.com/api/eod/" + str(ticker) + "?api_token=" + key + "&period=d."
            ETF = pd.read_csv(url, index_col = 'Date', parse_dates = True)[['Close']].iloc[:-1, :]
            ETFs = ETFs.merge(ETF, left_index = True, right_index = True, how='outer')
        except:
            print('Download of fund ' + ticker + ' failed')
            
    ETFs.columns = tickers
    ETFs = ETFs.fillna(method='ffill')
    ETFs = ETFs.replace(to_replace=0, method='ffill')
    
    return ETFs

def download_eod_data_single(tickers, key):
    Begin = '2000-03-10'
    ETFs = pd.DataFrame()
    
    # Download
    for ticker in tickers:
        try:
            url = "https://eodhistoricaldata.com/api/eod/" + str(ticker) + "?api_token=" + key + "&period=d."
            ETF = pd.read_csv(url, index_col = 'Date', parse_dates = True)
            ETFs = ETFs.merge(ETF, left_index = True, right_index = True, how='outer')
        except:
            print('Download of fund ' + ticker + ' failed')
            
    ETFs = ETFs.dropna().replace(to_replace=0, method='ffill')
    
    return ETFs

def download_ms(MSids, nomes, key):
    # Downloading funds and creating quotes and returns dataframes
    Begin = '2000-03-10'
    fundos = pd.DataFrame()

    # Download
    for MSid in MSids:
        try:
            url = "https://lt.morningstar.com/api/rest.svc/timeseries_price/" + key + "?id=" + str(MSid) + "&currencyId=BAS&idtype=Morningstar&frequency=daily&startDate=" + Begin + "&outputType=CSV"
            fundo = pd.read_csv(url, sep = ";" , index_col = 'date', parse_dates = True)
            fundo =  fundo.drop('Unnamed: 2', axis=1)
            fundos = fundos.merge(fundo, left_index = True, right_index = True, how='outer')
        except:
            print('Download of fund ' + MSid + ' failed')

    fundos.columns = nomes
    
    return fundos

def search_investing_etf(isins=False, tickers=False, visual=''):
    etfs = investpy.get_etfs()
    
    if isins:
        for isin in isins:
            if visual=='jupyter':
                display(etfs.loc[etfs['isin'] == isin.upper()][['symbol', 'isin', 'stock_exchange', 'currency', 'name', 'country']])
            else:
                print(etfs.loc[etfs['isin'] == isin.upper()][['symbol', 'isin', 'stock_exchange', 'currency', 'name', 'country']])
    
    elif tickers:
        for ticker in tickers:
            if visual=='jupyter':
                display(etfs.loc[etfs['symbol'] == ticker.upper()].sort_values(by='def_stock_exchange', ascending=False)[['symbol', 'isin', 'stock_exchange', 'currency', 'name', 'country']])
            else:
                print(etfs.loc[etfs['symbol'] == ticker.upper()].sort_values(by='def_stock_exchange', ascending=False)[['symbol', 'isin', 'stock_exchange', 'currency', 'name', 'country']])
                
    else:
        print('Something went wrong with the function inputs')

def get_quotes_investing_etf(names, countries, colnames='',
                             begin='1990-01-01', end='2025-01-01',
                             merge='inner',growth_index=False):    
    begin = pd.to_datetime(begin).strftime('%d/%m/%Y')
    end = pd.to_datetime(end).strftime('%d/%m/%Y')
    iteration = 0
                         
    for i in range(len(names)):
        iteration += 1
        etf = investpy.get_etf_historical_data(etf=names[i],
                                              from_date=begin,
                                              to_date=end,
                                              country=countries[i])[['Close']]
        if iteration == 1:
            etfs = etf.copy()
        
        else:
            etfs = merge_time_series(etfs, etf, how=merge)
        
    if colnames:
        etfs.columns = colnames
    else:
        etfs.columns = names
        
    if growth_index:
        etfs = compute_growth_index(etfs)
        
    return etfs

def read_xlsx_MSCI(file_name, nomes):
        
    MSCI = pd.read_excel(file_name + '.xlsx', sheet_name='Historical', index_col='As Of', parse_dates=True)[['Fund Return Series']]
    MSCI.columns = nomes
    MSCI.index.names = ['Date']
    
    return MSCI

def download_yahoo_data(tickers, normalize_quotes=True,
                      start='1970-01-01', end='2030-12-31'):
    quotes=pd.DataFrame()
    for ticker in tickers:
        df = yf.download(ticker, start=start, end=end, progress=False)
        df = df[['Adj Close']]
        df.columns=[ticker]
        quotes = merge_time_series(quotes, df)
    
    quotes = quotes.ffill()
     
    if normalize_quotes:
        quotes = normalize(quotes)

    return quotes