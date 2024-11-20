import pandas as pd

cleaned_CPI = pd.DataFrame(columns=['Date'])
cleaned_employment = pd.DataFrame(columns=['Date'])
cleaned_fed_funds_rate = pd.DataFrame(columns=['Date'])
cleaned_GDP = pd.DataFrame(columns=['Date'])
All_data = pd.DataFrame(columns=['Date'])
combined_stocks = pd.DataFrame(columns=['Date'])

def monthly_stock_return(name, stock):
    stock['Date'] = pd.to_datetime(stock['Date'])
    stock['Date'] = stock['Date'].dt.to_period('M')
    stock.set_index('Date', inplace=True)
    curr_date = stock.index[0]
    prev_stock_price = stock.iloc[0]['Open']
    curr_stock_price = 0
    monthly_returns = []
    for date, row in stock.iterrows():
        if date.month != curr_date.month:
            curr_date = date
            curr_stock_price = row['Open']
            monthly_return = (curr_stock_price - prev_stock_price) / prev_stock_price
            monthly_returns.append({'Date': date, f'{name}': monthly_return})
            prev_stock_price = row['Close/Last']
    monthly_returns_df = pd.DataFrame(monthly_returns)
    global combined_stocks
    combined_stocks = pd.merge(combined_stocks, monthly_returns_df, on='Date', how='outer')

def combine_stocks(convert_to_csv):
    stock_names = ['AAPL', 'AMZN', 'GOOGL', 'MSFT', 'TSLA', 'BRK_B', 'META', 'NVDA', 'UNH', 'S&P 500']
    stock_dataframes = [pd.read_csv(f'Top 10 Current Stocks/{name}.csv') for name in stock_names[:-1]]
    stock_dataframes.append(pd.read_csv('S&P 500.csv'))

    for i in range(len(stock_names)):
        monthly_stock_return(stock_names[i], stock_dataframes[i])

    global combined_stocks
    combined_stocks['Date'] = combined_stocks['Date'].astype(str)
    combined_stocks = combined_stocks.sort_values(by='Date').reset_index(drop=True)
    if (convert_to_csv):
        combined_stocks.to_csv('combined stocks.csv', index=False)

def clean_CPI():
    cpi = pd.read_csv('Consumer Price Index.csv')
    cpi['DATE'] = pd.to_datetime(cpi['DATE'])
    cpi['DATE'] = cpi['DATE'].dt.to_period('M')
    cleaned_CPI['Date'] = cpi['DATE'].dt.strftime('%Y-%m')
    cleaned_CPI['CPI'] = cpi['Value']

def clean_employment():
    employment = pd.read_csv('Employment_Data.csv')
    employment['Month'] = employment['Month'].astype(str).str.zfill(2)
    cleaned_employment['Date'] = employment['Year'].astype(str) + '-' + employment['Month']
    cleaned_employment['Total nonfarm'] = employment['Total nonfarm']
    cleaned_employment['Total private'] = employment['Total private']
    cleaned_employment['Average weekly earnings'] = employment['Average weekly earnings']
    return cleaned_employment

def clean_fed_funds_rate():
    fed_funds = pd.read_csv('Federal Funds Effective Rate.csv')
    fed_funds['MONTH'] = pd.to_datetime(fed_funds['MONTH'])
    fed_funds['MONTH'] = fed_funds['MONTH'].dt.to_period('M')
    cleaned_fed_funds_rate['Date'] = fed_funds['MONTH'].dt.strftime('%Y-%m')
    cleaned_fed_funds_rate['FEDFUNDS'] = fed_funds['FEDFUNDS']
    
def clean_GDP():
    gdp = pd.read_csv('Gross Domestic Product.csv')
    gdp['DATE'] = pd.to_datetime(gdp['DATE'])
    gdp['DATE'] = gdp['DATE'].dt.to_period('M')
    cleaned_GDP['Date'] = gdp['DATE'].dt.strftime('%Y-%m')
    cleaned_GDP['GDP'] = gdp['GEPUCURRENT']

combine_stocks(False)
clean_CPI()
clean_employment()
clean_fed_funds_rate()
clean_GDP()

All_data = pd.merge(combined_stocks, cleaned_CPI, on='Date', how='outer')
All_data = pd.merge(All_data, cleaned_employment, on='Date', how='outer')
All_data = pd.merge(All_data, cleaned_fed_funds_rate, on='Date', how='outer')
All_data = pd.merge(All_data, cleaned_GDP, on='Date', how='outer')
All_data['Date'] = pd.to_datetime(All_data['Date'])
All_data['Date'] = All_data['Date'].dt.to_period('M')
All_data = All_data[All_data['Date'] >= '2014-10']
All_data = All_data[All_data['Date'] < '2024-01']
All_data.to_csv('combined data.csv', index=False)
