import pandas as pd
import os

cleaned_CPI = pd.DataFrame(columns=['Date'])
cleaned_employment = pd.DataFrame(columns=['Date'])
cleaned_fed_funds_rate = pd.DataFrame(columns=['Date'])
cleaned_GDP = pd.DataFrame(columns=['Date'])
All_data = pd.DataFrame(columns=['Date'])
combined_stocks = pd.DataFrame(columns=['Date'])
combined_stock_values = pd.DataFrame(columns=['Date'])

def combine_stock_values(convert_to_csv):
    stock_names = os.listdir('Top 10 Current Stocks')
    stock_data = []
    for name in stock_names:
        df = pd.read_csv(f'Top 10 Current Stocks/{name}')
        df = df[['Date', 'Open']]
        df['Date'] = pd.to_datetime(df['Date'])
        df['Date'] = df['Date'].dt.to_period('M')
        df.rename(columns={'Open': f'{name.split(".")[0]}'}, inplace=True)
        stock_data.append(df)
    df = pd.read_csv('S&P 500.csv')
    df = df[['Date', 'Open']]
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.to_period('M')
    df.rename(columns={'Open': 'SP_500'}, inplace=True)
    stock_data.append(df)
    global combined_stock_values
    combined_stock_values = pd.concat(stock_data).groupby('Date').first().reset_index()
    if convert_to_csv:
        combined_stock_values.to_csv('combined stock values.csv', index=False)

def monthly_stock_return(name, stock):
    stock['Date'] = pd.to_datetime(stock['Date'])
    stock['Date'] = stock['Date'].dt.to_period('M')
    stock.set_index('Date', inplace=True)
    monthly_returns = []
    prev_stock_price = None
    for date, group in stock.groupby('Date'):
        first_price = group.iloc[0]['Open']
        if prev_stock_price is not None:
            monthly_return = (first_price - prev_stock_price) / prev_stock_price
            monthly_returns.append({'Date': date, f'{name}': monthly_return})
        prev_stock_price = first_price
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
    if convert_to_csv:
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
    
    # Convert columns to strings, remove commas, and convert to numeric
    employment['Total nonfarm'] = employment['Total nonfarm'].astype(str).str.replace(',', '').astype(float)
    employment['Total private'] = employment['Total private'].astype(str).str.replace(',', '').astype(float)
    employment['Average weekly earnings'] = employment['Average weekly earnings'].astype(str).str.replace(',', '').astype(float)
    
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

def combine(stock_returns):
    combine_stock_values(False)
    combine_stocks(True)
    clean_CPI()
    clean_employment()
    clean_fed_funds_rate()
    clean_GDP()

    # Ensure all Date columns are of the same type before merging
    combined_stock_values['Date'] = combined_stock_values['Date'].astype(str)
    combined_stocks['Date'] = combined_stocks['Date'].astype(str)
    cleaned_CPI['Date'] = cleaned_CPI['Date'].astype(str)
    cleaned_employment['Date'] = cleaned_employment['Date'].astype(str)
    cleaned_fed_funds_rate['Date'] = cleaned_fed_funds_rate['Date'].astype(str)
    cleaned_GDP['Date'] = cleaned_GDP['Date'].astype(str)

    # Merge the data
    if stock_returns:
        All_data = pd.merge(combined_stocks, cleaned_CPI, on='Date', how='outer')
    else:
        All_data = pd.merge(combined_stock_values, cleaned_CPI, on='Date', how='outer')
    All_data = pd.merge(All_data, cleaned_employment, on='Date', how='outer')
    All_data = pd.merge(All_data, cleaned_fed_funds_rate, on='Date', how='outer')
    All_data = pd.merge(All_data, cleaned_GDP, on='Date', how='outer')
    All_data['Date'] = pd.to_datetime(All_data['Date'])
    All_data['Date'] = All_data['Date'].dt.to_period('M')
    All_data = All_data[All_data['Date'] >= '2014-10']
    All_data = All_data[All_data['Date'] < '2024-01']
    if stock_returns:
        All_data.to_csv('combined data.csv', index=False)
    else:
        All_data.to_csv('combined data without stock returns.csv', index=False)

# True if with stock returns, False if with stock values
combine(True)