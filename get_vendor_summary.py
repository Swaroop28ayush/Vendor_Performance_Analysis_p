import sqlite3
import time
import pandas as pd

import logging
logging.basicConfig(
    filename = r'D:\credit_\data\logs\get_vendor_summary.log',
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = 'a'
)

from ingestion_db import ingest_db

def create_vendor_summary(conn):
    '''
    This function will merge the different tables to get the overall vendor summary and adding new        columns in the resultant data
    
    '''

    start = time.time()
    vendor_sales_summary = pd.read_sql("""with FreightSummary as (
    select
    VendorNumber,
    sum(Freight) as FreightCost
    from vendor_invoice
    group by VendorNumber
    ),
    
    PurchaseSummary as (
    select
    p.VendorNumber,
    p.VendorName,
    p.Brand,
    p.Description,
    p.PurchasePrice,
    pp.Price as ActualPrice,
    pp.Volume,
    sum(p.Quantity) as TotalPurchaseQuantity,
    sum(p.Dollars) as TotalPurchaseDollars
    from purchases as p
    join purchase_prices as pp
    on p.Brand = pp.Brand
    where p.PurchasePrice > 0
    group by p.VendorNumber,p.VendorName,p.Brand,p.Description,p.PurchasePrice,pp.Price,pp.Volume
    ),
    
    SalesSummary as (
    select 
    VendorNo,
    Brand,
    sum(SalesQuantity) as TotalSalesQuantity,
    sum(SalesDollars) as TotalSalesDollars,
    sum(SalesPrice) as TotalSalesPrice,
    sum(ExciseTax) as TotalExciseTax
    from sales
    group by VendorNo,Brand
    )
    
    select
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
    from PurchaseSummary as ps
    left join SalesSummary as ss
    on ps.VendorNumber = ss.VendorNo
    and ps.Brand = ss. Brand
    Left join FreightSummary as fs
    on ps.VendorNumber = fs.VendorNumber
    order by ps.TotalPurchaseDollars desc
    """,conn)
    end = time.time()
    print(f'Total Processing Time: {(end-start)/60} minutes.')

    return vendor_sales_summary



def clean_data(df):
    '''This Function will clean the data'''

    # changing Datatype to float
    df['Volume'] = df['Volume'].astype('float64')

    # filling the missing values with 0
    df.fillna(0,inplace=True)

    # removing Spaces from categorical Variables
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Creating New Columns for Better Analysis
    df['GrossProfit'] = df['TotalSalesDollars'] -df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit']/df['TotalSalesDollars'])*100
    df['StockTurnOver'] = df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars']/df['TotalPurchaseDollars']
    return df


if __name__ == '__main__':
    # creating Database connection
    conn = sqlite3.connect('inventory.db') 

    logging.info('Creating Vendor Summary Table...')
    summary_df = create_bendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('cleaning Data........')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting Data......')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')

    