import numpy as np
import pandas as pd
import sklearn as sk
#import PyTorch as pt

#load methods from given code + rest of files
def load_customer_info():
    return pd.read_csv("data/NYU_Cust_Info.txt",sep="|",names=["Customer_id","age","FirstContactDay","PBK_Ind","HIB_Status","OccuCode","OccuDesc","Gender","Cntry_Correspondence","InterCorpACIndicator","NationCode","IncomeLevel","Salary","Period_Salary","Marital_Status","Number_Children","Education_Level","Home_Ownership","Car_Ownership","Cust_Segment","BusSector","BusDesc"])

def load_transaction_history():
    return pd.read_csv("data/NYU_DDTNJNP.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Customer_ID","Acct_id","dpvldt","dpxccy","LCY_AMT","FCY_AMT","Tran_Type","product_code"])

def load_ISST_Summary():
    return pd.read_csv("data/NYU_ISST_Summary.txt", 
                       sep="|",
                       names=["Event_Date","Channel","Customer_ID","Producttype","Pagetype","Pageviewnum","Pageviewtime"])

#def load_Credit_Card_Txn():
 #   return pd.read_csv("data/NYU_Credit_Card_TXN.txt", 
  #                     sep="|",
   #                    enconding = "unicode_escape",
    #                   names = ["Customer_ID", "ARR_ID_CARD", "MERCHANT", "XTRANNO", "TRANS_TYPE", "TRANS_GROUP", "TRANS_GROUP_SUB", "BILL_AMTFLAG", "BILL_AMT", "BILL_CURRENCY", "ORGN_AMT", "ORGN_CURRENCY", "CARD_PLAN")]

def load_DD_Acct_20170630():
    return pd.read_csv("data/NYU_DD_Acct_20170630.txt",
                      sep = '|',
                      encoding = "unicode_escape",
                      names = ["Product_Class", "Customer_id", "Acct_id", "ACOpenDate", "Currency", 
                              "RCYEOD", "LCYEOD", "MTDAVG"])
def load_Cust_Purchase_Ind():
    return pd.read_csv("data/NYU_Cust_Purchase_Ind.txt", 
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Customer_ID", "Month 1", "Month 2", "Month 3", "Month 4", "Month 5", "Month 6", "Month 7", "Month 8", "Month 9", "Month 10", "Month 11", "Month 12", ])

"""
   _____ _____ _____ 
  / ____|  __ \_   _|
 | |    | |__) || |  
 | |    |  ___/ | |  
 | |____| |    _| |_ 
  \_____|_|   |_____|   
  
  Capital Protected Investment
  
  Fixed term, principle protected investment product. Upon maturity, investor receives all principle and variable interest.
                     
  Functions to load the CPI data, the load_CPI function loads both the mapping data (containing relevant product information)
  and the holding data (who holds what product).

"""
def load_CPI_holding():
    return pd.read_csv("data/NYU_CPI_Holding_20170630.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Product_Class","Customer_id","Acct_id","ACOpenDate","Currency","RCYEOD","LCYEOD","MTDAVG","Product_Code"])

def load_CPI_mapping():
    return pd.read_csv("data/NYU_CPI_Mapping.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Product_Code","Investment_Currency","Start_Date","Maturity_Date","NAV"])

def load_CPI():
    df1 = load_CPI_holding();
    df2 = load_CPI_mapping();

    #some products are not defined in both sets so we choose to drop them
    df = df1.merge(df2, on="Product_Code", how="inner");
    
    if((df1.shape[0] - df.shape[0]) > 0):
        print("Warning: dropped ", (df1.shape[0] - df.shape[0]) ,"rows due to failure to look up Product Code.");
    return df


"""
   ____  _____  _    _ _______ 
  / __ \|  __ \| |  | |__   __|
 | |  | | |  | | |  | |  | |   
 | |  | | |  | | |  | |  | |   
 | |__| | |__| | |__| |  | |   
  \___\_\_____/ \____/   |_|   
  
  Mutual Funds
                                                    
  Functions to load the QDUT data and mappings.

"""
def load_QDUT_holding():
    return pd.read_csv("data/NYU_QDUT_Holding_20170630.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Customer_id","Product_Code","Currency","FUM_RCY","FUM_LCY"])
def load_QDUT_mapping():
    return pd.read_csv("data/NYU_QDUT_Mapping.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Product_Code","Y_M","Product_Type","Investment_Currency","NAV_YYYYMM"])
def load_QDUT():
    df1 = load_QDUT_holding();
    df2 = load_QDUT_mapping();

    df = df1.merge(df2, on="Product_Code", how="inner");
    if((df1.shape[0] - df.shape[0]) > 0):
        print("Warning: dropped ", (df1.shape[0] - df.shape[0]) ,"rows due to failure to look up Product Code.");
    return df


"""
  _____                                          
 |_   _|                                         
   | |  _ __  ___ _   _ _ __ __ _ _ __   ___ ___ 
   | | | '_ \/ __| | | | '__/ _` | '_ \ / __/ _ \
  _| |_| | | \__ \ |_| | | | (_| | | | | (_|  __/
 |_____|_| |_|___/\__,_|_|  \__,_|_| |_|\___\___|
                                                 
                                                 
    Insurance product with different needs focus (Protection, Education, Retirement, Medical, Legacy). Purchased by paying premium. Market value of insurance product is not always equal to the premium paid.
 
"""

def load_insurance_holding():
    return pd.read_csv("data/NYU_Insurance_Holding_20170630.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Customer_id","Product_Code","Term","Market_Value","Insurer"])

def load_insurance_mapping():
    return pd.read_csv("data/NYU_Insurance_Mapping.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Product_Code","Needs","Insurer"])
def load_insurance():
    df1 = load_insurance_holding();
    df2 = load_insurance_mapping()

    df = df1.merge(df2, on=["Product_Code","Insurer"], how="inner");
    if((df1.shape[0] - df.shape[0]) > 0):
        print("Warning: dropped ", (df1.shape[0] - df.shape[0]) ,"rows due to failure to look up Product Code.");
    return df


"""

  _______ __  __ _____  
 |__   __|  \/  |  __ \ 
    | |  | \  / | |  | |
    | |  | |\/| | |  | |
    | |  | |  | | |__| |
    |_|  |_|  |_|_____/     
    
    Term Deposit
    
    Fixed term, fixed interest deposit. Investor receives all principle and fixed interest upon product maturity.
                        
"""

def load_TD_holding():
    return pd.read_csv("data/NYU_TD_Holding_20170630.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names=["Product_Class","Customer_id","Acct_id","ACOpenDate","Currency","RCYEOD","LCYEOD","MTDAVG","Term","Startdate","Duedate"])

def load_TD_mapping():
    return pd.read_csv("data/NYU_TD_mapping.txt",
                       sep="|",
                       encoding="unicode_escape", 
                       names =["Product_Class","Customer_id","Acct_id","ACOpenDate","Currency","Term","Startdate","Duedate"])

def load_TD():
    df1 = load_TD_holding();
    df2 = load_TD_mapping();
    df2 = df2.drop(["ACOpenDate","Currency","Product_Class","Term","Startdate","Duedate"],axis=1)

    df = df1.merge(df2, on=["Customer_id","Acct_id"], how="inner");
    if((df1.shape[0] - df.shape[0]) > 0):
        print("Warning: dropped ", (df1.shape[0] - df.shape[0]) ,"rows due to failure to look up Product Code.");
    return df