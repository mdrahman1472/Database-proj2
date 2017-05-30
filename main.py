# Author: MD Rahman, Database final project
# This file is main() function. You need to run this file 

import MySQLdb
from datetime import datetime
from data_proj_2 import*
from history_update import*

# Connect to the database
con = MySQLdb.connect(host='134.74.126.107',
                             user='####',
                             passwd='####',
                             db='F16336team8',
                     
)
con.autocommit(True) # commit all sql automaticaly 


#curr_datetime = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
#curr_datetime = datetime.strptime(curr_datetime, '%y-%m-%d %H:%M:%S') # converting to datetime format

#curr_date = datetime.now().date() # current date only


# declaring variables and initializing them
instr_id = 0        # INSTRUMENT_ID
seq_nbr = 0         # QUOTE_SEQ_NBR
trade_symb = ""     # TRADING_SYMBOL
ask_price = 0.0     # ASK_PRICE
ask_size = 0        # ASK_SIZE
bid_price = 0.0     # BID_PRICE
bid_size = 0        # BID_SIZE

#Taking input from user
instr_id = input("Enter Instrument ID: ")
trade_symb = input('Enter Trading Symbol(inside " " mark): ')
ask_price = input("Enter Ask price: ")
ask_size = input("Enter Ask size: ")
bid_price = input("Enter Bid price: ")
bid_size = input("Enter Bid size: ")

# calling function which has algoritm for matching engine 
matchingEngine(con, instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size) 


