# Author: MD Rahman, Database final project
# This file has several functions defination but MatchingEngine() is main function which call others functions when needed

from datetime import datetime, timedelta # importing for current  date and time
import time
import MySQLdb
import MySQLdb.cursors
import random
from  decimal import Decimal
from history_update import*

 
def matchingEngine(con,instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size):
	
	#---------------------------------- First trade that will inserted by user---------------------------------------------
	curr_datetime = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
	curr_datetime = datetime.strptime(curr_datetime, '%y-%m-%d %H:%M:%S') # converting to datetime format
	curr_date = datetime.now().date() # current date only
	
	seq_nbr = getSeqNbr(con, trade_symb) # genarating QUOTE_SEQ_NBR
	# insert quote
	insert_fun (con, instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size, seq_nbr, curr_datetime, curr_date)
	#----------------------------------------------------------------------------------------------------------------------
		

	#---------------------------------datetime for cheking when to update stock history ----------------------------------	
	c_datetime_hist_func = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
    	c_datetime_hist_func = datetime.strptime(c_datetime_hist_func, '%y-%m-%d %H:%M:%S') # converting to datetime format	
	timeInerval_hist_func = c_datetime_hist_func + timedelta(minutes= 10) # upadte STOCK_HISTORY in every 10 minutes
	#---------------------------------------------------------------------------------------------------------------------

	#--------------------------------datetime to update history of all trades after this time-----------------------------
	update_hist_time = datetime.now().strftime('%y-%m-%d %H:%M:%S')
	update_hist_time = datetime.strptime(update_hist_time, '%y-%m-%d %H:%M:%S')
	#---------------------------------------------------------------------------------------------------------------------	

	k = 1
	# keep running whole process inifinty times inside while loop
	while True:
		# calling makewave
		cursor = con.cursor()
		cursor.execute("call make_wave()")	
		cursor.close()
		
# ----------------------------------------------------------------------------------------------------------------------------------
# getting all necessary data of field from makewave and process it to send check bid insert procedure to inser in stock quote 
		cursor = con.cursor()
		cursor.execute("select * from STOCK_QUOTE_WAVE")	
		result = cursor.fetchall()	
		cursor.close()
		
		res =[]
		for row in result:
			res.append(row)

			
		for row in res:
		#	random.seed() 
			i = 0
			id = row[0]
			seq = row[2]
			symb = row[3]
			a_p = row[5]# +Decimal(str(random.uniform(-2,2))) you can comment of to  use to test of  fluctuate more
			a_s = row[6]
			b_p = row[7] #+Decimal(str(random.uniform(-2,2)))
			b_s = row[8]
			curr_datetime = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
			curr_datetime = datetime.strptime(curr_datetime, '%y-%m-%d %H:%M:%S') # converting to datetime format
			curr_date = datetime.now().date() # current date only
			i = i+1 # icrementing i 

			cursor = con.cursor()
			# check if it is a bid then call procedure
			if b_s > 0:
				arg= [id,curr_date, seq, symb, curr_datetime, a_p,a_s, b_p, b_s]
				cursor.callproc("check_bid_insert",arg)
			else:
				cursor.execute("INSERT INTO STOCK_QUOTE (INSTRUMENT_ID, QUOTE_DATE, QUOTE_SEQ_NBR, TRADING_SYMBOL, QUOTE_TIME, ASK_PRICE, ASK_SIZE, BID_PRICE, BID_SIZE) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", (id,curr_date, seq, symb, curr_datetime, a_p,a_s, b_p, b_s))
					
			cursor.close()
#--------------------------------------------------------------------------------------------------------------------------------------

		# deleting all the quotes that has sold indicated by -1
		clean_up(con)
		
		# clean up according to time interval
		clean_up_with_time(con)

		# calling update stock history which is located on history_update.py
		if c_datetime_hist_func > timeInerval_hist_func:
			print("stock history updating")
			update_hist_time = update_history(con, update_hist_time) # calling function to update stock history	
			timeInerval_hist_func = c_datetime_hist_func + timedelta(minutes = 10) #upadte STOCK_HISTORY in every 10 minutes
		c_datetime_hist_func = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
		c_datetime_hist_func = datetime.strptime(c_datetime_hist_func, '%y-%m-%d %H:%M:%S') # converting to datetime format

		
		print(k, " iteration is done")
		k = k + 1
		
#+++++++++++++++++++++++++++++++++++++++End of Machine Engine function ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++









# return QUOTE_SEQ_NBR
def getSeqNbr(con, trade_symb):
	cursor = con.cursor()
	cursor.execute("select count(*) from STOCK_QUOTE where TRADING_SYMBOL = %s;",trade_symb)
	result = cursor.fetchone()
	# getting current se_nbr 	
	seq_nbr = result[0] + 1 # adding 1 for new insert number
	cursor.close()
	return seq_nbr 
	

# insert function
def insert_fun(con,instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size, seq_nbr, curr_datetime, curr_date):

	cursor = con.cursor()
	cursor.execute("INSERT INTO STOCK_QUOTE (INSTRUMENT_ID, QUOTE_DATE, QUOTE_SEQ_NBR, TRADING_SYMBOL, QUOTE_TIME, ASK_PRICE, ASK_SIZE, BID_PRICE, BID_SIZE) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", (instr_id,curr_date, seq_nbr, trade_symb, curr_datetime, ask_price, ask_size, bid_price, bid_size))
	cursor.close()
	
	 	
#clean up function delete all quote that has been sold
def clean_up(con):
	print "sold quotes are deleting"
	cursor = con.cursor()
	cursor.execute("DELETE from STOCK_QUOTE WHERE (ASK_SIZE = -1 ) OR (BID_SIZE = -1)")
	cursor.close()


# clean up with certain time period i.e. delete all whose time difference is more than 10
def clean_up_with_time(con):
	print "Delete all quotes that are living on poll more than 5 minutes"
	curr_datetime = (datetime.now()-timedelta(minutes=5)).strftime('%y-%m-%d %H:%M:%S') # string of current datetime
        curr_datetime = datetime.strptime(curr_datetime, '%y-%m-%d %H:%M:%S') # converting to datetime format
	
	cursor = con.cursor()
        cursor.execute("DELETE from STOCK_QUOTE WHERE QUOTE_TIME < %s",(curr_datetime))
        cursor.close()

	
		




