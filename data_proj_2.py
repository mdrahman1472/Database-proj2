from datetime import datetime # importing for current  date and time
import time
import MySQLdb
import MySQLdb.cursors

def matchingEngine(con, cursor,instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size):
	
	curr_datetime = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
	curr_datetime = datetime.strptime(curr_datetime, '%y-%m-%d %H:%M:%S') # converting to datetime format
	curr_date = datetime.now().date() # current date only
	
	#seq_nbr = getSeqNbr(con, cursor, trade_symb) # genarating QUOTE_SEQ_NBR
	# insert quote
	#insert_fun (con, cursor, instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size, seq_nbr, curr_datetime, curr_date)
	


	# calling makewave
	cursor.execute("call make_wave()")	
	cursor.close()
	
	cursor = con.cursor()
	cursor.execute("select * from STOCK_QUOTE_WAVE")	
	result = cursor.fetchall()	
	cursor.close()
	
	
	for row in result:
		i = 0
		id = row[0]
		seq = row[2]
		symb = row[3]
		a_p = row[5]
		a_s = row[6]
		b_p = row[7]
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


	# deleting all the quotes that has sold indicated by -1
	clean_up(con)
	
	# clean up according to time interval
	clean_up_with_time(con)




# return QUOTE_SEQ_NBR
def getSeqNbr(con, cursor, trade_symb):
	cursor.execute("select count(*) from STOCK_QUOTE where TRADING_SYMBOL = %s;",trade_symb)
	result = cursor.fetchone()
	# getting current se_nbr 	
	seq_nbr = result[0] + 1 # adding 1 for new insert number
	return seq_nbr 
	

# insert function
def insert_fun(con, cursor, instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size, seq_nbr, curr_datetime, curr_date):
	cursor.execute("INSERT INTO STOCK_QUOTE (INSTRUMENT_ID, QUOTE_DATE, QUOTE_SEQ_NBR, TRADING_SYMBOL, QUOTE_TIME, ASK_PRICE, ASK_SIZE, BID_PRICE, BID_SIZE) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", (instr_id,curr_date, seq_nbr, trade_symb, curr_datetime, ask_price, ask_size, bid_price, bid_size))
	
	
	 	
#clean up function
def clean_up(con):
	cursor = con.cursor()
	cursor.execute("DELETE from STOCK_QUOTE WHERE (ASK_SIZE = -1 ) OR (BID_SIZE = -1)")
	cursor.close()


# clean up with certain time period i.e. delete all whose time difference is more than 10
def clean_up_with_time(con):
	curr_datetime = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
        curr_datetime = datetime.strptime(curr_datetime, '%y-%m-%d %H:%M:%S') # converting to datetime format
	
	cursor = con.cursor()
        cursor.execute("DELETE from STOCK_QUOTE WHERE QUOTE_TIME > %s -INTERVAL 1 MINUTE",(curr_datetime))
        cursor.close()

	
		




