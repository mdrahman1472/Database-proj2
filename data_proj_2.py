from datetime import datetime # importing for current  date and time
import time

def matchingEngine(cursor,instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size):
	curr_datetime = datetime.now().strftime('%y-%m-%d %H:%M:%S') # string of current datetime
	curr_datetime = datetime.strptime(curr_datetime, '%y-%m-%d %H:%M:%S') # converting to datetime format
	curr_date = datetime.now().date() # current date only
	
	seq_nbr = getSeqNbr(cursor, trade_symb) # genarating QUOTE_SEQ_NBR
	# inseting quote
	insert_fun (cursor, instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size, seq_nbr, curr_datetime, curr_date)
	
	time.sleep(60)
	if bid_price > 0 and bid_size > 0:
		print "just test"

	# Delete quote when inserted quote is not for bid  
	else:
		cursor.execute("DELETE FROM STOCK_QUOTE where INSTRUMENT_ID = %s AND QUOTE_TIME = %s AND BID_PRICE = 0", (instr_id, curr_datetime))
		print("Inserted Quote has been deleted because bid price and size are 0")






# return QUOTE_SEQ_NBR
def getSeqNbr(cursor, trade_symb):
	cursor.execute("select count(*) from STOCK_QUOTE where TRADING_SYMBOL = %s;",trade_symb)
	result = cursor.fetchone()
	# getting current se_nbr 	
	seq_nbr = result[0] + 1 # adding 1 for new insert number
	return seq_nbr 
	

# insert function
def insert_fun(cursor, instr_id, trade_symb, ask_price, ask_size, bid_price, bid_size, seq_nbr, curr_datetime, curr_date):
	cursor.execute("INSERT INTO STOCK_QUOTE (INSTRUMENT_ID, QUOTE_DATE, QUOTE_SEQ_NBR, TRADING_SYMBOL, QUOTE_TIME, ASK_PRICE, ASK_SIZE, BID_PRICE, BID_SIZE) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", (instr_id,curr_date, seq_nbr, trade_symb, curr_datetime, ask_price, ask_size, bid_price, bid_size))
	
	
	 	
