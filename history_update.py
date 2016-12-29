from datetime import datetime, timedelta # importing for current  date and time

def update_history(con):
	count = 0
	cursor = con.cursor()
        cursor.execute("SELECT INSTRUMENT_ID from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP by TRADING_SYMBOL")
        resultCount = result = cursor.fetchone()
	count = resultCount[0]
        cursor.close()
	
	
	listId = []
	listSymb = []
	listOp_price = []
	listCl_price = []
	listLow = []
	listHigh = []
	listVol = []
	
	# -------------------------- getting instrument id ---------------------------------
	cursor = con.cursor()
        cursor.execute("SELECT INSTRUMENT_ID from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP by TRADING_SYMBOL")
        resultId = cursor.fetchall()
       	 
	for id in resultId:	
		listId.append(id[0])
	
	
        cursor.close()	

	# ----------------------------------------------------------------------------------

	 # -------------------------- getting trade symbol ---------------------------------
	cursor = con.cursor()
        cursor.execute("SELECT TRADING_SYMBOL from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP by TRADING_SYMBOL")
        resultSymb = cursor.fetchall()
       
        for symb in resultSymb:
                listSymb.append(symb[0]) 
               
        
        cursor.close()


        # ----------------------------------------------------------------------------------


	 # -------------------------- getting Open price ---------------------------------
	cursor = con.cursor()
        cursor.execute("SELECT TRADE_PRICE, MIN(TRADE_TIME) from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP by TRADING_SYMBOL")
        resultOp_price = cursor.fetchall()
       
        for Op_price in resultOp_price:
                listOp_price.append(Op_price[0])
		
                
       
        cursor.close()

        # ----------------------------------------------------------------------------------


	 # -------------------------- getting Close Price ---------------------------------
	cursor = con.cursor()
        cursor.execute("SELECT TRADE_PRICE, MAX(TRADE_TIME) from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP by TRADING_SYMBOL")
        resultCl_price = cursor.fetchall()
       
        for Cl_price in resultCl_price:
                listCl_price.append(Cl_price[0])
               
               

        cursor.close()
	
        # ----------------------------------------------------------------------------------


	 # -------------------------- getting Low Price ---------------------------------
	cursor = con.cursor()
        cursor.execute("SELECT MIN(TRADE_PRICE) from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP by TRADING_SYMBOL")
        resultLow = cursor.fetchall()
       
        for low in resultLow:
                listLow.append(low[0])
                
               

        cursor.close()

        # ----------------------------------------------------------------------------------


	 # -------------------------- getting High price---------------------------------
	cursor = con.cursor()
        cursor.execute("SELECT MAX(TRADE_PRICE) from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP by TRADING_SYMBOL")
        resultHigh = cursor.fetchall()
        
        for high in resultHigh:
                listHigh.append(high[0])


        cursor.close()

        # ----------------------------------------------------------------------------------



	 # -------------------------- getting volume ---------------------------------
	cursor = con.cursor()
        cursor.execute("select sum(TRADE_SIZE) from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP BY TRADING_SYMBOL")
        resultVol = cursor.fetchall()
        
        for vol in resultVol:
                listVol.append(vol[0])
       

        cursor.close()

        # ----------------------------------------------------------------------------------



	# ******************************************Insert on stock History ******************************************************************
	curr_date = datetime.now().date() # current date only
	i = 0
	while i < len(listId):
		cursor = con.cursor()
		cursor.execute("INSERT INTO STOCK_HISTORY (INSTRUMENT_ID, TRADE_DATE,  TRADING_SYMBOL, OPEN_PRICE, CLOSE_PRICE, LOW_PRICE, HIGH_PRICE, VOLUME) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)", (listId[i],curr_date, listSymb[i], listOp_price[i], listCl_price[i], listLow[i], listHigh[i], listVol[i]))
		i = i+1
		cursor.close()
