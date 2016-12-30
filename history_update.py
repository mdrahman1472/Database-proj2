from datetime import datetime, timedelta # importing for current  date and time

def update_history(con):
	
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
        cursor.execute("select top.TRADE_PRICE, top.TRADING_SYMBOL, top.TRADE_SEQ_NBR,top.TRADE_TIME from STOCK_TRADE top, ( SELECT min(a.TRADE_SEQ_NBR) as tm, a.TRADING_SYMBOL as ts, a.TRADE_PRICE as tp, a.TRADE_TIME as tt, a.TRADE_DATE as td from STOCK_TRADE a, ( SELECT TRADE_SEQ_NBR, TRADING_SYMBOL, min(TRADE_TIME) as min_time from STOCK_TRADE WHERE `TRADE_DATE`= CURDATE() GROUP BY TRADING_SYMBOL ) b where a.TRADING_SYMBOL = b.`TRADING_SYMBOL` AND a.TRADE_TIME = b.min_time group BY `a`.`TRADING_SYMBOL` ASC ) bottom where top.TRADING_SYMBOL = bottom.ts and top.TRADE_SEQ_NBR = bottom.tm and top.TRADE_DATE = bottom.td ORDER BY `top`.`TRADING_SYMBOL` ASC")
        resultOp_price = cursor.fetchall()
       
        for Op_price in resultOp_price:
                listOp_price.append(Op_price[0])
		
                
       
        cursor.close()

        # ----------------------------------------------------------------------------------


	 # -------------------------- getting Close Price ---------------------------------

	cursor = con.cursor()

	# sorry for sloppy look of it got bigger since I have get accending order of Trade symbol and there is multiple trade on 
	# same time. I tried to arrange in string to look nice but for some reason in vim editor on lab gave me error of indentatio
	# but ok on my home computer
        cursor.execute("select top.TRADE_PRICE, top.TRADING_SYMBOL, top.TRADE_SEQ_NBR,top.TRADE_TIME from STOCK_TRADE top, ( SELECT max(a.TRADE_SEQ_NBR) as tm, a.TRADING_SYMBOL as ts, a.TRADE_PRICE as tp, a.TRADE_TIME as tt, a.TRADE_DATE as td from STOCK_TRADE a, ( SELECT TRADE_SEQ_NBR, TRADING_SYMBOL, max(TRADE_TIME) as max_time from STOCK_TRADE where TRADE_DATE = CURDATE() GROUP BY TRADING_SYMBOL ) b where a.TRADING_SYMBOL = b.`TRADING_SYMBOL` AND a.TRADE_TIME = b.max_time group BY `a`.`TRADING_SYMBOL` ASC ) bottom where top.TRADING_SYMBOL = bottom.ts and top.TRADE_SEQ_NBR = bottom.tm and top.TRADE_DATE = bottom.td ORDER BY `top`.`TRADING_SYMBOL` ASC")
        
        resultCl_price = cursor.fetchall()

        for Cl_price in resultCl_price:                    
                listCl_price.append(Cl_price[0])

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
