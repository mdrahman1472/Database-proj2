def test(con):
	listCl_price = []

	listAllSymb = [] # list for all trading symbol
	
	# +++++++++++++++++++++++getting all symbols by quiry ++++++++++++++++++++++
	
	cursor = con.cursor()
        cursor.execute("SELECT DISTINCT TRADING_SYMBOL FROM STOCK_TRADE ORDER BY TRADING_SYMBOL ASC")
        resultAllSymb = cursor.fetchall()

        for s in resultAllSymb:
                listAllSymb.append(s[0])
	
        cursor.close()

	#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 
	cursor = con.cursor()
        cursor.execute("SELECT a.`TRADING_SYMBOL`, a.`TRADE_PRICE`, a.`TRADE_TIME` from STOCK_TRADE a, (SELECT TRADING_SYMBOL, MAX(TRADE_TIME) as max_time from STOCK_TRADE GROUP BY TRADING_SYMBOL) b where a.TRADING_SYMBOL = b.`TRADING_SYMBOL` AND a.TRADE_TIME = b.max_time ORDER BY `a`.`TRADING_SYMBOL` ASC ")
        
	resultCl_price = cursor.fetchall()

	i = 0
        for Cl_price in resultCl_price:
        	if (Cl_price[0] == listAllSymb[i]):        
			listCl_price.append(Cl_price[1])
			i = i + 1
	
	print(len(listCl_price),"	" , len(listAllSymb))			
        cursor.close()
'''	j=0
	while j < len(listCl_price):
		print(listAllSymb[j] + "  |  " + listCl_price[j])
		j = j+1 
'''
