from datetime import datetime, timedelta

def test(con,c_t):
	t=fun2(con, c_t)
	return t


def fun2(con, c_t):
	print c_t
	c = datetime.now().date() + timedelta(days = -2) 
	listCl_price = []
'''
	cursor = con.cursor()

# sorry for sloopy look of it got bigger since I have get accending order of Trade symbol and there is multiple trade on 
# same time. I tried to arrange in string to look nice but for some reason in vim editor on lab gave me error of indentatio
# but ok on my home machine
        cursor.execute("select top.TRADE_PRICE, top.TRADING_SYMBOL, top.TRADE_SEQ_NBR,top.TRADE_TIME from STOCK_TRADE top, ( SELECT max(a.TRADE_SEQ_NBR) as tm, a.TRADING_SYMBOL as ts, a.TRADE_PRICE as tp, a.TRADE_TIME as tt, a.TRADE_DATE as td from STOCK_TRADE a, ( SELECT TRADE_SEQ_NBR, TRADING_SYMBOL, max(TRADE_TIME) as max_time from STOCK_TRADE where TRADE_DATE = %s GROUP BY TRADING_SYMBOL ) b where a.TRADING_SYMBOL = b.`TRADING_SYMBOL` AND a.TRADE_TIME = b.max_time group BY `a`.`TRADING_SYMBOL` ASC ) bottom where top.TRADING_SYMBOL = bottom.ts and top.TRADE_SEQ_NBR = bottom.tm and top.TRADE_DATE = bottom.td ORDER BY `top`.`TRADING_SYMBOL` ASC", (c))
        
	resultCl_price = cursor.fetchall()

	
        for Cl_price in resultCl_price:        
		listCl_price.append(Cl_price[0])
		print Cl_price
'''
	while True:	
		print "printing c_t: ", c_t
	
		return datetime.now().date() + timedelta(days = -1)
