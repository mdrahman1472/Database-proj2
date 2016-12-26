import MySQLdb

# Connect to the database
con = MySQLdb.connect(host='134.74.126.107',
                             user='F16336mrahman',
                             passwd='23148232',
                             db='F16336mrahman',
                     
)

c = con.cursor()
c.execute("Select * from sales")

rows = c.fetchall()

for eachRow in rows:
	print eachRow

