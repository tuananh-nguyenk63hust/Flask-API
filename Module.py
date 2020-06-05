import mysql.connector as Connection 
class DATA:
    def __init__(self):
        self.conn=Connection.connect(user='admin_python',password='LabSPARC1',host='localhost',db='admin_python')
        #self.conn=Connection.connect(user='sparclab',password='SPARCLab1',host='localhost',db='Airsense')
    def closeSQL(self):
        self.conn.close()
    TIME_DAY=3600*24
    TIME_WEEK=TIME_DAY*7
    
    def average(self,NodeId,FromTime,ToTime):
        query="SELECT * FROM Data WHERE NodeId=%s AND (Time BETWEEN %s AND %s)" 
        setcursor=self.conn.cursor()
        setcursor.execute(query,(NodeId,FromTime,ToTime))
        val=setcursor.fetchall()
        INT_VALUES=0
        AVERAGE_VALUE=[0,0,0,0,0]
        for row in val:
            AVERAGE_VALUE[0]=(AVERAGE_VALUE[0]*INT_VALUES+float(row[2]))/(INT_VALUES+1)
            AVERAGE_VALUE[1]=(AVERAGE_VALUE[1]*INT_VALUES+float(row[3]))/(INT_VALUES+1)
            AVERAGE_VALUE[2]=(AVERAGE_VALUE[2]*INT_VALUES+float(row[4]))/(INT_VALUES+1)
            AVERAGE_VALUE[3]=(AVERAGE_VALUE[3]*INT_VALUES+float(row[5]))/(INT_VALUES+1)
            AVERAGE_VALUE[4]=(AVERAGE_VALUE[4]*INT_VALUES+float(row[6]))/(INT_VALUES+1)
            INT_VALUES+=1
        value={
            'NodeId':self.convertdectohex(int(NodeId)),
            'Time':ToTime,
            'PM2.5':AVERAGE_VALUE[0],
            'PM10':AVERAGE_VALUE[1],
            'PM1':AVERAGE_VALUE[2],
            'Tem':AVERAGE_VALUE[3],
            'Hum':AVERAGE_VALUE[4]
        }
        del INT_VALUES
        del AVERAGE_VALUE
        setcursor.close()
       	# self.conn.close()
        return value
    def getNodeId(self,FromTime,ToTime):
        query="SELECT NodeId FROM Data WHERE Time BETWEEN %s AND %s GROUP BY NodeId"
        setcursor=self.conn.cursor()
        setcursor.execute(query,(FromTime,ToTime))
        val=setcursor.fetchall()
        NodeList=[]
        for Id in val:
            NodeList.append(Id[0])
        setcursor.close()
        #self.conn.close()
        return NodeList
    def get_data(self,ToTime,Timee): #Timee: hours
        FromTime=ToTime-self.TIME_WEEK
        Result=[]
        Timee=3600*Timee
        #return 'ok'
        while(ToTime-Timee>FromTime):
            NodeList=self.getNodeId(ToTime-Timee,ToTime)
            for NodeId in NodeList:
                value=self.average(NodeId,ToTime-Timee,ToTime)
                Result.append(value)
            ToTime=ToTime-Timee
        return Result
    def convertdectohex(self,s):
        s1=str(hex(s))
        res=""
        for i in s1:
            if (i!='0') and (i!='x'):
                res+=i
        #res1=res.upper()
        return(res.upper())
    def DataNow(self,TimeNow):
        FromTime=TimeNow-210
        query="SELECT * FROM Data WHERE Time BETWEEN %s AND %s"
        setcursor=self.conn.cursor()
        setcursor.execute(query,(FromTime,TimeNow))
        NodeList=setcursor.fetchall()
        Result=[]
        for val in NodeList:
            jsonfile={
                "NodeId": self.convertdectohex(val[0]),
                "Time": val[1],
                "Pm2.5":val[2],
                "Pm10":val[3],
                "Pm1":val[4],
                "Tem":val[5],
                "Hum":val[6]
            }
            Result.append(jsonfile)
        return Result
    
    
    



            

