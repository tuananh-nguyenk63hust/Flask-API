import flask
from flask import json, jsonify,request
import mysql.connector as Connection
import calendar
import time
import io
import csv
import os
from datetime import datetime
from flask_restful import Api
from Module import DATA
app=flask.Flask(__name__)
api=Api(app)
app.config['DEBUG']=True
#conn=Connection.connect(user='sparclab',password='SPARCLab1',host='localhost',db='Airsense')
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin','*')
    response.headers.add('Access-Control-Allow-Headers','Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods','GET,PUT,POST,DELETE,OPTIONS')
    return response
#------------------------------------------------------------------------------------------------------#
@app.route('/',methods=['GET'])
def home():
    return "Welcome to SPARCLab Airsense Server"
#------------------------------------------------------------------------------------------------------#
def ConnectionSQL():
   Conn=Connection.connect(user='admin_python',password='LabSPARC1',host='localhost',db='admin_python')
    #Conn=Connection.connect(user='sparclab',password='SPARCLab1',host='localhost',db='Airsense')
   return Conn
#------------------------------------------------------------------------------------------------------#
@app.route('/airsense/extended',methods=['GET'])
def Extended():
    conn=ConnectionSQL()
    curExte=conn.cursor()
    #return "ok"
    queryExe="SELECT NodeId,Time,CO,CO2,SO2,NO2,O3 FROM ExtendedData"

    curExte.execute(queryExe)
    valExten=curExte.fetchall()
    Extenlist=[]
    #return "{}".format(valExten)
    #return "ok"
    for row in valExten:
        #return "ok"
        airsensedata={

                "NodeId":row[0],
                "Time":row[1],
                "CO":row[2],
                "CO2":row[3],
                "SO2":row[4],
                "NO2":row[5],
                "O3":row[6]

        }
        Extenlist.append(airsensedata)
    #return jsonify(Extenlist)
    #JsonData=json.dumps(Extenlist)
    conn.close()
    if 'NodeId' in request.args:
        #return "ok"
        NodeId=int(request.args['NodeId'],16)
    else:
        return jsonify(Extenlist)
    result=[]
    for dataextend in Extenlist:
        if dataextend['NodeId']==NodeId:
            result.append(dataextend)
    return jsonify(result)
#-----------------------------------------------------------------------------------------------------#
@app.route('/airsense/data', methods=['GET'])
def data():
#    return "OK"
    conn=ConnectionSQL()
    curdata=conn.cursor()
    querydata="SELECT NodeId,Time,PM2p5,PM10,PM1,Temperature,Humidity FROM Data"
    dataall=[]
#           return "ok"
    curdata.execute(querydata)
    #return "{}".format(val)
    val=curdata.fetchall()
    for row in val:
#        return "ok"
        airsensedata={

                "Time":row[1],
                "PM2.5":row[2],
                "PM10":row[3],
                "PM1":row[4],
                "Tem":row[5],
                "Hum":row[6],
                "NodeId":row[0]

        }
        dataall.append(airsensedata)
    conn.close()
#    return "okok"
    if 'NodeId'in request.args:
        NodeId=int(request.args['NodeId'],16)
    else:
        return jsonify(dataall)
    #return type(request.arg['NodeId'])
    result=[]
    for intdata in dataall:
        if intdata['NodeId']==NodeId:
            result.append(intdata)
    return jsonify(result)
#-----------------------------------------------------------------------------------------------------#
@app.route('/airsense/datapos',methods=['GET'])
def pos():
    conn=ConnectionSQL()
    curdata=conn.cursor()
    querydata="SELECT * FROM Data WHERE Time BETWEEN %s AND %s"
    timenow=calendar.timegm(time.gmtime())
    curdata.execute(querydata,(timenow-180,timenow))
    val=curdata.fetchall()
    PosList=[]
    #return "ok"
    for i in range(len(val)):
          curdata=conn.cursor()
          #return "ok"   
          querydata2="SELECT * FROM Location WHERE NodeId BETWEEN %s AND %s"
          NodeId=val[-i-1][0]
          curdata.execute(querydata2,(NodeId,NodeId))
          val2=curdata.fetchall()
          if len(val2)!=0:
            airsense={
              "NodeId":val[-i-1][0],
              "Time":val[-i-1][1],
              "PM2.5": val[-i-1][2],
              "PM1": val[-i-1][3],
              "PM10":val[-i-1][4],
              "Tem":val[-i-1][5],
              "Hum":val[-i-1][6],
              "Latitude":float(val2[0][1]),
              "Longtitude":float(val2[0][2]),
              "Altitude":val2[0][3],
              "Position":val2[0][8]
              }
            PosList.append(airsense)
    conn.close()
    if 'NodeId' in request.args:
        NodeId=int(request.args['NodeId'],16)
    else:
        return jsonify(PosList)
    result=[]
    for datapost in PosList:
        if datapost['NodeId']==NodeId:
            result.append(datapost)
    return jsonify(result)
#----------------------------------------------------------------------------------------------------#
@app.route('/airsense/getcsv',methods=['GET'])
def getcsv():
    conn=ConnectionSQL()
    datacur=conn.cursor()
    fromtime=int(flask.request.args["fromtime"])
    #strS=""
    #return 'ok'
    #for i in range(0,len(command)-1):
    #    if (command[i]>='0') and (command[i]<="9"):
    #        strS+=command[i]
    #    if command[i]=='T':
    #        fromtime=int(strS)
    #        strS=""
    #    if command[i]=='N':
    #        endtime=int(strS)
    #        strS=""
    #    if i==len(command)-1:
    #        NodeIdHex=strS
    #        NodeId=int(strS,16)
    endtime=int(flask.request.args["endtime"])
    NodeId=int(flask.request.args["NodeId"],16)
    NodeIdHex=flask.request.args["NodeId"]
    query="SELECT * FROM Data WHERE Time BETWEEN %s AND %s"
    datacur.execute(query,(fromtime,endtime))
    #return str(NodeId)
    val=datacur.fetchall()
    #return "okok"
    datacsv=[["NodeId","Time","PM2p5","PM10","PM1","Tem","Hum"]]
    #return "ok1"
    dataex=[]
    os.environ['TZ']='Asia/Ho_Chi_Minh'
    for row in val:
        #timesta=float(row[1])
        if row[0]==NodeId:
            timesta=float(row[1])
            TimeUni=datetime.fromtimestamp(timesta)
            #return "Done!"
            datarow=[NodeIdHex,TimeUni,row[2],row[3],row[4],row[5],row[6]]
            datacsv.append(datarow)
    #return "ok1"
    #return flask.jsonify(dataex)
    si=io.StringIO()
    cw=csv.writer(si)
    cw.writerows(datacsv)
    output=flask.make_response(si.getvalue())
    namefile=NodeIdHex+".csv"
    querycommand="attachment; filename="+namefile
    output.headers["Content-Disposition"]=querycommand
    output.headers["Content-type"]="text/csv"
    conn.close()
    return output
#---------------------------------------------------------------------------#
@app.route('/airsense/devices',methods=['GET'])
def getdevices():
    conn=ConnectionSQL()
    datacur=conn.cursor()
    querydata="SELECT NodeId, Id, Latitude, Longtitude, Altitude, ReverseGeocode FROM Location"
    DataLocation=[]
    datacur.execute(querydata)
    val=datacur.fetchall()
    for row in val:
        content={
            "NodeId":DATA().convertdectohex(int(row[0])),
            "Title":row[1],
            "Latitude":row[2],
            "Longtitude":row[3],
            "Altitude":row[4],
            "ReverseGeocode":row[5]
        } 
        DataLocation.append(content)
    conn.close()
    return jsonify(DataLocation)
#---------------------------------------------------------------------------#
@app.route('/airsense/AQI',methods=['GET'])
def getAQI():
    conn=ConnectionSQL()
    datacur=conn.cursor()
    querydata="SELECT * FROM AQI1Hour"
    DataAQI=[]
    datacur.execute(querydata)
    val=datacur.fetchall()
    for row in val:
        content={
            "NodeId":DATA().convertdectohex(int(row[0])),
            "Time":row[1],
            "AQI":row[2]
        }
        DataAQI.append(content)
    conn.close()
    if 'NodeId' in request.args:
        NodeId=request.args['NodeId']
    else:
        return jsonify(DataAQI)
    DataNodeId=[]
    for ContentAQI in DataAQI:
        if ContentAQI['NodeId']==NodeId:
            DataNodeId.append(ContentAQI)
    return jsonify(DataNodeId)
#---------------------------------------------------------------------------#
@app.route('/airsense/Data7day',methods=['GET'])
def getdata7day():
    ToTime=int(time.time())
    if 'Time' in request.args:
        Timee=int(request.args['Time'])
    else:
        Timee=4
    Result=DATA().get_data(ToTime,Timee)
    DATA().closeSQL()
    if 'NodeId' in request.args:
        NodeId=request.args['NodeId']
    else:
        return jsonify(Result)
    Resultde=[]
    for value in Result:
        if value['NodeId']==NodeId:
            Resultde.append(value)
    return jsonify(Resultde) 

#---------------------------------------------------------------------------#
@app.route('/airsense/DataNow',methods=['GET'])
def NowData():
    TimeNow=time.time()
    Result=DATA().DataNow(TimeNow)
    DATA().closeSQL()
    if 'NodeId' in request.args:
        NodeId=request.args['NodeId']
    else:
        return jsonify(Result)
    Resultnode=[]
    for value in Result:
        if value['NodeId']==NodeId:
            Resultnode.append(value)
    return jsonify(Resultnode)
#--------------------------------------------------------------------------#
if __name__=='__main__':
    app.run(host='0.0.0.0',port=4000)


