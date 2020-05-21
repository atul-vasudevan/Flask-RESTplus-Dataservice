from flask import Flask, request
from flask_restplus import Resource, Api, fields, inputs, reqparse
import requests
import pandas as pd
import sqlite3
from sqlite3 import Error
import json
app = Flask(__name__)
app.config.SWAGGER_UI_DOC_EXPANSION = 'list'
api = Api(
  app,
  default="Worldbank",
  title='Flask-Restplus Data Service',
  description='Develop a Flask-Restplus data service that allows a client to ' \
  'read and store some publicly available economic indicator ' \
  'data for countries around the world, and allow the consumers ' \
  'to access the data through a REST API.'
)
api_model = api.model('Bank', {'indicator_id': fields.String,})

def url(indicator_id):
    return f'http://api.worldbank.org/v2/countries/all/indicators/{indicator_id}?date=2012:2017&format=json&per_page=1000'

def databaseConnection(indicator_id,indicator_value,entries):
    conn=sqlite3.connect('test.db')
    c=conn.cursor()
    dataTuple=(indicator_id,indicator_value,entries)
    query="""CREATE TABLE IF NOT EXISTS collections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id text,
    indicator_value text,
    creation_time datetime default current_timestamp,
    entries text
    );"""
    c.execute(query)
    c.execute('INSERT INTO collections(indicator_id,indicator_value,entries) VALUES(?,?,?);',dataTuple)
    c.execute('select * from collections')
    row=c.fetchone()
    conn.commit()
    conn.close()

@api.route('/collections', methods=['GET', 'POST'])
class Collection(Resource):
    @api.doc(description="Import a collection from the data service")
    @api.response(201, 'Successfully Created')
    @api.response(200, 'Successfully Retrived')
    @api.response(404, 'Error')
    def post(self):
        global i_id
        i_id=request.args.get('indicator_id')
        if not i_id:
            return {
                "Error":"Indicator ID not found"},404 
        response=requests.get(url(i_id))
        data=response.json()
        df = pd.DataFrame(data[1])
        df.dropna()
        df["country"]=df["country"].apply(lambda row: row["value"])
        indicator_id = df.iloc[0].indicator["id"]
        indicator_value = df.iloc[0].indicator["value"]
        df= df.drop(['indicator','countryiso3code','unit','obs_status','decimal'],axis=1)
        entries=df.to_json(orient='records')
        databaseConnection(indicator_id,indicator_value,entries)
        conn = sqlite3.connect("test.db")
        c = conn.cursor()
        c.execute("SELECT id,creation_time,indicator_id FROM collections WHERE indicator_id=?",(i_id,))
        rows = c.fetchone()
        conn.close()
        id, creation_time, indicator_id=rows
        return{
            "uri":f"/collections/{id}",
            "id":id,
            "creation_time":creation_time,
            "indicator":indicator_id
        }, 200

    @api.doc(description="Retrieve the list of available collections ")
    @api.response(200, 'Ok')
    def get(self):
        conn = sqlite3.connect("test.db")
        c = conn.cursor()
        c.execute("SELECT id,creation_time,indicator_id FROM collections")
        rows = c.fetchall()
        creationList=[]
        creationListDict={}
        for i in range(len(rows)):
            creationListDict["uri"]=f"/collections/{rows[i][0]}"
            creationListDict["id"]=rows[i][0]
            creationListDict["creation_time"]=rows[i][1]
            creationListDict["indicator"]=rows[i][2]
            creationList.append(creationListDict)
        order_by=request.args.get('order_by')
        sign_list=[]
        order_by_list=order_by.replace('{','').replace('}','').split(',')
        for e in range(len(order_by_list)):
            if '-' in order_by_list[e]:
                sign_list.append('-')
                order_by_list[e] = order_by_list[e].replace('-','').lstrip()
            else:
                sign_list.append('+')
                order_by_list[e]=order_by_list[e].strip()
        df = pd.DataFrame(creationList)
        df.sort_values(by=order_by_list,ascending=sign_list)
        creationList_dict=df.to_json(orient='records')
        conn.close()
        return json.loads(creationList_dict),200
        
@api.route('/collections/<cid>',methods=['DELETE','GET'])   
@api.param('cid','Collection ID')
class CollectionByID(Resource):         
    @api.response(200, 'OK')
    @api.response(404, 'Resource not Found')
    @api.doc(description="Deleting a collection with the data service")
    def delete(self,cid):
        conn=sqlite3.connect('test.db')
        c=conn.cursor()
        c.execute('select count(*) from collections where id=?',(cid,))
        resultSelect=c.fetchone()
        if resultSelect[0] >0:
            c.execute('Delete from collections where id=?',(cid,))
            conn.commit()
            return{
                "message" :"The collection {} was removed from the database!".format(cid,),
                "id": cid
            },200
        else:
            return {
                "message":"Resource not Found"
            },404

    @api.doc(description="Retrieve a collection")
    @api.response(200, 'Ok')
    def get(self,cid):
        conn = sqlite3.connect("test.db")
        c = conn.cursor()
        c.execute("SELECT * FROM collections where id=?",(cid,))
        rows = c.fetchone()
        collection_id, indicator_id, indicator_value, creation_time, entries=rows
        conn.close()
        return{
            "id":collection_id,
            "indicator_id":indicator_id,
            "indicator_value":indicator_value,
            "creation_time":creation_time,
            "entries":json.loads(entries)
        }, 200

@api.route('/collections/<cid>/<year>/<country>',methods=['GET'])   
@api.param('cid','Collection ID')
@api.param('year','Year')
@api.param('country','Country')
class CollectionByYear(Resource):
    @api.doc(description="Retrieve economic indicator value for given country and a year")
    @api.response(200, 'Ok')
    @api.response(500, 'Invalid parameter')
    def get(self,cid,year,country):
        conn = sqlite3.connect("test.db")
        c = conn.cursor()
        c.execute("SELECT indicator_id, entries FROM collections where id=?",(cid,))
        rows = c.fetchone()
        indicator_id, entries=rows
        data=json.loads(entries)
        for i in data:
            if i['country'] == country and i['date'] == year:
                ind_value=i['value']
        conn.close()
        return{
            "id":cid,
            "indicator":indicator_id,
            "country":country,
            "year":year,
            "value":ind_value
        }, 200

def handleNone(val,direction):
    if val is None:
        if direction == '+':
            return -float('inf')
        else:
            return float('inf')
    else:
        return val

@api.route('/collections/<cid>/<year>')
@api.param('cid','Collection ID')
@api.param('year','Year')
class CollectionsSort(Resource):    
    @api.response(200,'OK')
    @api.response(500,'Invalid parameter')
    @api.doc(description = "Question 6: Retrieve top/bottom economic indicator values for a given year")
    def get(self, cid, year):
        conn = sqlite3.connect("test.db")
        c = conn.cursor()
        c.execute("SELECT indicator_id, indicator_value, entries FROM collections where id=?",(cid,))
        rows = c.fetchone()
        indicator_id, indicator_value, entries=rows
        data=json.loads(entries)
        entries_list=[]
        for i in data:
            if i["date"]==year:
                json_obj={
                    "country": i["country"],
                    "value": i["value"]
                }
                entries_list.append(json_obj)
        if 'q' in request.args:
            print("here")
            q=int(request.args.get('q'))
            entries_list_sorted=[]
            if q > 0:
                el = sorted(entries_list, key = lambda v:handleNone(v['value'],'+'),reverse=True)
            else:
                el = sorted(entries_list, key = lambda v:handleNone(v['value'],'-'))
            return{
                "indicator_id":indicator_id,
                "indicator_value":indicator_value,
                "entries":el[:abs(q)]
            }, 200
        return{
            "indicator_id":indicator_id,
            "indicator_value":indicator_value,
            "entries":entries_list
        }, 200
        
if __name__=='__main__':
    app.run(debug=True)
