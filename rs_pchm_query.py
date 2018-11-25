#!/usr/bin/python 
# -*- coding: UTF-8 -*-

import cgi
import cgitb
cgitb.enable()
import json
import pyodbc
import datetime
import base64

datetime_format='%Y-%m-%dT%H:%M:%S'

def people_count(form, db_cursor):
    q_cid = form.getvalue('cid')
    q_from = datetime.datetime.strptime(form.getvalue('from'), datetime_format)
    q_to = datetime.datetime.strptime(form.getvalue('to'), datetime_format)

    sql_people_count = """
        SELECT m.DeviceId, sum(cast(m.Data as int))
          FROM VAS_Retail_Package.dbo.VAS_IVS_MetaData AS m
         WHERE m.Type = '%s'
           AND m.LocalTime BETWEEN '%s' AND '%s'
           AND m.DeviceId = '%s'
      GROUP BY m.DeviceId
    """
    
    out_data = {}
    out_data['in'] = 0
    out_data['out'] = 0
    #people in
    db_cursor.execute(sql_people_count%(
      'People In',
      q_from.strftime(datetime_format),
      q_to.strftime(datetime_format),
      q_cid))
    row = db_cursor.fetchone()
    if row:
        out_data['in']  = row[1]

    #people out
    db_cursor.execute(sql_people_count%(
      'People Out',
      q_from.strftime(datetime_format),
      q_to.strftime(datetime_format),
      q_cid))
    row = db_cursor.fetchone()
    if row:
        out_data['out'] = row[1]
    return 'application/json', json.dumps(out_data)

def heatmap(form, db_cursor):
    q_cid = form.getvalue('cid')
    q_from = datetime.datetime.strptime(form.getvalue('from'), datetime_format)
    q_to = datetime.datetime.strptime(form.getvalue('to'), datetime_format)

    sql_heatmap = """
      SELECT m.RecordId, m.LocalTime
        FROM VAS_Retail_Package.dbo.VAS_IVS_MetaData AS m
       WHERE m.Type = 'HeatMapMetaData'
         AND m.LocalTime BETWEEN '%s' AND '%s'
         AND m.DeviceId = '%s'
    """
    db_cursor.execute(sql_heatmap%(
      q_from.strftime(datetime_format),
      q_to.strftime(datetime_format),
      q_cid))
      
    out_data = {}
    out_data['heatmap'] = []
    while True:
        row = db_cursor.fetchone()
        if not row:
            break
        out_data['heatmap'].append({'time': row[1].strftime(datetime_format), 'hmid': row[0]})
        
    return 'application/json', json.dumps(out_data)
    
def heatmap_data(form, db_cursor):
    q_hmid = form.getvalue('hmid')
    sql_heatmap_data = """
      SELECT b.Data
        FROM VAS_Retail_Package.dbo.MAS_IVS_BinaryData AS b
       WHERE b.MetaDataId = %s
    """
    db_cursor.execute(sql_heatmap%q_hmid)
    row = db_cursor.fetchone()
    out_data = {}
    if row:
        out_data['heatmap_data'] = base64.b64encode(row[0])
    else:
        out_data['error']= "hmid %s not fount"%q_hmid
    return 'application/json', json.dumps(out_data)

    
def main():
    db_ip     = '192.168.225.253'
    db_name   = 'VAS_Retail_Package'
    db_user   = 'sa'
    db_pwd    = 'Aa123456'
    db_driver = '{ODBC Driver 17 for SQL Server}'
    db_conn   = pyodbc.connect('DRIVER='+db_driver+';SERVER='+db_ip+';PORT=1433;DATABASE='+db_name+';UID='+db_user+';PWD='+db_pwd)
    db_cursor = db_conn.cursor()

    form = cgi.FieldStorage()
    q_type = form.getvalue('type')

    content_type = ""
    body = None
    if q_type == 'people_count':
        content_type, body = people_count(form, db_cursor)
    elif q_type == 'heatmap':
        content_type, body = heatmap(form, db_cursor)
    elif q_type == 'heatmap_data':
        content_type, body = heatmap_data(form, db_cursor)
    else
        content_type = 'application/json'
        body = json.dumps({'error': 'unknow command type'})

    print "Status: 200 OK"
    print "Content-Type: %s"%content_type
    print "Length:", len(body)
    print ""
    print body

if __name__ == '__main__':
    main()

