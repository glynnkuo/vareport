#!/usr/bin/python 
# -*- coding: UTF-8 -*-

import cgi
import cgitb
cgitb.enable()
import json
import pyodbc
import datetime
import base64

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
    q_cid = form.getvalue('cid')
    datetime_format='%Y-%m-%dT%H:%M:%S'
    q_from = datetime.datetime.strptime(form.getvalue('from'), datetime_format)
    q_to = datetime.datetime.strptime(form.getvalue('to'), datetime_format)

    out_data = {'type': q_type, 'cid': q_cid, 'from': q_from.strftime(datetime_format), 'to': q_to.strftime(datetime_format)}

    sql_people_count = """
        SELECT m.DeviceId, sum(cast(m.Data as int))
          FROM VAS_Retail_Package.dbo.VAS_IVS_MetaData AS m
         WHERE m.Type = '%s'
           AND m.LocalTime BETWEEN '%s' AND '%s'
           AND m.DeviceId = '%s'
      GROUP BY m.DeviceId"""
      
    sql_heatmap = """
        SELECT m.RecordId, m.LocalTime
          FROM VAS_Retail_Package.dbo.VAS_IVS_MetaData AS m
         WHERE m.Type = 'HeatMapMetaData'
           AND m.LocalTime BETWEEN '%s' AND '%s'
           AND m.DeviceId = '%s'
    """
    sql_heatmap_data = """
        SELECT b.Data
          FROM VAS_Retail_Package.dbo.MAS_IVS_BinaryData AS b
         WHERE b.MetaDataId = %d
    """


    if q_type == 'people_count':
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
            out_data['cid'] = row[0]
            out_data['in']  = row[1]

        #people out
        db_cursor.execute(sql_people_count%(
          'People Out',
          q_from.strftime(datetime_format),
          q_to.strftime(datetime_format),
          q_cid))
        row = db_cursor.fetchone()
        if row:
            out_data['cid'] = row[0]
            out_data['out'] = row[1]

    elif q_type == 'heatmap':
        db_cursor.execute(sql_heatmap%(
          q_from.strftime(datetime_format),
          q_to.strftime(datetime_format),
          q_cid))
        out_data['heatmap'] = []
        while True:
            row = db_cursor.fetchone()
            if not row:
                break
            out_data['heatmap'].append({'time': row[1].strftime(datetime_format), 'hmid': row[0]})
    elif q_type == 'heatmap_data':
        db_cursor.execute(sql_heatmap%q_hmid)
        row = db_cursor.fetchone()
        if row:
            out_data['data': row[0]]

    body = json.dumps(out_data)

    print "Status: 200 OK"
    print "Content-Type: application/json"
    print "Length:", len(body)
    print ""
    print body

if __name__ == '__main__':
    main()

