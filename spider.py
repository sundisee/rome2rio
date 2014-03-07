#encoding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import urllib
import urllib2
import time
import json
import MySQLdb
import threading


key = ('uyZPoJBM',)
rome2rio_api_url = 'http://free.rome2rio.com/api/1.2/json/Search'

top50_city_sql = 'select id from city where cityname_en is not null order by beentocounts desc limit 50'
top100_poi_sql = 'select id,lat,lng,englishname,chinesename from place_poi where englishname is not null and mapstatus=1 and cateid=32 and cityid=%s order by beentocounts desc limit 100;'
insert2json_sql = 'insert into json(oid,olat,olng,oenglishname,ochinesename,did,dlat,dlng,denglishname,dchinesename,json_data) values(%r,%r,%r,%r,%r,%r,%r,%r,%r,%r,"%s")'

conn=MySQLdb.connect(host='localhost',user='root',passwd='',db='amy',port=3306,charset='utf8')
cur=conn.cursor()
cur.execute(top50_city_sql)
citys = cur.fetchall()

#class mutilThread(threading.Thread):
def main(c):
    global cur
    try:
        for cityid in c:
            #查询同一个城市100个poi之间的路线
            cur.execute(top100_poi_sql % cityid[0])
            pois = cur.fetchall()
            for k,_ in enumerate(pois):
                for step in xrange(len(pois)-k):
                    step += k + 1
                    if step < len(pois):
                        for i  in xrange(2):
                            did = str(pois[step][0])
                            dlat = str(pois[step][1])
                            dlng = str(pois[step][2])
                            denglishname = str(pois[step][3])
                            dchinesename = str(pois[step][4])
                            oid = str(pois[k][0])
                            olat = str(pois[k][1])
                            olng = str(pois[k][2])
                            oenglishname = str(pois[k][3])
                            ochinesename = str(pois[k][4])
                            dPos = dlat+','+dlng
                            oPos = olat+','+olng
                            if i == 1:
                                dlat,olat = olat,dlat
                                dlng,olng = olng,dlng
                                oid,did = did,oid
                                denglishname,oenglishname = oenglishname,denglishname
                                dchinesename,ochinesename = ochinesename,dchinesename
                                dPos,oPos = oPos,dPos
                            params = {'key':key[0],'oPos':oPos,'dPos':dPos}
                            params = urllib.urlencode(params)
                            request_url = rome2rio_api_url+'?'+params
                            print request_url
                            response = urllib2.urlopen(request_url)
                            data = response.read()
                            data = json.loads(data)
                            print insert2json_sql % (oid,olat,olng,oenglishname,ochinesename,did,dlat,dlng,denglishname,dchinesename,str(data))
                            cur.execute(insert2json_sql % (oid,olat,olng,oenglishname,ochinesename,did,dlat,dlng,denglishname,dchinesename,str(data)))
                        time.sleep(0.1)
    except Exception,e:
        conn=MySQLdb.connect(host='localhost',user='root',passwd='',db='amy',port=3306,charset='utf8')
        cur=conn.cursor()
        print str(e)

if __name__ == '__main__':
#    main(citys)
    for i in xrange(5):
        c = citys[10*i:10*(i+1)-1]
        th = threading.Thread(target=main,args=(c,))
        print '%s' % th
        th.start()
    time.sleep(2)
