from flask import Flask,request,render_template,redirect,url_for
import pymongo
import ide
from sshtunnel import SSHTunnelForwarder
import json
import  paramiko
import seaborn as sns
import pandas as pd
import time,datetime

app = Flask(__name__)

@app.route('/')
def main():
    info = [{'id':20869092,'cnt':580},
            {'id':31077175,'cnt':361},
            {'id':9453790,'cnt':268},
            {'id':6013487,'cnt':241},
            {'id':11753619,'cnt':234},
            {'id':5964843,'cnt':228},
            {'id':1026451,'cnt':227},
            {'id':6033632,'cnt':225},
            {'id':605314,'cnt':215},
            {'id':6033826,'cnt':207}]
    csv = pd.read_csv("listing.csv")
    expensive = []
    for i in range(len(csv)):
        item = {}
        item['rank'] = i + 1
        item['id'] = csv.iloc[i].id
        item['url'] = csv.iloc[i].listing_url
        item['nbh'] = csv.iloc[i].neighbourhood_cleansed
        item['room_type'] = csv.iloc[i].room_type
        item['price'] = csv.iloc[i].price
        expensive.append(item)
    return render_template('unit.html',info=info,expensive=expensive)

@app.route('/user')
def query():
    csv = pd.read_csv("host.csv").sort_values(by='total_listings_count',ascending=False)
    tophost = []
    for i in range(len(csv)):
        item = {}
        item['name'] = csv.iloc[i].host_name
        item['rank'] = i+1
        item['url'] = csv.iloc[i].url
        item['location'] = csv.iloc[i].location
        item['response_rate'] = csv.iloc[i].response_rate
        item['total_listings_count'] = csv.iloc[i].total_listings_count
        tophost.append(item)
    guest = pd.read_csv("guest.csv")
    topguest = []
    for i in range(len(guest)):
        item = {}
        item['rank'] = i + 1
        item['guest_name'] = guest.iloc[i].guest_name
        item['count'] = guest.iloc[i].reviews
        item['id'] = guest.iloc[i].id
        topguest.append(item)

    return render_template('user.html',info=tophost,info2=topguest)


@app.route('/time_pred/<pickup>&<dropoff>',methods=['GET'])
def time_pred(pickup,dropoff):
    dist = request.args.get("dist", type=str, default='')

    if dist == '':
        #ssh连接
        HOST = '106.75.255.69'
        server = SSHTunnelForwarder(
            HOST,
            ssh_username='ubuntu',
            ssh_password='yyjhx991231',
            remote_bind_address=('127.0.0.1', 27017)
        )
        server.start()
        #连接mongoDB数据库
        client = pymongo.MongoClient("127.0.0.1",
                                     server.local_bind_port)  # server.local_bind_port is assigned local port

        for item in client['taxidata']['dist'].find({'_id':{'pickup': int(pickup), 'dropoff': int(dropoff)}}):
            avg = item['avg']
        for item in  client['taxidata']['dist'].find({'_id':{'pickup': int(pickup), 'dropoff': int(dropoff)}}):
            min = item['min']
        for item in  client['taxidata']['dist'].find({'_id':{'pickup': int(pickup), 'dropoff': int(dropoff)}}):
            max = item['max']
        #从dist集合中取数，取出指定上下车点距离的平均、最小、最大值
        server.stop()
        return render_template('time.html',avg=avg,min=min,max=max,pickup=pickup,dropoff=dropoff,res=0)
    else:
        date = request.args.get("date", type=str, default=None)
        time = request.args.get("time", type=str, default=None)
        hour = int(time[:2])
        min = int(time[-2:])
        #ssh连接
        HOST = '106.75.255.69'
        server = SSHTunnelForwarder(
            HOST,
            ssh_username='ubuntu',
            ssh_password='yyjhx991231',
            remote_bind_address=('127.0.0.1', 27017)
        )
        # 连接mongoDB数据库
        server.start()
        client = pymongo.MongoClient("127.0.0.1",
                                     server.local_bind_port)  # server.local_bind_port is assigned local port

        print(dist, date, time, hour,pickup,dropoff)

        #封装MongoDB查询语句
        for item in client['taxidata']['dist'].find({'_id': {'pickup': int(pickup), 'dropoff': int(dropoff)}}):
            avg = item['avg']
        for item in client['taxidata']['dist'].find({'_id': {'pickup': int(pickup), 'dropoff': int(dropoff)}}):
            min = item['min']
        for item in client['taxidata']['dist'].find({'_id': {'pickup': int(pickup), 'dropoff': int(dropoff)}}):
            max = item['max']
        print({'dist': float(dist), 'dow': datetime.datetime.strptime(date, "%Y-%m-%d").weekday() + 1,
             'dom': datetime.datetime.strptime(date, "%Y-%m-%d").day, 'dropoffid': int(dropoff), 'hour': int(hour),
             'minute': int(min),
             'month': datetime.datetime.strptime(date, "%Y-%m-%d").month, 'pickupid': int(pickup),
             })
        client['taxidata']['input'].drop()
        client['taxidata']['input'].insert(
            {'dist': float(dist), 'dow': datetime.datetime.strptime(date, "%Y-%m-%d").weekday() + 1,
             'dom': datetime.datetime.strptime(date, "%Y-%m-%d").day, 'dropoffid': int(dropoff), 'hour': int(hour),
             'minute': int(min),
             'month': datetime.datetime.strptime(date, "%Y-%m-%d").month, 'pickupid': int(pickup),
             })
        client['taxidata']['result'].drop()
        command = "sudo python3 spark.py"
        # 创建SSH对象
        ssh = paramiko.SSHClient()
        # 允许连接不在know_hosts文件中的主机
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # 连接服务器
        ssh.connect(hostname=HOST, username='ubuntu', password='yyjhx991231')
        # 执行命令
        stdin, stdout, stderr = ssh.exec_command(command)
        time = stdout.read()
        time = str(time).split('prediction=')[1].split(')')[0]
        # 获取命令结果
        # 关闭连接
        ssh.close()
        return render_template('time.html',time = time,res=1)


if __name__ == '__main__':
    listing = pd.read_csv('~/Desktop/MsDSML/DSA5104/data/final_listings.csv')
    app.run()
