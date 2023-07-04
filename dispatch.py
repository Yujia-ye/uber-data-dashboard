import random
import math
import pandas as pd
import pymongo
import ide
from sshtunnel import SSHTunnelForwarder

near = {12:[13,261,88],88:[12,261,87],87:[88,261,209],13:[231,261,12],261:[13,231,209,87,88,12],209:[87,261,231,45],
            231:[13,261,209,45,144,211,125],45:[209,231,144,148,232],
            125:[158,249,114,211,231],211:[125,114,144,231],144:[45,231,211,114,148],148:[45,144,79,232],232:[45,148,4],
            158:[246,68,249,125],249:[158,68,90,113,114,125],114:[125,211,144,79,249],113:[114,49,234,79],79:[113,114,107,224,4,148],4:[232,79,224],
            246:[50,48,68,158],68:[246,48,100,90,186,249,158],90:[68,186,234,249],186:[68,19,164,90,234],100:[68,48,230,164,186],164:[186,100,161,170,234],234:[90,186,164,107,113],107:[79,224,234,137,170],224:[4,70,107,130],137:[224,107,170,233],170:[137,107,164,161,162,233],
            50:[246,48,143],48:[68,246,50,142,163,230,100],230:[100,48,163,161],161:[164,230,163,162,170],162:[170,161,163,237,229,233],229:[233,162,141,140],233:[137,170,162,229],
            143:[50,142,239],142:[48,143,239,43,163],43:[142,239,238,151,24,41,75,236,237,163],237:[162,162,43,236,141],141:[237,236,263,140,229],140:[229,141,262],
            239:[142,142,43,238],238:[239,151,43],236:[237,43,75,263],263:[141,236,75,262],262:[140,265,75],
            151:[24,238,43],24:[166,41,43,151],75:[236,263,262,43,41,74],
            166:[24,41,152],41:[43,24,166,42,74],74:[75,41,42],
            152:[42,166,116],42:[41,74,120,116,152],116:[152,42,120,244],
            244:[120,116,243],120:[244,243,42,116],243:[120,244,128,127],
            127:[128,120,243],128:[243,127]}

dist = {}


time = {}


def get_near_loc(locID):
    #查询一个区相邻的所有区
    return near[locID]

#定义一个出租车类
class vehicle():
    def __init__(self,ID):
        #构造函数进行可视化
        self.ID = ID #出租车ID
        self.locID = list(near.keys())[random.randint(0,61)] #出租车所在的区号
        self.vacatime = random.randint(0,10*60) #空车时间，随机生成
        self.available = 1 #是否有乘客
        self.desID = 0 #目的地
        self.endTime = 0 #当前行程结束时间
        self.startTime = 0 #当前行程开始时间

    def update(self,vacatime,locID,available,desID):
        self.vacatime = vacatime
        self.locID = locID
        self.available = available
        self.desID = desID

#定义一个订单请求类
class call():
    def __init__(self,ID,pickupID,dropID,calltime,duration,dist):
        self.callID = ID #订单ID
        self.pickupID = list(near.keys())[random.randint(0,61)] #订单上车点
        self.dropID = list(near.keys())[random.randint(0,61)] #下车点
        self.calltime = 0 #订单请求时间
        self.duration = duration #订单时长
        self.dist = dist #距离


def probability(taxi,call,near=1):
    #计算taxi司机接call单概率
    if near == 1:
        dist =call.dist
        return 0.2*math.sqrt(taxi.vacatime)/math.sqrt(600) + 0.8 * (random.randint(8,10)/10) **(math.exp(-((dist-3)/0.5)**2))
    else:
        dist = call.dist
        return 0.2 * math.sqrt(taxi.vacatime) / math.sqrt(600) + 0.5 * (random.randint(8,10)/10) **(math.exp(-((dist-3)/0.5)**2))

def cal_avg(solutions,a):
    #计算某个单子的平均接单率
    avg_ = 0
    for call in range(len(users)):
        ei = 1
        time = 0
        for taxi in range(len(taxis)):
            ei *= (1 - solutions[call][taxi][1]) ** a[call][taxi]
            time += solutions[call][taxi][0] * a[call][taxi]/3600
        avg_ += 1 - ei + 6 / math.exp(time)

    avg_ /= len(users)
    return avg_

def gen_dispatch(taxis,users,max_res):
    #调度算法
    a = [[0 for j in taxis] for i in users]
    solutions =[[] for i in users]

    D = [0 for i in taxis]
    M = [-1 for i in users]

    failure = 0
    #对于每一个订单和每一个请求，都计算接单概率
    for call in users:
        solution = [[] for i in taxis]
        nearlocs = get_near_loc(call.pickupID) + [call.pickupID]
        for taxi in taxis:
            if taxi.locID in nearlocs or taxi.desID in nearlocs:
                if taxi.available == 1:
                    solution[taxi.ID] = [call.calltime+call.duration,probability(taxi,call)]
                else :
                    solution[taxi.ID] = [taxi.endTime+call.duration,probability(taxi,call)]
            else:
                if taxi.available == 1:
                    solution[taxi.ID] = [call.calltime+call.duration,probability(taxi,call,0)]
                else :
                    solution[taxi.ID] = [taxi.endTime+call.duration,probability(taxi,call,0)]

        solutions[call.callID] = solution
    #对于每一个司机，把他接单概率最高的单子派给他
    for taxi in range(len(taxis)):
        max = 0
        for call in range(len(users)):
            if solutions[call][taxi][1] > max:
                max = solutions[call][taxi][1]
                D[taxi] = call

        a[D[taxi]][taxi] = 1

    avg = cal_avg(solutions,a)
    #计算当前平均接单率
    res = [0 for i in users]
    for call in range(len(users)):
        for taxi in range(len(taxis)):
            if a[call][taxi] == 1:
                res[call] += 1

    #使用Hill climbing算法来优化
    for call in range(len(users)):
        Upairs = {}
        U = []
        if res[call] >= max_res:
            continue
        #若订单i没有派给司机j，计算将派给司机j的订单改派为i，平均接单率会不会上升，如果上升就改派
        for taxi in range(len(taxis)):
            if a[call][taxi] == 0:
                Upairs[taxi] = solutions[call][taxi][0]/60*solutions[call][taxi][1]

        for item in sorted(Upairs.items(),key = lambda x:x[1])[:max_res]:
            U.append(item[0])

        for j in range(len(U)):
            k = U[j]
            replace = a.copy()
            if res[D[k]] == 1:
                continue
            replace[D[k]][k] = 0
            replace[call][k] = 1
            new_avg = cal_avg(solutions,replace)
            if new_avg > avg:
                D[k] = call
                a = replace.copy()
                avg = new_avg
                res[D[k]] -= 1
                res[call] += 1

    #根据当前的结果返回每个司机被派到的单，每个单派到的司机
    for i in range(len(a)):
        dispatched = []
        for j in range(len(a[i])):
            if a[i][j] == 1:
                dispatched.append(j)
        try:

            picked = random.randint(0,len(dispatched)-1)
            M[i] = dispatched[picked]
            #print(i,'派给了',dispatched,' 接单' + str(M[i]))
        except:
            failure += 1
            #print(i, '派给了', dispatched)
            continue


    return D,M,avg,solutions,failure



if __name__ == '__main__':
    HOST = '106.75.255.69'
    server = SSHTunnelForwarder(
        HOST,
        ssh_username='ubuntu',
        ssh_password='yyjhx991231',
        remote_bind_address=('127.0.0.1', 27017)
    )

    server.start()
    #ssh连接读mongoDB的数据
    client = pymongo.MongoClient("127.0.0.1", server.local_bind_port)  # server.local_bind_port is assigned local port
    dblist = client.list_database_names()
    data = client['taxidata']['dispatch'].find({},{"_id": 0})

    input = []
    for item in data:
        input.append(item)

    data = pd.DataFrame(input).sort_values(by='pickup')
    print(data)

    data['pickup'] = pd.to_datetime(data['pickup'])
    data['dropof'] = pd.to_datetime(data['dropof'])

    taxis = []
    taxi_num = 500
    
    #对于每一个司机乘客与订单数量之比，计算接单率和响应时间
    for ratio in [0.5,0.6,0.7,0.8,0.9,1,1.1,1.2,1.3,1.4,1.5]:
        for taxi in range(int(taxi_num*ratio)):
            taxis.append(vehicle(taxi))
        time = 0
        for item in range(10):
            users = []
            fail = 0
            wait = 0
            count = 0
            for callid in range(item*50,(item+1)*50):
                time = int(data.iloc[callid].pickup.strftime('%M:%S')[:2])*60+int(data.iloc[callid].pickup.strftime('%i:%S')[2:])
                users.append(call(count,data.iloc[callid].pickupid,data.iloc[callid].dropoffid,
                                  time,data.iloc[callid].duration,data.iloc[callid].dist))
                count += 1

            D, M, avg_, solutions,failure = gen_dispatch(taxis, users,3)

            for taxi in taxis:
                if taxi.ID in M:
                    taxi.available=0
                    taxi.vacatime=0
                    taxi.endTime =users[M.index(taxi.ID)].calltime+users[M.index(taxi.ID)].duration
                    taxi.desID=users[M.index(taxi.ID)].dropID


                if taxi.endTime < time and taxi.available ==0:
                    taxi.vacatime = time-taxi.endTime
                    taxi.available = 1
                    taxi.locID = taxi.desID
                    taxi.desID = 0


            fail += failure/len(users)
            wait += sum([solutions[i][M[i]][0] for i in range(len(users)) if M[i]!=-1])/len(users)/60
        print(ratio,wait/10,fail/10)






