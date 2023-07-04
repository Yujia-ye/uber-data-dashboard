import paramiko
from sql2mongo import sql_to_mongo


def get_response(command):
    HOST = '192.168.0.116'

    try:
        query = sql_to_mongo(command)+'.pretty()'
        print(query)
    except:
        return 'Syntax error or Syntax not supported'

    command = "/bin/echo '{}' |  ~/mongodb/bin/mongo 192.168.0.116:4000/airbnb".format(query)

    # 创建SSH对象
    ssh = paramiko.SSHClient()
    # 允许连接不在know_hosts文件中的主机
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 连接服务器
    ssh.connect(hostname=HOST, username='angeliaye', password='1231')
    # 执行命令
    stdin, stdout, stderr = ssh.exec_command(command)
    # 获取命令结果
    result = stdout.read()
    result = str(result, encoding = "utf-8")
    # 关闭连接
    new_idx = result.index("MongoDB server version: 3.6.23")+len("MongoDB server version: 3.6.23")
    ssh.close()
    return result[new_idx:-4]

if __name__ == '__main__':
    get_response("""SELECT * FROM book """)