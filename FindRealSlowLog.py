import re
from datetime import datetime
import mysql.connector
from threading import Thread, Event

def execute_sql(sql, timeout=10):
    # 连接到MySQL数据库
    conn = mysql.connector.connect(
        host="yourhost",
        user="yourusername",
        password="yourpassword",
        database="youtdatabase",
        buffered = True
    )

    # 创建一个事件对象,用于控制超时
    timeout_event = Event()

    def run_sql():
        try:
            # 获取游标
            cursor = conn.cursor()
            
            # 执行SQL语句
            start_time = datetime.now()
            cursor.execute(sql)
            end_time = datetime.now()
            
            # 计算执行时间
            execution_time = (end_time - start_time).total_seconds()
            
            if execution_time > timeout:
                print(f"SQL语句执行超时 ({execution_time:.2f}秒):")
                print(sql)
            else:
                if execution_time > 3 :
                    print(f"SQL语句执行成功 ({execution_time:.2f}秒)")
                    print(sql)
        except Exception as e:
            # print(f"SQL语句执行失败: {e}")
            pass
        finally:
            # 关闭游标和连接
            cursor.close()
            conn.close()
            
            # 设置事件,表示执行完毕
            timeout_event.set()

    # 启动一个线程执行SQL语句
    thread = Thread(target=run_sql)
    thread.start()
    
    # 等待线程执行或超时
    timeout_event.wait(timeout)
    
    # 如果超时,则终止线程
    if not timeout_event.is_set():
        print(f"SQL语句执行超时 ({timeout}秒):")
        print(sql)
def GetSql(seekSize : int ):
    slowlog_patern =  r'(?i)select\s+(.+?)\s+from\s+(.+?)(;|$)'
    with open('mysql-slow.log','r',encoding="utf8") as f :
        f.seek(seekSize)
        lines = f.read()
        for match in re.finditer(slowlog_patern, lines,  re.DOTALL):
            querySql = match.group()
            yield querySql

def extract_content_after_time(file_path, target_time):
    ###当文件过大时，定位到对应时间点。
    with open(file_path, 'r',encoding='utf8') as file:
        lines = file.readlines()
        target_time_Date = datetime.strptime(target_time, '%Y-%m-%d %H:%M:%S')
        year , month , day = target_time_Date.year , str(target_time_Date.month).zfill(2) ,str(target_time_Date.day).zfill(2)
        time_pattern = rf'# Time: ({year}-{month}-\d{{2}}T\d{{2}}:\d{{2}}:\d{{2}}\.\d+Z)'
        seekSize = 0  
        for line in lines:
            match = re.search(time_pattern, line)
            if match:
                line_time = datetime.strptime(match.group(1), '%Y-%m-%dT%H:%M:%S.%fZ')
                print(line_time)
                if line_time >= target_time_Date:
                    return seekSize
            seekSize += len(line)

if __name__ == '__main__' :
    filePath = "mysql-slow.log"
    ##从这个时间点开找
    timeStr = "2024-07-22 00:00:00"
    # SeekSize = extract_content_after_time(filePath,timeStr)
    SeekSize = 160569701
    for sql in GetSql(SeekSize):
        execute_sql(sql)
