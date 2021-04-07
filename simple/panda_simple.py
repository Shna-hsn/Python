import pandas as pd
import cx_Oracle as cx

def gettablecolumns(tablename):
    conn = cx.connect('SFCFA139', 'SFCFA139', '10.41.129.38:1522/P8MICQ')
    tablecolumns = []
    sql = "SELECT column_name FROM all_tab_columns WHERE table_name = '{0}'"
    sql = sql.format(tablename.upper())
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    for i in range(len(result)):
        tablecolumns.append(result[i][0])
    conn.close()
    return tablecolumns

def dataframeInstance(file):
    try:
        df = pd.read_csv(file) # 所有内容,index_col被当成索引的栏位
        # print(df)
        fileheadertmp = [item for item in df.columns.values if 'Unnamed:' not in item] #表头
        # print(str(fileheadertmp).lower())
        datatmp = pd.DataFrame(df,columns=fileheadertmp) #筛选出表头跟值，排除Unnamed:栏
        # print(datatmp)
        fileheader = [item.strip() for item in datatmp.columns.values] #表头,strip()移除字符串头尾指定的字符
        # print(fileheader)
        data = pd.DataFrame(datatmp.values,columns=fileheader) #筛选出表头跟值，排除Unnamed:栏
        data.dropna(axis=0, how='all') #dropna删除空值，axis为0以列删除，1以行删除，how='all'代表行或者列都为空才删除，为any代表行或者列其中一个值为空就删除
        if (True in pd.isnull(data['Time']).values) or (True in pd.isnull(data['WorkStation']).values):
            data = {}
            fileheader = []
            print('there are null value in primary_key columns')
    except Exception as e:
        print('e2:')
        print(e)
        print("def dataframeInstance error:")
        print(file)
        data = {}
        fileheader = []
    finally:
        return data,fileheader

def Dataupload(tablename,data,columns):
    connection = cx.connect('SFCFA139', 'SFCFA139', '10.41.129.38:1522/P8MICQ')
    fileindex = data.index.values
    valueslist = []
    for i in fileindex:
        j = 0
        sql = "select * from lb_log_machine_test where time = '{0}' and workstation = '{1}'"
        sql = sql.format(data.loc[i,'Time'],data.loc[i,'WorkStation'])
        cur = connection.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        # print(result)
        if bool(result):
            pass
        else:
            for item in columns:
                # print(data.loc[i,item]) #逐行打印每一列的值
                if j == 0:
                    j += 1
                    valuestr = "'" + str(data.loc[i,item]).replace('nan','') + "'"
                else:
                    valuestr = valuestr + ",'" + str(data.loc[i,item]).replace('nan','') + "'"
            valueslist.append(valuestr)
    # print(valueslist)

    for i in range(len(columns)):
        if i == 0:
            columnstr = columns[i].lower()
        else:
            columnstr = columnstr + "," + columns[i].lower()
    # print(columnstr)

    try:
        for i in (range(len(valueslist))):
            sql = "insert into " + tablename + '(' + columnstr + ')' + " values (" + valueslist[i] + ")"
            print(sql)
            cur = connection.cursor()
            cur.execute(sql)
            connection.commit()
    except Exception as e:
        print(e)
    connection.close()

if __name__ == '__main__':
    file = r'C:\Users\Z18073047\Desktop\TB1-1FT-F05-LB@D--camdata-WIZS_TB1-1FT-05_LI.csv'
    tablename = 'lb_log_machine_test'
    header = []
    tablecolumns = gettablecolumns(tablename)
    data,fileheader = dataframeInstance(file)
    # print(values.index.values) #索引的值
    for i in range(len(fileheader)):
        header.append(fileheader[i].upper())
    if set(header).issubset(set(tablecolumns)):
        Dataupload(tablename,data,fileheader)