import happybase

def getHBase(SN):
    connection = happybase.Connection('10.41.158.65')
    try:
        table = connection.table('p8_aoi_csv')
        row = table.row(SN)
        print(type(row))
        # print(row[b'BOT:20210328081302'])
    except BaseException as e:
        print(e)
    return row
    connection.close()

if __name__ == '__main__':
    USN = 'FPW1141G1VJQ4P6B7'
    getHBase(USN)
    result = getHBase(USN).items()
    for key,values in result:
        print(key)
    print(list(result)[0])