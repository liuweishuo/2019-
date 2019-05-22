import pandas as pd
import time

def  static_clean():
    data = pd.read_csv('c:/Users/七月听雪/Desktop/Tencent/algo.qq.com_641013010_testa/testA/ad_static_feature.out',sep='\t',header=None)
    data[0] = data[0].astype(str)
    data.drop_duplicates(keep='first', inplace=True)  # 去重
    data.dropna(axis=0, how='any', inplace=True)  # 去空
    data = data[data[1] != 0]  # 创建时间不为0
    data = data[(~data[5].str.contains(','))]  # 行业id唯一
    data = data[(~data[0].str.contains(','))]  # 广告id唯一
    data = data.reset_index(drop=True)
    data.to_csv('D:/data/ad_static_feature.csv',index=False,header=False)

def operation_clean():
    data = pd.read_csv('C:/Users/七月听雪/Desktop/Tencent/algo.qq.com_641013010_testa/testA/ad_operation.dat',sep='\t',header=None,dtype=str)
    data.drop_duplicates(inplace=True)  # 去重
    data.dropna(axis=0, how='any', inplace=True)  # 去空
    for i in range(len(data)):   # 提出时间格式不对的
        print(str(i))
        try:
            if data[1][i] != '0':
                pd.to_datetime(data[1][i])
        except:
            data = data.drop(i,axis=0).reset_index(drop=True)
    print('第一阶段完毕！')
    data.reset_index()
    static_data = pd.read_csv('D:/data/ad_static_feature.csv',header=None,low_memory=False)
    ad_id = static_data[0].unique()
    static_data=""
    data = data[data[0].isin(ad_id)]
    data.reset_index()
    data.to_csv('D:/data_new/ad_operation.csv',index=False)

def log_set():
    with open('C:/Users/七月听雪/Desktop/Tencent/algo.qq.com_641013010_testa/testA/imps_log/totalExposureLog.out',
              'rb') as f:
        data = []
        for i, line_1 in enumerate(f):

            line = line_1.strip().split('\t'.encode())
            try:
                for j in range(len(line)):
                    line[j] = line[j].decode()
                line[1]= time.strftime("%Y--%m--%d ", time.localtime(int(line[1])))
                data.append(line)
            except:
                pass
            if (i%1000000==0)and(i>=1000000):
                data=pd.DataFrame(data)
                data.reset_index(drop=True)
                data.to_csv('D:/data_new/log_'+str(int(i/1000000))+'.csv',index=False)
                data=[]
                print('生产了日志表：',str(int(i/1000000)))
        data=pd.DataFrame(data)
        data.reset_index(drop=True)
        data.to_csv('D:/data_new/log_' + str(int(i / 1000000)+1) + '.csv',index =False)

def log_clean():
    for i in range(1,104):
        data = pd.read_csv('D:/data_new/log_' + str(i) + '.csv',usecols = [0,1,2,3,4,5,6,7,8,9],dtype=str)
        data.drop_duplicates(inplace=True)  # 去重
        data.dropna(axis=0, how='any', inplace=True)  # 去空
        data = pd.DataFrame(data)
        data.to_csv('D:/data_new/log_new_' + str(i) + '.csv', index=False)
        print('处理了：',i)


def log_static():
    # 按照各天分组
    for i in range(1,104):
        data = pd.read_csv('D:/data_new/log_new_' + str(i) + '.csv',  usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],dtype=str)
        data.columns = [0, 1, 2, 3, 4, 5, 6, 7, 8,9]
        for name, group in data.groupby(by=data[1]):
            # print(name)
            group.to_csv('D:/data_1/log_' + str(name) + '.csv', mode='a', header=None,index=False)
        print('处理了表：',i)
    dt=[]
    for i in range(16,29):
        data = pd.read_csv('D:/data_1/log_2019--02--' + str(i) + ' .csv',dtype=str)
        data.columns = [0, 1, 2, 3, 4, 5, 6, 7, 8,9]
        for name, group in data.groupby(by=data[4]):
            # print(name, group.shape[0])
            dt.append([name, group.shape[0]])
        dt = pd.DataFrame(dt)
        dt[2]=data[1][1]
        dt.to_csv('D:/data_1/log.csv', header=None,index=False,mode='a',columns=[0,1,2])
        dt=[]
        print('统计完2019--02--'+ str(i))
    dt=[]
    for i in range(1,10):
        data = pd.read_csv('D:/data_1/log_2019--03--0' + str(i) + ' .csv',dtype=str)
        data.columns = [0, 1, 2, 3, 4, 5, 6, 7, 8,9]
        for name, group in data.groupby(by=data[4]):
            # print(name, group.shape[0])
            dt.append([name, group.shape[0]])
        dt = pd.DataFrame(dt)
        dt[2]=data[1][1]
        dt.to_csv('D:/data_1/log.csv', header=None,index=False,mode='a',columns=[0,1,2])
        dt=[]
        print('统计完2019--03--0'+ str(i))

    for i in range(10,20):
        data = pd.read_csv('D:/data_1/log_2019--03--' + str(i) + ' .csv',dtype=str)
        data.columns = [0, 1, 2, 3, 4, 5, 6, 7, 8,9]
        for name, group in data.groupby(by=data[4]):

            dt.append([name, group.shape[0]])
        dt = pd.DataFrame(dt)
        dt[2]=data[1][1]
        dt.to_csv('D:/data_1/log.csv', header=None,index=False,mode='a',columns=[0,1,2])
        dt=[]
        print('统计完2019--03--'+ str(i))

def operation_set():
    data = pd.read_csv('D:/data/ad_operation.csv',dtype=str)

    data = data.reset_index()
    data_new = data.pivot(index='index', columns='3', values='4')
    data_new = data_new.reset_index()
    data_new.columns = ['index', '广告状态', '出价', '人群定向', '广告时段']
    data_set = pd.merge(data, data_new, on=['index'])
    data_set.drop(['index', '3', '4'], axis=1, inplace=True)

    data_set = data_set.sort_values(['0', '1']).reset_index(drop=True)
    data_set['ad_id'] = data_set['0']
    data_set = data_set.groupby(by=data_set['0']).fillna(method='ffill')
    data_set.drop('ad_id', axis=1, inplace=True)
    data_set.dropna(axis=0, how='any', inplace=True)  # 去空
    data_set.to_csv('D:/data/ad_operation_new.csv', index=False)

    data = pd.read_csv('D:/data/ad_operation_new.csv',dtype=str)
    n=len(data)
    for i in range(n):  # 提出时间格式不对的
        try:
            if data['1'][i] != '0':
                t = data['1'][i][0:8]+'000000'
                data['1'][i] = pd.to_datetime(t)
                print(n,len(data),str(i))
                # data.to_csv('D:/data/ad_operation_new_1.csv', index=False,model='a')
        except:
            data = data.drop(i, axis=0)
    data=data.reset_index(drop=True)
    data.drop_duplicates(subset=['1', '0'], keep='last', inplace=True)
    data.to_csv('D:/data/ad_operation.csv', index=False)

def log_merge():
    data = pd.read_csv('D:/data/ad_operation.csv',dtype=str)
    static = pd.read_csv('D:/data/ad_static_feature.csv',header=None,dtype=str)
    log = pd.read_csv('D:/data_1/log.csv',header=None)
    data_set= pd.merge(data,static,how='left',left_on= ['0'],right_on=[0])
    data_set.columns=['ad_id', '修改时间', '修改字段', '广告状态', '出价', '人群定向', '广告时段', 0, '创建时间', '广告账户id', '商品id', '商品类型', '广告行业id', '素材尺寸']
    data_set.drop(0,inplace=True,axis=1)
    # data_set.to_csv('D:/data/data_set.csv')

    log.columns=['ad_id', '曝光次数','修改时间']
    log['修改时间']=pd.to_datetime(log['修改时间'])-pd.Timedelta(days=1)
    data_set['修改时间']=pd.to_datetime(data_set['修改时间'])
    data_last= pd.merge(data_set,log,how='outer',on=['ad_id','修改时间'])
    data_last.dropna(axis=0, how='any', inplace=True)  # 去空
    data_last.drop('Unnamed: 0', axis=1, inplace=True)
    data_last.rest_index(drop=True, inplace=True)
    data_last.rename(columns={'Unnamed: 0': '样本id'}, inplace=True)
    data_last.drop('修改时间', axis=1, inplace=True)
    data_last = data[['样本id', 'ad_id', '创建时间', '素材尺寸', '广告行业id', '商品类型', '商品id', '广告账户id', '广告时段', '人群定向', '出价']]
    data_last.to_csv('D:/data/train.csv',index=False)


def LGB_predict ():
    pass


if __name__ == '__main__':
    # static_clean()
    # print('静态处理完！')
    # operation_clean()
    # print('动态处理完！')
    # log_set()
    # print('日志文件生成完！')
    # log_clean()
    # print('日志文件清洗完！')
    # log_static()
    # print('日志统计完')
    # operation_set()
    # log_merge()
    # print('日志合并完！')
    LGB_predict()







