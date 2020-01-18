import numpy as np
import pymysql

def main():
    T = 10   
    global_avg = np.zeros(T, dtype=np.float64)    
    N=8
    R=1
    dfs_meta = {}
    conn = pymysql.connect(host  = "******",.rds.amazonaws.com",port = 3306,user = "******",",password = "******",db = "mpi")    
    conn_handler = conn.cursor()
    conn_handler.execute("select * from metadata")
    dataframes = conn_handler.fetchall()
    N = len(dataframes)
    for i in range(N):
        data = np.zeros((dataframes[i][1],dataframes[i][2]), dtype=np.float64)
        df_meta = {'table_id':dataframes[i][0], 'rows':dataframes[i][1], 'cols':dataframes[i][2], 'avg': 0, 'data': data}
        conn_handler.execute("select row,col,value from data where table_id={0}".format(dataframes[i][0]))
        dataframe = conn_handler.fetchall()        
        for record in dataframe:
            data[record[0]-1, record[1]-1] = record[2]
        dfs_meta.update( {i : df_meta} )        
    #Initialization Complete
    for i in range(T):
        iteration_avg = 0
        for j in range(N):            
            df = dfs_meta[j]['data']            
            new_col = np.zeros((df.shape[0], 1), dtype=np.float64)
            df = np.column_stack((df,new_col))
            for k in range(df.shape[0]):
                df[k][df.shape[1]-1] = np.mean(df[k,:-1])
            dfs_meta[j]['data'] = df
            dfs_meta[j]['cols'] = df.shape[1]
            dfs_meta[j]['avg'] = np.mean(df)
            iteration_avg += dfs_meta[j]['avg']                                
        iteration_avg /= N        
        global_avg[i] = iteration_avg            
    print("The global average is: {0}".format(np.mean(np.mean(global_avg))))
    conn_handler.execute("INSERT INTO analytics (global_average) VALUES ({0}); ".format(np.mean(global_avg)))
    conn.commit()        
    for i in range(N):        
        conn_handler.execute("update metadata set cols = {0} where table_id={1}".format(dfs_meta[i]['cols'], dfs_meta[i]['table_id']))
        conn_handler.execute("delete from data where table_id={0}".format(dfs_meta[i]['table_id']))                
        for j in range(dfs_meta[i]['rows']):
            for k in range(dfs_meta[i]['cols']):                                
                conn_handler.execute("insert into data (table_id,row,col,value) values ({0},{1},{2},{3})".format(dfs_meta[i]['table_id'], j, k, dfs_meta[i]['data'][j][k]))
        conn.commit() 
    conn_handler.close()
    conn.close()
    print("done")

if __name__ == '__main__':
    main()
