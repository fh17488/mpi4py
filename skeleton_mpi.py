from mpi4py import MPI
import socket
import numpy as np
import pymysql

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()    
    R = comm.Get_size()
    T = 10 
    N=8
    dfs_meta = {}
    if rank == 0:  
        global_avg = np.zeros(T, dtype='d')       
    conn = pymysql.connect(host  = "mpi2.cqsthdahfs0e.eu-west-1.rds.amazonaws.com",port = 3306,user = "root",password = "farazfaraz",db = "mpi")    
    conn_handler = conn.cursor()
    conn_handler.execute("select * from metadata")
    dataframes = conn_handler.fetchall()
    N = len(dataframes)
    for i in range(int(rank*N/R), int((rank+1)*N/R), 1):
        data = np.zeros((dataframes[i][1],dataframes[i][2]), dtype='d')
        df_meta = {'table_id':dataframes[i][0], 'rows':dataframes[i][1], 'cols':dataframes[i][2], 'avg': 0, 'data': data}
        conn_handler.execute("select row,col,value from data where table_id={0}".format(dataframes[i][0]))
        dataframe = conn_handler.fetchall()        
        for record in dataframe:
            data[record[0]-1, record[1]-1] = record[2]
        dfs_meta.update( {i : df_meta} )        
    #Initialization Complete
    comm.Barrier()
    for i in range(T):
        iteration_avg = 0
        for j in range(int(rank*N/R), int((rank+1)*N/R), 1):           
            df = dfs_meta[j]['data']            
            new_col = np.zeros((df.shape[0], 1), dtype='d')
            df = np.column_stack((df,new_col))
            for k in range(df.shape[0]):
                df[k][df.shape[1]-1] = np.mean(df[k,:-1])
            dfs_meta[j]['data'] = df
            dfs_meta[j]['cols'] = df.shape[1]
            dfs_meta[j]['avg'] = np.mean(df)
            iteration_avg += dfs_meta[j]['avg']                                
        iteration_avg /= N/R
        if rank == 0:
            send_buff = np.zeros(1, dtype='d')            
            recv_buff = np.zeros(1, dtype='d')
            send_buff[0] = iteration_avg
            comm.Reduce([send_buff, MPI.DOUBLE], [recv_buff, MPI.DOUBLE],
            op=MPI.SUM, root=0)
            global_avg[i] = recv_buff[0]
            global_avg[i] /= R            
        else:
            send_buff = np.zeros(1, dtype='d')            
            send_buff[0] = iteration_avg
            comm.Reduce([send_buff, MPI.DOUBLE], None,
            op=MPI.SUM, root=0)
        comm.Barrier()                
    if rank == 0:
        print("The global average is: {0}".format(np.mean(np.mean(global_avg))))
        conn_handler.execute("INSERT INTO analytics (global_average) VALUES ({0}); ".format(np.mean(global_avg)))
        conn.commit()        
    for i in range(int(rank*N/R), int((rank+1)*N/R), 1):
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
