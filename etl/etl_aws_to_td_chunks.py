user_TD = TDID
passwd_TD = TDPW
logmech = 'ldap'
host_TD = "awstdprd1.sys.cigna.com"

database = 'BISDM_CA_BASE_PRD'
view_database = 'BISDM_CA_VIEW_PRD'


#using SQL Alch instead of pyodbc
dbname = 'cigna_reporting_prd62'

td_cont = create_context(host=host_TD, username=user_TD, password=passwd_TD, logmech='LDAP', temp_database_name=database, database=database)
database_SS = 'cigna_reporting_prd62'
chunk_size = 100000


table = 'AttestForms'
table_stg = table+'_STG'


query = 'SELECT * FROM '+database_SS+'.sandbox.'+table

#Loop through dataframe with chuncks 
for chunk_num, chunked_df in enumerate(pd.read_sql(query
                                                    #, MSConn
                                                    , fv2_conn
                                                    , chunksize=chunk_size)):
    
    #cleaning df from bad chars - non-ASCII values
    #chunked_df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
    chunked_df = chunked_df.applymap(lambda x: re.sub(r'[^\x00-\x7F]+', '', str(x)))

    #get mem consumption for each chunck
    mem_usage = chunked_df.memory_usage(deep=True).sum()/1000000
    
    print('>--------------------------------------------------------------------------------------------------------------------------<')
    print(f'Imported dataframe from Foundry | chunk number: {chunk_num+1} | Size: {len(chunked_df):,} rows | Memory Usage: {mem_usage:.2f} | {time.ctime()}')
    
    
    #loading the df
    if chunk_num == 0:
        dml_operation = 'replace'
    else:
        dml_operation = 'append'
    
    print('>--------------------------------------<')
    print(f'TD Load Start | {time.ctime()}')

    # load data by chuncks 
    f_load = fastload(df = chunked_df
                    , table_name = table_stg                        
                    , schema_name = database
                    , index = False
                    , primary_index = ['person_id', 'form_id']
                    , if_exists = dml_operation                        
                    #, types=data_types
                    )
    print('>--------------------------------------<')
    print(f'TD Load Complete | {time.ctime()}')