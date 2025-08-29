fv2_conn = psycopg2.connect(f'host={host} port={port} dbname={dbname} user={user} password={password}')
td_cont = create_context(host=host_TD, username=user_TD, password=passwd_TD, logmech='LDAP', temp_database_name=database, database=database)

df

print('>--------------------------------------<')
print(f'TD Load Start | {time.ctime()}')
f_load = fastload(df = df
                , table_name = table_teradata                        
                , schema_name = database
                , index = False
                , primary_index = ['codevalue']
                , if_exists = 'replace'                        
                #, types=data_types
                )

print(f'TD Load End | {time.ctime()}') 

