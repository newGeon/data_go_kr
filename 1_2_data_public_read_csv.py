import os
import gc
import csv
import time
import shutil
import datetime
import jaydebeapi


from nl2sql_util.db_util import tibero_connector

if __name__ =="__main__":

    csv_path = os.path.join(os.getcwd(), 'csv_data')
    complete_path = os.path.join(os.getcwd(), 'csv_complete')
    not_insert_path = os.path.join(os.getcwd(), 'not_data')
    file_list = os.listdir(csv_path)
    # file_list = file_list[::-1]
    
    conn = tibero_connector()
    
    ## 1.CSV 파일 읽기
    for one in file_list:

        full_path = os.path.join(csv_path, one)
        file_ext = os.path.splitext(full_path)[1]
        file_ext = file_ext.replace('.', '')
        
        file_nmae = one.rsplit('.')[0]
        
        refined_file_name = ''

        # CSV 관련 저장 데이터 리스트
        data_list = []
        try:
            # CSV 파일만
            if file_ext == 'csv':
                f_open = open(full_path, 'r', encoding='cp949')
                r_data = csv.reader(f_open)

                # 날짜 형태를 제거한 파일명
                refined_file_name = file_nmae.rsplit('_', 1)[0]
                
                for line in r_data:
                    defined_line = [l.replace('\x00', '') for l in line]
                    data_list.append(defined_line)
                f_open.close()

        except UnicodeDecodeError as e:
            print(one)
            continue

        print("### Tibero 데이터 검색 ###################################")
        print(f'FILE NAME = {refined_file_name}')
        
        # 파일 형태가 CSV 파일 인 경우에만 진행
        if file_ext == 'csv':
            cur = conn.cursor()

            data_basic_sql = """ SELECT ID, DATA_NAME, COLLECT_URL_LINK, DATA_ORIGIN_KEY
                                   FROM AE_DATA_BASIC_INFO 
                                  WHERE DATA_NAME = ?
                                    AND COLLECT_SITE_ID = ?
                             """
            values_basic = (refined_file_name, 3)
            cur.execute(data_basic_sql, values_basic)
            data_basic_fetch = cur.fetchone()

            # 2. 검색된 데이터가 있는 경우에 TABLE, COLUMN 정보 입력
            if data_basic_fetch != None:
                
                print("2. 검색된 데이터가 AE_DATA_BASIC_INFO 있는 경우 ")

                data_basic_id = data_basic_fetch[0]
                data_name = data_basic_fetch[1]
                len_rows = len(data_list) - 1
                
                # 다시 연결
                cur = conn.cursor()

                new_num = 30000 + data_basic_id
                str_num = str(new_num).rjust(6, '0')
                
                logical_table_english = 'DATA_TMP_' + str_num
                physical_table_name = 'NLDATA_' + str_num
                orig_table_name = 'TMP_' + str_num
                
                table_select = """ SELECT ID, DATA_BASIC_ID, LOGICAL_TABLE_KOREAN
                                     FROM AE_MANAGE_PHYSICAL_TABLE
                                    WHERE PHYSICAL_TABLE_NAME = ?
                               """
                values_table_select = (physical_table_name, )
                cur.execute(table_select, values_table_select)
                table_result = cur.fetchone()

                # 2.1. Table 정보가 없을 경우에만 Table 정보 입력
                if table_result == None:
                    print("2.1. Table 정보가 없기 때문에 Table 정보 입력 준비 ")

                    # MANAGE_PHYSICAL_TABLE 테이블 데이터 INSERT     
                    table_sql = """INSERT INTO AE_MANAGE_PHYSICAL_TABLE (ID, DATA_BASIC_ID, LOGICAL_TABLE_KOREAN, LOGICAL_TABLE_ENGLISH, 
                                                                        PHYSICAL_CREATED_YN, DATA_INSERTED_YN, DATA_INSERT_ROW, TARGET_ROWS, 
                                                                        PHYSICAL_TABLE_NAME, ORIG_TABLE_NAME) 
                                   VALUES (AE_SEQ_MTABLE.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """
                    values_table = (data_basic_id, data_name, logical_table_english, 'N', 'N', 0, len_rows, physical_table_name, orig_table_name)
                    cur.execute(table_sql, values_table)
                    print("2.1. Table 데이터 INSERT 완료!!!!! ")

                # MANAGE_PHYSICAL_TABLE 의 ID 추출
                cur.execute(table_select, values_table_select)
                check_table_result = cur.fetchone()

                table_id = check_table_result[0]
                
                # Column 정보가 없을 경우에만 Column 데이터 입력
                column_select = """ SELECT ID, DATA_PHYSICAL_ID, LOGICAL_COLUMN_KOREAN, PHYSICAL_COLUMN_NAME, PHYSICAL_COLUMN_TYPE
                                        FROM AE_MANAGE_PHYSICAL_COLUMN
                                        WHERE DATA_PHYSICAL_ID = ?
                                """
                values_column_select = (table_id, )
                cur.execute(column_select, values_column_select)
                column_result = cur.fetchone()
                
                if column_result == None:
                    # 2.2. MANAGE_PHYSICAL_COLUMN 컬럼 데이터 INSERT
                    print("2.2. MANAGE_PHYSICAL_COLUMN 컬럼 데이터 INSERT")

                    column_list = data_list[0]
                    column_order = 1
                
                    type_sample_data = data_list[1]

                    for ko_col, s_data in zip(column_list, type_sample_data):
                        physical_column_type = 'NUMBER'

                        try:
                            int(s_data)
                        except ValueError:
                            physical_column_type = 'VARCHAR'
                        
                        logical_column_en = 'DATA_COL_' + str(column_order).rjust(3, '0')
                        physical_column_name = 'COL_' + str(column_order).rjust(3, '0')
                        
                        insert_col_sql = """ INSERT INTO AE_MANAGE_PHYSICAL_COLUMN (ID, DATA_PHYSICAL_ID, LOGICAL_COLUMN_KOREAN, LOGICAL_COLUMN_ENGLISH, PHYSICAL_COLUMN_NAME,
                                                                                    PHYSICAL_COLUMN_TYPE, PHYSICAL_COLUMN_ORDER, IS_CREATED_YN, IS_USE_YN, COLUMN_CREATE_DATE)
                                            VALUES (AE_SEQ_MCOLUMN.NEXTVAL, ?, ?, ?, ?, ?, ? ,?, ?, SYSTIMESTAMP)                        
                                        """
                        values_col = (table_id, ko_col, logical_column_en, physical_column_name, physical_column_type, column_order, 'N', 'Y')
                        cur.execute(insert_col_sql, values_col)
                        column_order += 1
                        
                        print("MANAGE_PHYSICAL_COLUMN 데이터 INSERT 완료!!")

                # 3. TMP 테이블 Create
                print("===================================================================================")
                print(f'TMP 테이블 Name = {orig_table_name}')
                
                # 테이블 생성 유무 확인
                check_real_table_sql = """ SELECT COUNT(*) AS CNT
                                                FROM ALL_TABLES
                                            WHERE TABLE_NAME = ?
                                        """
                values_real_table = (orig_table_name, )
                cur.execute(check_real_table_sql, values_real_table)
                table_cnt_result = cur.fetchone()

                # 테이블이 없을 경우 테이블 CREATE SQL 실행
                if table_cnt_result[0] == 0:
                    print("테이블이 없을 경우 테이블 CREATE SQL 실행 준비~~~!!")

                    # 3.1. 컬럼 정보 확인
                    cur.execute(column_select, values_column_select)
                    check_column_result = cur.fetchall()
                    
                    define_col_list = []

                    for o_col in check_column_result:
                        phy_col = o_col[3]
                        phy_type = o_col[4]
                        
                        real_type = 'VARCHAR(65532)'

                        """
                        if phy_type == 'NUMBER':
                            real_type = 'NUMBER'
                        """

                        one_define = ", " + phy_col + " " + real_type
                        define_col_list.append(one_define)

                    col_data = ''.join(define_col_list)
                    create_orig_sql = f" CREATE TABLE {orig_table_name} (ID NUMBER{col_data}) """
                    cur.execute(create_orig_sql)
                    print("테이블 생성 완료!!!!")
                
                ## 4. DATA 테이블에 데이터 INSERT
                # 데이터가 있는 경우 한 번 삭제 후 데이터 다시 입력 하는 구조
                # 데이터 한 번 삭제                
                delete_data_sql = "DELETE FROM " + orig_table_name + " WHERE 1 = 1"
                cur.execute(delete_data_sql)

                insert_data = data_list[1:]
                
                nl_id = 1
                if len_rows > 0:
                    # INSERT 쿼리 조합
                    temp_list = insert_data[0]
                    len_temp = len(temp_list)
                    
                    str_question = ''
                    for i in range(0, len_temp + 1):
                        str_question += '?,'                    
                    str_question = str_question[:-1]
                    
                    cur.execute(column_select, values_column_select)
                    insert_column_result = cur.fetchall()

                    str_col_list = []
                    for one_i in insert_column_result:
                        i_col = one_i[3]
                        str_col_list.append(i_col)

                    str_columns = ', '.join(str_col_list)
                    str_columns = 'ID, ' + str_columns

                    csv_data_insert_sql = "INSERT INTO " + orig_table_name + " (" + str_columns + ") " +\
                                          "VALUES (" + str_question + ")"

                    for one_data in insert_data:                        
                        temp_data = one_data
                        temp_data.insert(0, nl_id)
                        tuple_data = tuple(temp_data,)
                        
                        cur.execute(csv_data_insert_sql, tuple_data)                        
                        nl_id += 1                        

                    ## 5. UPDATE
                    ## DATA_BASIC_INFO 테이블 is_collect_yn N -> A 으로 UPDATE
                    update_basic_sql = """ UPDATE AE_DATA_BASIC_INFO 
                                              SET IS_COLLECT_YN = ?
                                            WHERE id = ?
                                       """
                    values_update_basic = ('A', data_basic_id)
                    cur.execute(update_basic_sql, values_update_basic)

                    ## MANAGE_PHYSICAL_TABLE 테이블 data_inserted_yn N -> Y, data_insert_row None -> len_rows UPDATE 
                    update_table_sql = """ UPDATE AE_MANAGE_PHYSICAL_TABLE
                                              SET DATA_INSERTED_YN = ?,
                                                  PHYSICAL_CREATED_YN = ?,
                                                  DATA_INSERT_ROW = ?,
                                                  TABLE_CREATE_DATE = SYSTIMESTAMP,
                                                  DATA_INSERT_DATE = SYSTIMESTAMP
                                            WHERE ID = ?
                                       """
                    valeus_update_table = ('Y', 'Y', len_rows, table_id)
                    cur.execute(update_table_sql, valeus_update_table)

                    ## MANAGE_PHYSICAL_COLUMN 테이블 is_created_yn N -> Y 으로 UPDATE
                    update_column_sql = """ UPDATE AE_MANAGE_PHYSICAL_COLUMN
                                               SET IS_CREATED_YN = ?
                                             WHERE DATA_PHYSICAL_ID = ?
                                        """
                    valeus_update_column = ('Y', table_id)
                    cur.execute(update_column_sql, valeus_update_column)

                    print("--- DATA STATE UPDATE--------------------------------")

                shutil.move(full_path, complete_path)
                time.sleep(5)
            else:
                shutil.move(full_path, complete_path)
                time.sleep(5)

            cur.close()
            del cur
            gc.collect()
            print('---------------------------------------')

        print("===================================================================================")
        
    conn.close()
    del conn
    print("===================================================================================")