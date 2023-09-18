
import time
import os
import gc
import shutil
from selenium import webdriver

from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


import jaydebeapi

if __name__ == "__main__":

    print("Data.go.kr 크롤링 (CSV 파일 다운로드 및 DATA_BASIC_INFO data Insert) ===================")
    print(os.getcwd())
        
    # 크롬드라이버 다운로드 후, path 설정
    executable_path = './driver/chromedriver.exe'
    # csv 파일 다운로드 위치
    current_file_path = os.path.join(os.getcwd(), 'downloads')
    # csv 파일 이동 dir 위치
    destination_path = os.path.join(os.getcwd(), 'csv_data')

    options = webdriver.ChromeOptions()
    # options.add_argument('headless')
    options.add_experimental_option("prefs", {
        "download.default_directory": current_file_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(executable_path=executable_path, chrome_options=options)

    # csv 파일 다운로드 url
    # 공공데이터 포털 확장자 csv 선택 후 검색 -> 파일데이터 선택 -> 페이지 선택(현재 1) -> url 복사 후 설정
    start_num = 1

    url = 'https://www.data.go.kr/tcs/dss/selectDataSetList.do?' \
          'dType=FILE&keyword=&detailKeyword=&publicDataPk=&'\
          'recmSe=&detailText=&relatedKeyword=&commaNotInData=&'\
          'commaAndData=&commaOrData=&must_not=&tabId=&'\
          'dataSetCoreTf=&coreDataNm=&sort=&relRadio=&'\
          'orgFullName=&orgFilter=&org=&orgSearch=&currentPage=1&'\
          'perPage=10&brm=&instt=&svcType=&kwrdArray=&'\
          'extsn=CSV&coreDataNmArray=&pblonsipScopeCode='

    driver.get(url)

    total_count = driver.find_element(By.XPATH, f'//*[@id="mainTotalCnt"]').text
    total_count = int(total_count.replace(',', ''))
    
    for_cnt = total_count // 10 + 1
    
    time.sleep(5)

    conn = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@127.0.0.1:1234:tibero",
        ["root", "1234"],
        "tibero6-jdbc.jar",
    )
    
    for page_num in range(1717, for_cnt + 1):
        time.sleep(1.5)
        url = 'https://www.data.go.kr/tcs/dss/selectDataSetList.do?' \
          'dType=FILE&keyword=&detailKeyword=&publicDataPk=&'\
          'recmSe=&detailText=&relatedKeyword=&commaNotInData=&'\
          'commaAndData=&commaOrData=&must_not=&tabId=&'\
          'dataSetCoreTf=&coreDataNm=&sort=&relRadio=&'\
          'orgFullName=&orgFilter=&org=&orgSearch=&currentPage=' + str(page_num) + '&'\
          'perPage=10&brm=&instt=&svcType=&kwrdArray=&'\
          'extsn=CSV&coreDataNmArray=&pblonsipScopeCode='

        driver.get(url)

        for i in range(1, 11):
            time.sleep(0.1)

            try:
                # 1.CSV 파일 다운로드
                driver.find_element(By.XPATH, f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/div[2]/a').send_keys(Keys.ENTER)

                # 2.DATA_BASIC_INFO 테이블 필요 데이터 search
                category = driver.find_element(By.XPATH, f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/p/span[1]').text
                description = driver.find_element(By.XPATH, f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dd').text.replace("'","")
                data_name = driver.find_element(By.XPATH, f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dt/a/span[@class="title"]').text
                url = driver.find_element(By.XPATH, f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dt/a').get_attribute('href')
                org_key = url.split('/')[-2]

            except NoSuchElementException:
                continue

            time.sleep(1.1)
            driver.implicitly_wait(10)           # 암묵적 대기 10s

            cur = conn.cursor()
            
            time.sleep(0.5)

            # 3.다운로드한 데이터의 data_origin_key를 기존 AE_DATA_BASIC_INFO 테이블 내 데이터와 비교
            inserted_check_sql = "SELECT ID FROM AE_DATA_BASIC_INFO WHERE DATA_ORIGIN_KEY = ?"
            check_values = (org_key, ) 
            cur.execute(inserted_check_sql, check_values)
            inserted_check = cur.fetchall()

            time.sleep(1.1)

            # DATA_BASIC_INFO 테이블에 data_origin_key가 없으면 insert
            if not inserted_check:
                insert_sql = """ INSERT INTO AE_DATA_BASIC_INFO(ID, COLLECT_SITE_ID, CATEGORY_BIG, CATEGORY_SMALL, DATA_NAME, DATA_DESCRIPTION, 
                                                                PROVIDE_DATA_TYPE, PROVIDE_URL_LINK, COLLECT_DATA_TYPE, COLLECT_URL_LINK, IS_COLLECT_YN, DATA_ORIGIN_KEY) 
                                 VALUES(AE_SEQ_BASIC.NEXTVAL, 3, '공공데이터', ?, ?, ?, 'File', ?, 'File', ?, 'N', ?)
                             """
                values_insert = (category, data_name, description, url, url, org_key)
                cur.execute(insert_sql, values_insert)
                
                # 4.다운로드, insert가 완료된 파일을 1_1_data_public_read_csv.py에 사용 가능하도록 이동
                time.sleep(30.5)
                file_list = os.listdir(current_file_path)

                for file in file_list:
                    shutil.move(f'{current_file_path}/{file}', destination_path) if file.startswith(data_name) else None

            cur.close()
            del cur
            gc.collect()

            time.sleep(1.1)

        print('==========================================================================')
        print('======= page_num = ' + str(page_num) + ' <<<<<<< (SUCCESS) =======')
        print('==========================================================================')

        driver.implicitly_wait(5)      # 암묵적 대기 10s
    
    conn.close()
    del conn
    gc.collect()

    # 드라이버 종류
    driver.close()
