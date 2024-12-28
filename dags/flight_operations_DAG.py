from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
import time
import os
import pandas as pd

# 한국 시간 설정
KST = pendulum.timezone("Asia/Seoul")

# DAG 기본 설정
default_args = {
    'owner': 'nykim',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 24, tzinfo=KST),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def setup_chrome_driver():
    """EC2용 Chrome WebDriver 설정"""
    try:
        # Chrome Driver 자동 설치 및 경로 획득
        chrome_driver_path = chromedriver_autoinstaller.install()
        print(f"ChromeDriver 설치 완료: {chrome_driver_path}")
        
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # headless 모드
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-extensions')
        
        # Chrome 버전 출력
        chrome_version = chromedriver_autoinstaller.get_chrome_version()
        print(f"Chrome Version: {chrome_version}")
        
        # Service 객체 생성 및 WebDriver 초기화
        service = Service(executable_path=chrome_driver_path)
        driver = webdriver.Chrome(service=service, options=options)
        
        # 타임아웃 설정
        driver.set_page_load_timeout(300)
        driver.implicitly_wait(30)
        
        return driver
        
    except Exception as e:
        print(f"Chrome Driver 설정 중 에러 발생: {str(e)}")
        raise

def select_radio_button(driver, wait, button_type):
    """출발/도착 라디오 버튼 선택"""
    try:
        time.sleep(2)
        value = "D" if button_type == "출발" else "A"
        radio_buttons = driver.find_elements(By.CSS_SELECTOR, "div.iradio_square-green")
        
        for radio in radio_buttons:
            try:
                input_element = radio.find_element(By.XPATH, f".//input[@value='{value}']")
                if 'checked' not in radio.get_attribute('class'):
                    driver.execute_script("arguments[0].click();", radio)
                    time.sleep(1)
                    print(f"{button_type} 라디오 버튼 선택")
                return True
            except:
                continue
        
        return False
        
    except Exception as e:
        print(f"{button_type} 라디오 버튼 선택 중 에러: {str(e)}")
        return False

def input_date_with_retry(driver, input_element, date_value):
    """날짜 입력 재시도 함수"""
    max_attempts = 3
    for attempt in range(max_attempts):
        input_element.clear()
        time.sleep(0.5)
        driver.execute_script(f"arguments[0].value = '{date_value}'", input_element)
        time.sleep(0.5)
        
        current_value = input_element.get_attribute('value')
        if current_value == date_value:
            break
        
        print(f"날짜 입력 재시도 {attempt + 1}/{max_attempts}")
        time.sleep(1)

def download_daily_data(target_date, data_type, download_path):
    """일일 데이터 다운로드"""
    driver = None
    try:
        driver = setup_chrome_driver()
        wait = WebDriverWait(driver, 120)
        
        print(f"네트워크 상태 확인: {data_type} 데이터 다운로드 시작")
        print("1. 메인 페이지 접속 중...")
        driver.get("https://www.airportal.go.kr/life/airinfo/RbHanFrmMain.jsp")
        
        print("2. 로그인 버튼 클릭...")
        login_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//a[text()='로그인']"))
        )
        login_button.click()
        
        print("3. 팝업창으로 전환 중...")
        wait.until(lambda d: len(d.window_handles) > 1)
        main_window = driver.current_window_handle
        popup_window = [handle for handle in driver.window_handles if handle != main_window][0]
        driver.switch_to.window(popup_window)
        
        print("4. 로그인 정보 입력 중...")
        username_input = wait.until(
            EC.presence_of_element_located((By.NAME, "df_userid"))
        )
        password_input = driver.find_element(By.NAME, "df_passwd")
        
        username_input.send_keys("{{ var.value.airportal_username }}")
        password_input.send_keys("{{ var.value.airportal_password }}")
        
        print("5. 로그인 시도...")
        login_submit = driver.find_element(By.CSS_SELECTOR, "input[type='image'][src='img/btn_login1.jpg']")
        login_submit.click()
        
        print("6. 메인 창으로 복귀...")
        driver.switch_to.window(main_window)
        time.sleep(3)
        
        print("7. 데이터 다운로드 페이지로 이동 중...")
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                driver.set_page_load_timeout(600)  # 타임아웃 시간 10분으로 증가
                driver.get("https://www.airportal.go.kr/life/airinfo/FlightScheduleToExcel.jsp")
                
                # 페이지가 실제로 로드되었는지 확인
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.iradio_square-green")))
                time.sleep(5)  # 추가 대기 시간
                print("데이터 다운로드 페이지 로드 성공")
                break
            except Exception as e:
                print(f"페이지 로드 시도 {attempt + 1}/{max_attempts} 실패: {str(e)}")
                if attempt == max_attempts - 1:
                    raise Exception("데이터 다운로드 페이지 로드 실패") from e
                time.sleep(10)  # 재시도 전 대기
        
        if not select_radio_button(driver, wait, data_type):
            raise Exception(f"{data_type} 버튼 선택 실패")
        
        date_str = target_date.strftime('%Y%m%d')
        
        start_date_input = wait.until(EC.presence_of_element_located((By.NAME, "sDate")))
        input_date_with_retry(driver, start_date_input, date_str)
        
        end_date_input = driver.find_element(By.NAME, "eDate")
        input_date_with_retry(driver, end_date_input, date_str)
        
        time.sleep(1)
        
        search_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.mainSearchBtn"))
        )
        driver.execute_script("arguments[0].click();", search_button)
        
        print("검색 결과 로드 중...")
        time.sleep(30)
        
        download_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.mainExcellBtn"))
        )
        driver.execute_script("arguments[0].click();", download_button)
        
        print("다운로드 진행 중...")
        time.sleep(300)
        
        print(f"{target_date.strftime('%Y-%m-%d')} {data_type} 데이터 다운로드 완료")
        
        # 다운로드된 파일명 변경
        time.sleep(5)  # 파일 다운로드 완료 대기
        files = os.listdir(download_path)
        excel_files = [f for f in files if f.startswith('항공기출도착현황')]
        
        for file in excel_files:
            if '출발' not in file and '도착' not in file:  # 아직 이름이 변경되지 않은 파일만
                old_path = os.path.join(download_path, file)
                new_filename = f"항공기출도착현황_{data_type}.xlsx"
                new_path = os.path.join(download_path, new_filename)
                os.rename(old_path, new_path)
                print(f"파일명 변경: {file} -> {new_filename}")
                break
                
        return True
        
    except Exception as e:
        print(f"데이터 다운로드 중 에러: {str(e)}")
        raise e
    finally:
        if driver:
            driver.quit()

def save_to_s3(**context):
    """S3 업로드 함수"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'team5-s3'
    download_path = '/home/ubuntu/airflow/data'  # EC2 환경의 경로
    
    execution_date = context['execution_date']
    target_date = execution_date.in_timezone(KST) - timedelta(days=1)
    date_str = target_date.strftime('%Y%m%d')
    
    try:
        # 이름이 변경된 파일 찾기
        files = [f for f in os.listdir(download_path) if f.startswith('항공기출도착현황_')]
        
        if not files:
            raise FileNotFoundError("처리할 파일을 찾을 수 없습니다")
        
        for file in files:
            local_path = os.path.join(download_path, file)
            # 파일명에서 출발/도착 구분 (_출발.xlsx 또는 _도착.xlsx)
            operation_type = file.split('_')[1].split('.')[0]  # "출발" 또는 "도착" 추출
            
            # S3 키 생성
            s3_key = f'raw_data/flight_operations/flight_operations_{date_str}_{operation_type}.xlsx'
            
            # S3에 업로드
            s3_hook.load_file(
                filename=local_path,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"S3 업로드 완료: {s3_key}")
            
            # 로컬 파일 삭제
            os.remove(local_path)
            
    except Exception as e:
        print(f"파일 처리 중 에러: {str(e)}")
        raise

with DAG(
    'flight_operations_data_collection',
    default_args=default_args,
    description='매일 전날의 항공운항 데이터 수집',
    schedule_interval='0 4 * * *', # UCT 4시 = KST 13시
    #timezone = 'Asia/Seoul',
    tags=['flight_operations'],
    catchup=True,
) as dag:
    def download_task_function(**context):
        execution_date = context['execution_date']
        target_date = execution_date.in_timezone(KST) - timedelta(days=1)
        download_path = '/var/lib/airflow/data'  # EC2 환경의 경로
        
        for data_type in ["출발", "도착"]:
            download_daily_data(target_date, data_type, download_path)
    
    download_task = PythonOperator(
        task_id='download_daily_data',
        python_callable=download_task_function,
        provide_context=True,
    )
    
    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=save_to_s3,
        provide_context=True,
    )
    
    download_task >> upload_to_s3_task