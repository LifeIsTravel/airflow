import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from datetime import datetime, timedelta
import pandas as pd
import time
import os
import io
import glob
import configparser
import logging
import boto3
import pyarrow
import pyarrow.parquet as pq

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_credentials(config_file='config.ini'):
    """설정 파일에서 로그인 정보 로드"""
    if not os.path.exists(config_file):
        with open(config_file, 'w') as f:
            f.write("""[Credentials]
    username = your_username
    password = your_password""")
        raise FileNotFoundError(f"Please fill in your credentials in {config_file}")
    
    config = configparser.ConfigParser()
    config.read(config_file)
    return (
        config.get('Credentials', 'username'),
        config.get('Credentials', 'password')
    )

def setup_driver():
    """Chrome 웹드라이버 설정"""
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory": os.getcwd(),
        "download.prompt_for_download": False,
    })
    return webdriver.Chrome(options=options)

def select_radio_button(driver, wait, button_type):
    """출발/도착 라디오 버튼 선택"""
    try:
        # JavaScript 초기화를 위해 대기
        time.sleep(2)
        
        # 버튼 타입에 따라 value 설정
        value = "D" if button_type == "출발" else "A"
        
        # radio button wrapper 찾기 (더 정확한 선택자 사용)
        radio_buttons = driver.find_elements(By.CSS_SELECTOR, "div.iradio_square-green")
        
        for radio in radio_buttons:
            # radio 내부의 input 태그를 찾기 전에 먼저 radio의 HTML 확인
            print(f"Radio button HTML: {radio.get_attribute('outerHTML')}")
            
            try:
                input_element = radio.find_element(By.XPATH, f".//input[@value='{value}']")
                print(f"Found radio button for {button_type}")
                
                if 'checked' not in radio.get_attribute('class'):
                    print(f"Clicking {button_type} radio button")
                    driver.execute_script("arguments[0].click();", radio)
                    time.sleep(1)
                return True
            except:
                continue
                
        print(f"{button_type} 라디오 버튼을 찾을 수 없습니다.")
        return False
        
    except Exception as e:
        print(f"{button_type} 라디오 버튼 선택 중 에러: {str(e)}")
        return False
    
def input_date_with_retry(driver, input_element, date_value):
    """날짜 입력을 재시도하는 함수"""
    max_attempts = 3
    for attempt in range(max_attempts):
        # 값 지우기
        input_element.clear()
        time.sleep(0.5)
        
        # 새 값 입력
        driver.execute_script(f"arguments[0].value = '{date_value}'", input_element)
        time.sleep(0.5)
        
        # 현재 값 확인
        current_value = input_element.get_attribute('value')
        if current_value == date_value:
            break
        
        print(f"날짜 입력 재시도 {attempt + 1}/{max_attempts}")
        time.sleep(1)

def download_monthly_data(driver, wait, start_date, end_limit_date, data_type):
    """월별 데이터 다운로드"""
    print(f"\n{start_date.strftime('%Y-%m')} {data_type} 데이터 다운로드 시작...")
    
    # 해당 월의 마지막일 계산
    month_end = (start_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    actual_end_date = min(month_end, end_limit_date)
    
    print(f"수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {actual_end_date.strftime('%Y-%m-%d')}")
    
    # 출발/도착 라디오 버튼 선택
    if not select_radio_button(driver, wait, data_type):
        raise Exception(f"{data_type} 버튼 선택 실패")
    
    try:
        # 날짜 입력 필드가 완전히 로드될 때까지 대기
        wait.until(EC.presence_of_element_located((By.NAME, "sDate")))
        
        # 시작일 입력 (재시도 로직 사용)
        start_date_input = driver.find_element(By.NAME, "sDate")
        start_date_str = start_date.strftime('%Y%m%d')
        input_date_with_retry(driver, start_date_input, start_date_str)
        
        # 종료일 입력 (재시도 로직 사용)
        end_date_input = driver.find_element(By.NAME, "eDate")
        end_date_str = actual_end_date.strftime('%Y%m%d')
        input_date_with_retry(driver, end_date_input, end_date_str)
        
        time.sleep(1)
        
        # 검색 버튼 클릭
        search_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.mainSearchBtn"))
        )
        driver.execute_script("arguments[0].click();", search_button)
        
        # 검색 결과 로드 대기
        print("검색 결과 로드 중...")
        time.sleep(5)  # 검색 결과 로드 대기
        
        # 다운로드 버튼 클릭
        download_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a.mainExcellBtn"))
        )
        driver.execute_script("arguments[0].click();", download_button)
        
        # 다운로드 완료 대기
        print("다운로드 진행 중...")
        time.sleep(10)
        
        print(f"{start_date.strftime('%Y-%m')} {data_type} 데이터 다운로드 완료")
        return True
        
    except Exception as e:
        print(f"데이터 다운로드 중 에러: {str(e)}")
        raise e

def upload_to_s3(data, bucket, s3_file, is_buffer=False):
    """
    AWS 자격증명 파일의 Team5 프로필을 사용하여 S3에 파일 업로드
    
    :param local_file: 로컬 파일 경로
    :param bucket: S3 버킷 이름
    :param s3_file: S3에 저장될 파일 경로
    :return: 업로드 성공 여부
    """
    # Team5로 저장한 프로필 사용
    session = boto3.Session(profile_name='Team5')
    s3 = session.client('s3')
    
    try:
        if is_buffer:
            s3.put_object(Body=data, Bucket=bucket, Key=s3_file)
        else:
            s3.upload_file(data, bucket, s3_file)
        print(f"업로드 성공: s3://{bucket}/{s3_file}")
        return True    

    except Exception as e:
        print(f"업로드 중 에러 발생: {e}")
        return False
    
def combine_excel_files(download_path, data_type, s3_bucket='team5-s3'):
    print("\n엑셀 파일 통합 시작...")
    
    try:
        # 파일명 패턴으로 파일 찾기
        all_files = glob.glob(os.path.join(download_path, "항공기출도착현황*.xlsx"))
        if not all_files:
            print("통합할 엑셀 파일이 없습니다.")
            return False
        
        print(f"총 {len(all_files)}개의 파일을 찾았습니다.")
        combined_df = pd.DataFrame()
        
        # 파일 다운로드 시간 기준으로 정렬
        sorted_files = sorted(all_files, key=os.path.getctime)

        
        for file in sorted_files:
            try:
                print(f"처리 중: {os.path.basename(file)}")
                df = pd.read_excel(file)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                print(f"데이터 통합 완료")

                # 처리 완료된 파일 삭제 
                os.remove(file)
            except Exception as e:
                print(f"{file} 처리 중 에러 발생: {str(e)}")
        

        if not combined_df.empty:
            table = pyarrow.Table.from_pandas(combined_df)

            # 메모리 버퍼에 Parquet 형식으로 쓰기
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
        
            # S3에 업로드
            s3_file_path = f"raw_data/flight_operations/flight_operations_{data_type}_20240101_to_20241222.parquet"
            upload_to_s3(buffer.getvalue(), s3_bucket, s3_file_path, is_buffer=True)
            print("S3 업로드 완료")
            return True
    except Exception as e:
        print(f"파일 통합 중 에러 발생: {str(e)}")
        return False

def test_login_and_download(download_path=r"D:/Downloads"):
    driver = webdriver.Chrome()
    wait = WebDriverWait(driver, 20)
    
    try:
        # 자격증명 로드
        username, password = load_credentials()
        
        # 메인 페이지 접속 및 로그인
        print("1. 메인 페이지 접속 중...")
        driver.get("https://www.airportal.go.kr/life/airinfo/RbHanFrmMain.jsp")
        
        # 로그인 과정 (이전과 동일)...
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
        
        username_input.send_keys(username)
        password_input.send_keys(password)
        
        print("5. 로그인 시도...")
        login_submit = driver.find_element(By.CSS_SELECTOR, "input[type='image'][src='img/btn_login1.jpg']")
        login_submit.click()
        
        print("6. 메인 창으로 복귀...")
        driver.switch_to.window(main_window)
        time.sleep(3)
        
        print("7. 데이터 다운로드 페이지로 이동 중...")
        driver.get("https://www.airportal.go.kr/life/airinfo/FlightScheduleToExcel.jsp#")
        time.sleep(3)
        
        # 다운로드 기간 설정
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 22)
        
        print(f"\n데이터 수집 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
        
        # 출발, 도착 데이터 각각 다운로드
        for data_type in ["출발", "도착"]:
            current_date = start_date
            while current_date <= end_date:
                try:
                    download_monthly_data(driver, wait, current_date, end_date, data_type)
                    current_date = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)
                    time.sleep(3)
                except Exception as e:
                    print(f"{current_date.strftime('%Y-%m')} {data_type} 다운로드 중 에러: {str(e)}")
                    current_date = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)
        
            # 다운로드된 파일들을 통합 및 S3에 저장
            combine_excel_files(download_path, data_type)
        
        print("\n모든 작업이 완료되었습니다.")
        print("통합 파일을 확인해주세요.")
        input("브라우저를 종료하려면 Enter 키를 누르세요...")
        
    except Exception as e:
        print(f"에러 발생: {str(e)}")
    finally:
        print("브라우저를 종료합니다...")
        if driver:
            driver.quit()

if __name__ == "__main__":
    test_login_and_download()