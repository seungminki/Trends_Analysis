from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import time
import pandas as pd

chrome_options = Options()
chrome_options.add_argument("User_Agent:Mozilla/5.0")
# chrome_options.add_argument('headless')
chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
chrome_options.add_argument('--window-size=360,640')
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.add_argument('incognito')

def crawler_main(topic):
    
    url = f"https://search.naver.com/search.naver?query=%EB%B0%80%ED%81%AC%ED%8B%B0%20%EC%B4%88{topic}&nso=&where=article&sm=tab_opt"
    # 데이터 갯수가 너무 많을 때 몇개까지 뽑을 건지 정하기
    end_num = 10

    # 검색 결과가 나올때까지 대기
    driver = webdriver.Chrome(executable_path='crawler/chromedriver.exe', options=chrome_options)
    driver.get(url)
    WebDriverWait(driver, 2).until(
        EC.presence_of_element_located((By.ID, "container"))
    )

    scroll_location = driver.execute_script("return document.body.scrollHeight")

    while True:
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        
        # driver.implicitly_wait(2)
        time.sleep(0.5)
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        
        # 원하는 개수만큼
        if len(driver.find_elements("xpath", f'/html/body/div[3]/div[2]/div/div[1]/section/html-persist/div/more-contents/div/ul/li')) >= end_num:
            break
            
        # 스크롤 끝까지                                       
        elif scroll_location == scroll_height:
            print(len(driver.find_elements("xpath", f'/html/body/div[3]/div[2]/div/div[1]/section/html-persist/div/more-contents/div/ul/li')))
            end_num = len(driver.find_elements("xpath", f'/html/body/div[3]/div[2]/div/div[1]/section/html-persist/div/more-contents/div/ul/li'))
            break

        else:
            scroll_location = driver.execute_script("return document.body.scrollHeight")

    for i in range(1, end_num+1):
        first_page = driver.find_element("xpath", f'/html/body/div[3]/div[2]/div/div[1]/section/html-persist/div/more-contents/div/ul/li[{i}]/div[1]/div/a')
        first_page.send_keys(Keys.ENTER)
        driver.switch_to.window( driver.window_handles[-1] )
        time.sleep(1)
        driver.switch_to.frame("cafe_main")
        driver.implicitly_wait(2)
        try:
            second_text = driver.find_element("xpath", '//*[@id="app"]/div/div/div[2]/div[2]/div[1]/div/div[1]').text
            date = driver.find_element(By.CLASS_NAME, "date").text
            click_count = driver.find_element(By.CLASS_NAME, "count").text
            # <div class="content Cateviewer">

        except NoSuchElementException as e:
            pass
            print(e)
            print("get out=============")

        else:
            print(i)
            if i == 1:
                df = pd.DataFrame({'text':[second_text], 'date':[date], 'click':[click_count]})

            else:
                new_data = pd.DataFrame({'text':[second_text], 'date':[date], 'click':[click_count]})
                df = pd.concat([df, new_data], ignore_index=True)
            
            driver.close()

        finally:
            driver.switch_to.window( driver.window_handles[0] )
            time.sleep(2)

    df.to_parquet(f"crawler/parquet/test{topic}.parquet", engine="pyarrow")
    driver.quit()
if __name__ == '__main__':
    import multiprocessing

    start = time.time()
    pages = [1, 2, 3]
    # 초1, 초2, 초3
    procs = []
    for page in pages:
        p = multiprocessing.Process(target=crawler_main, args=(page, ))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()  # 프로세스가 모두 종료될 때까지 대기

    end = time.time()

    print("수행시간: %f 초" % (end - start))

'''
# 댓글 따오는 코드
comment_num = len(driver.find_elements(By.CLASS_NAME, "CommentItem"))
for i in range(1, comment_num+1):
    # comment = driver.find_element(By.CLASS_NAME, "comment_text_box").text
    comment = driver.find_element("xpath", f'//*[@id="app"]/div/div/div[2]/div[2]/div[4]/ul/li[{i}]/div/div/div[2]').text
    print(comment)
'''