import os
from selenium.webdriver import Chrome, ChromeOptions
import time
import pandas as pd
import datetime
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import urllib.parse
import threading
from multiprocessing import Process, Queue

LOG_FILE_PATH = "./log/log_{datetime}.log"
EXP_CSV_PATH="./exp_list_{search_keyword}_{datetime}.csv"
MAX_THREAD_NUM = 5 # 最大マルチスレッド数を指定(多くすると処理が重くなるため、PCのスペックにより調整)
NOW = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
log_file_path = LOG_FILE_PATH.format(datetime=datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))

### Chromeを起動する関数
def set_driver(headless_flg):
    # Chromeドライバーの読み込み
    options = ChromeOptions()

    # ヘッドレスモード（画面非表示モード）をの設定　
    if headless_flg == True:
        options.add_argument('--headless')

    # 起動オプションの設定
    options.add_argument(
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36')
    # options.add_argument('log-level=3')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--incognito')          # シークレットモードの設定を付　与

    # ChromeのWebDriverオブジェクトを作成する。
    return Chrome(ChromeDriverManager().install(), options=options)

### ログファイル及びコンソール出力
def log(txt):
    now=datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    logStr = '[%s: %s] %s' % ('log',now , txt)
    # ログ出力
    with open(log_file_path, 'a', encoding='utf-8_sig') as f:
        f.write(logStr + '\n')
    print(logStr)

def find_table_target_word(th_elms, td_elms, target:str):
    # tableのthからtargetの文字列を探し一致する行のtdを返す
    for th_elm,td_elm in zip(th_elms,td_elms):
        if th_elm.text == target:
            return td_elm.text

def make_search_url(search_keyword:str):
     # キーワード調整
    adjust_search_keyword = " "
    for key in search_keyword.split():
        adjust_search_keyword += "kw{}_".format(key)
    adjust_search_keyword = adjust_search_keyword.rstrip("_").lstrip(" ")
    # エンコードurlを返す
    return  urllib.parse.quote(adjust_search_keyword)

def get_search_info(id, queue, search_keyword):
    # driverを起動
    if os.name == 'nt': #Windows
        driver = set_driver(False)
    elif os.name == 'posix': #Mac
        driver = set_driver(False)

    # 検索URL作成
    url = "https://tenshoku.mynavi.jp/list/{0}".format(make_search_url(search_keyword))
    # Webサイトを開く
    driver.get(url)
    time.sleep(5)

    try:
        # ポップアップを閉じる
        driver.execute_script('document.querySelector(".karte-close").click()')
        time.sleep(5)
        # ポップアップを閉じる
        driver.execute_script('document.querySelector(".karte-close").click()')
    except:
        pass
    time.sleep(5)

    exp_name_list = []
    exp_annual_income_list = []

    # 2ページ(2スレッド)目以降ページ数分次へボタンを押下してページ移動
    if(id != 0):
      for page in range(id):
        next_page = driver.find_elements_by_class_name("iconFont--arrowLeft")
        if len(next_page) >=1:
          next_page_link = next_page[0].get_attribute("href")
          driver.get(next_page_link)
        else:
            log("最終ページです。終了します。")
            return
        try:
            # urlで直接検索した場合、2ページ目以降にポップアップが出る事があるため閉じる
            driver.execute_script('document.querySelector(".karte-close").click()')
        except:
            pass
        time.sleep(5)

    try:
        # 会社名を取得
        name_list = driver.find_elements_by_css_selector(".cassetteRecruit__heading .cassetteRecruit__name")
        # 給与を取得
        table_list = driver.find_elements_by_css_selector(".cassetteRecruit .tableCondition")

        # 1ページ分繰り返し
        for name, table in zip(name_list, table_list):
            # 給与をテーブルから探す
            annual_income = find_table_target_word(table.find_elements_by_tag_name("th"), table.find_elements_by_tag_name("td"), "給与")
            exp_name_list.append(name.text)
            exp_annual_income_list.append(annual_income)
            log(f"{id}件目成功 : {name.text}")
    except Exception as e:
        log(f"{id}件目失敗 : {name.text}")
        log(e)
        driver.close()

    # csv出力
    df = pd.DataFrame({"企業名":exp_name_list, "給与":exp_annual_income_list})
    queue.put(df)

### main処理
def main():
    log("処理開始")
    queue = Queue()
    thread_list = []
    search_keyword = input("検索キーワードを入力してください")
    thred_num = input("スレッド数を1~5で入力してください。")

    while int(thred_num) < 1 or int(thred_num) > MAX_THREAD_NUM:
      thred_num = input("スレッド数を1~5で入力してください。")

    log("検索キーワード:{}".format(search_keyword))
    time_before = time.time()

    # マルチスレッド処理
    for id in range(int(thred_num)):
        t = threading.Thread(target=get_search_info, args=[id, queue, search_keyword])
        t.start()
        thread_list.append(t)

    # 全スレッドの終了を待つ
    for thread in thread_list:
        thread.join()

    # csvへ書き込み
    while not queue.empty():
      queue.get().to_csv(EXP_CSV_PATH.format(search_keyword=search_keyword,datetime=
                                NOW), mode='a', header=False, encoding="utf-8-sig")
    time_after = time.time()
    time_elapsed = time_after - time_before
    print("終了")
    print(f"処理の所要時間{time_elapsed}")

# 直接起動された場合はmain()を起動(モジュールとして呼び出された場合は起動しないようにするため)
if __name__ == "__main__":
    main()