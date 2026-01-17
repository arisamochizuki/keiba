import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from tqdm import tqdm
from datetime import datetime, timedelta

# --- 設定など ---
def get_headers():
    """
    ランダムなUser-Agentを返す
    """
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    ]
    return {'User-Agent': random.choice(user_agents)}

def random_sleep(min_sec=3, max_sec=6):
    """
    サーバー負荷軽減のための待機時間
    """
    time.sleep(random.uniform(min_sec, max_sec))

# --- フェーズ1: 有効なレースIDの収集 ---

def get_race_ids_for_period(start_date_str, end_date_str):
    """
    指定期間の「開催日ごとのレース一覧ページ」にアクセスし、有効なrace_idを収集する
    start_date_str: '20240101'
    end_date_str: '20240131'
    """
    print(f"--- ID収集開始: {start_date_str} ～ {end_date_str} ---")

    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')

    collected_race_ids = []

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        url = f"https://db.netkeiba.com/race/list/{date_str}/"

        try:
            response = requests.get(url, headers=get_headers())
            response.encoding = 'EUC-JP'
            soup = BeautifulSoup(response.text, 'html.parser')

            # ページ内のリンクから /race/YYYYJJkkddRR/ の形式を探す
            # db.netkeibaのリストページでは dl.race_top_data などの中にリンクがある
            links = soup.find_all('a', href=re.compile(r"^/race/\d{12}"))

            daily_ids = []
            for link in links:
                race_id = link['href'].strip('/race/').strip('/')
                daily_ids.append(race_id)

            # 重複除去
            daily_ids = list(set(daily_ids))

            if daily_ids:
                print(f"{date_str}: {len(daily_ids)} レース発見")
                collected_race_ids.extend(daily_ids)
            else:
                # 開催がない日はここに来る（エラーではない）
                pass

        except Exception as e:
            print(f"Error fetching date list {date_str}: {e}")

        # 1日分のリスト取得ごとにも必ずスリープを入れる
        random_sleep(2, 4)
        current_date += timedelta(days=1)

    return collected_race_ids

# --- フェーズ2: 詳細データの収集（メタデータ含む） ---

def scrape_race_data(race_id):
    """
    レース詳細ページから、これまで以上に詳細な情報を取得する関数
    """
    url = f"https://db.netkeiba.com/race/{race_id}/"
    try:
        response = requests.get(url, headers=get_headers())
        response.encoding = 'EUC-JP'
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Request Error: {e}")
        return None

    # --- A. レース自体のメタデータ (変更なし) ---
    try:
        intro = soup.find('div', class_='data_intro')
        if not intro:
            return None
       
        race_name = intro.find('h1').text.strip() if intro.find('h1') else ""
        details_text = intro.find('p', class_='smalltxt').text + " " + intro.find('diary_snap_cut').text
        details_text = details_text.replace(u'\xa0', u' ')

        # 正規表現での抽出
        course_match = re.search(r'(芝|ダ|障)[左右]?[外内]?\d+m', details_text)
        course_dist = course_match.group() if course_match else ""
       
        weather_match = re.search(r'天気\s*:\s*(\S+)', details_text)
        weather = weather_match.group(1) if weather_match else ""
       
        condition_match = re.search(r'(芝|ダート)\s*:\s*(\S+)', details_text)
        condition = condition_match.group(2) if condition_match else ""
       
        race_metadata = {
            'race_id': race_id,
            'race_name': race_name,
            'course_dist': course_dist,
            'weather': weather,
            'condition': condition,
        }

    except Exception as e:
        print(f"Metadata Error in {race_id}: {e}")
        return None

    # --- B. 馬ごとの詳細成績データ (大幅強化) ---
    table = soup.find('table', class_='race_table_01')
    if table is None:
        return None

    df_rows = []
    rows = table.find_all('tr')
   
    # ヘッダー行をスキップしてデータ行を処理
    for row in rows[1:]:
        cols = row.find_all('td')
       
        # 欠損や「取消」「除外」などでカラム数が合わない場合の対策
        if len(cols) < 15:
            continue

        try:
            # カラムインデックスの定義 (netkeibaの標準レイアウトに基づく)
            # 0:着順, 1:枠番, 2:馬番, 3:馬名, 4:性齢, 5:斤量, 6:騎手, 7:タイム, 8:着差
            # 9:タイム指数(プレミアムのみ/空欄多), 10:通過, 11:上り, 12:単勝, 13:人気, 14:馬体重
            # ※レイアウトが微妙に異なるケースがあるため、柔軟に対応できる設計にします
           
            # --- 基本情報 ---
            rank = cols[0].text.strip()
            frame_number = cols[1].text.strip()  # 枠番
            horse_number = cols[2].text.strip()  # 馬番
            horse_name = cols[3].text.strip()
           
            # 性齢 (例: "牡3" -> 性別:牡, 年齢:3 に後で分けるためそのまま取得)
            age_sex = cols[4].text.strip()
           
            jockey_weight = cols[5].text.strip() # 斤量
            jockey = cols[6].text.strip()
            time_str = cols[7].text.strip()
            margin = cols[8].text.strip()        # 着差 (AI学習で重要: 僅差か大差か)
           
            # --- 列ズレ対策 ---
            # netkeibaは時期やレースによって「タイム指数」等の列が入ったり消えたりします。
            # 後ろから数えるほうが確実な場合がありますが、ここでは標準的な位置を指定し、
            # エラーにならないよう取得します。
           
            # 人気・オッズ (非常に重要)
            # 通常、cols[12]が単勝オッズ、cols[13]が人気ですが、
            # HTML構造上の変動を見越して、ここでは「単勝」「人気」のカラム位置を固定と仮定します。
            # (多くの過去データでは 12=オッズ, 13=人気 のパターンが多いですが、
            #  cols[9]付近からズレることがあるため注意が必要です)
           
            # ここでは標準的なレイアウトを想定
            # 通過順位 (コーナー通過)
            passing_rank = cols[10].text.strip()
            last_3f = cols[11].text.strip()      # 上がり3F
            odds = cols[12].text.strip()         # 単勝オッズ
            popularity = cols[13].text.strip()   # 人気
           
            # 馬体重 (例: "480(+2)")
            horse_weight_raw = cols[14].text.strip()
           
            # 調教師 (ここが `race_table_01` に含まれていない場合、
            # `cols` の数を確認する必要がありますが、通常含まれています)
            # 実は標準テーブルだと調教師情報が隠れているか、別の列にあることが多いです。
            # \n で区切られている場合があるのでテキスト全体から探す処理も有効ですが、
            # ここではシンプルにそのまま取得を試みます。
           
            # ※注: netkeibaのテーブル構造上、調教師は `cols[18]` あたりにある場合や、
            # 表示されていない場合があります。表示されている範囲で取得します。
            # HTMLソースを見ると、trainerデータは `cols` の中盤以降にあることが多いです。
            # もし `cols` が多ければ取得します。
            trainer = ""
            if len(cols) > 18:
                 # 拡張表示されている場合など
                 trainer = cols[18].text.strip()
            else:
                # 一般的なビューでは調教師が aタグ で含まれていることが多い
                # cols内のどこかに 'href="/trainer/' を含むセルがあればそれが調教師
                for c in cols:
                    if c.find('a', href=re.compile(r'/trainer/')):
                        trainer = c.text.strip()
                        break

            # --- データの整形 (馬体重の分割) ---
            # "480(+2)" -> weight: 480, weight_diff: +2
            weight = horse_weight_raw
            weight_diff = 0
            if '(' in horse_weight_raw:
                try:
                    parts = horse_weight_raw.split('(')
                    weight = parts[0]
                    weight_diff = parts[1].replace(')', '')
                except:
                    pass

            # 行データの構築
            row_data = race_metadata.copy()
            row_data.update({
                'rank': rank,
                'frame_number': frame_number, # 追加: 枠番
                'horse_number': horse_number, # 追加: 馬番
                'horse_name': horse_name,
                'age_sex': age_sex,
                'jockey_weight': jockey_weight,
                'jockey': jockey,
                'time': time_str,
                'margin': margin,             # 追加: 着差
                'passing_rank': passing_rank,
                'last_3f': last_3f,
                'odds': odds,                 # 追加: オッズ
                'popularity': popularity,     # 追加: 人気
                'horse_weight': weight,       # 整形後: 馬体重
                'horse_weight_diff': weight_diff, # 追加: 体重増減
                'trainer': trainer            # 追加: 調教師
            })
            df_rows.append(row_data)
           
        except IndexError as ie:
            # 特定の行で取得エラーが起きても止まらないようにする
            continue
        except Exception as e:
            # その他のエラー
            print(f"Error parsing row: {e}")
            continue

    return pd.DataFrame(df_rows)
# --- メイン処理 ---

def main():
    # 1. 期間設定 (YYYYMMDD)
    start_date = "20240101"
    end_date = "20241231" # テスト用に短く設定しています。実際は月単位などで指定。

    # 2. 有効なIDを取得 (カレンダー/リストページを巡回)
    target_race_ids = get_race_ids_for_period(start_date, end_date)

    print(f"収集対象レース総数: {len(target_race_ids)}")

    if not target_race_ids:
        print("指定期間にレースが見つかりませんでした。")
        return

    all_data = pd.DataFrame()

    # 3. 各レースの詳細データを取得 (プログレスバー付き)
    # ここで tqdm を使って進捗を表示
    for race_id in tqdm(target_race_ids):
        df = scrape_race_data(race_id)

        if df is not None and not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)

        # 重要: ループごとに必ずスリープ
        random_sleep(3, 6)

    # 4. 保存
    if not all_data.empty:
        filename = f"netkeiba_enhanced_{start_date}_{end_date}.csv"
        all_data.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"データ収集完了。保存先: {filename}")
        print(f"取得データ件数: {len(all_data)} 行")
    else:
        print("データが取得できませんでした。")

if __name__ == "__main__":
    main()
