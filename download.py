from enum import Enum, auto
from pathlib import Path
import csv
import json
import pandas as pd
import sys
import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

'''
　実行する前にフォルダ内の "user_info.json", "user_info.csv"を削除して下さい <- これをしないで済むように修正予定
'''

'''
 アクセスする Firebase プロジェクトを指定するための列挙クラス
'''


class Project(Enum):
    debug = 'debug'
    experiment = 'experiment'


'''
Firebase プロジェクトの FireStore からデータを取得するためのクラス

 * FireStore の基本的な操作方法を知るには以下のページを参考にするとよい
    「Python で Google Firebase の Cloud Firestore （クラウド・ファイアストア）を使ってみる」
    URL: https://www.kkaneko.jp/tools/nosql/pythonfirebase.html
'''


class Downloader():
    project = None
    firebase_db = None
    collection_names_in_user = None
    collection_names_in_modes = None
    collection_names_in_vpet_mode = None
    collection_names_in_normal_mode = None

    '''
    初期化:
    アプリケーションの認証を通す
    FireStore データベースへの参照を取得する
    '''

    def __init__(self, project):
        # アクセスするプロジェクトを検証
        if project not in [Project.debug, Project.experiment]:
            print("Error: '{}' is invalid project".format(project))
            exit(1)
        self.project = project

        # アクセスするプロジェクトの証明書の path を取得
        cred_path = Path(__file__).parent / \
            str('admin-' + self.project.value + '.json')

        # 証明書を使って Firebase プロジェクトを認証
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        print('You passed the certification : {}'.format(self.project))

        # FireStore データベースへの参照を取得
        self.firebase_db = firestore.client()

        # データベースのうちアクセスするコレクションを設定
        if self.project == Project.debug:
            pass
        elif self.project == Project.experiment:
            pass
        else:
            print("Error: '{}' is invalid project".format(self.project))
            exit(1)

        self.collection_names_in_user = [
            'currentMode',
            'modeLog',
            'modes'
        ]
        self.collection_names_in_normal_mode = [
            'appUsageTime',
            'dailyUserWordLearningStatus',
            'goal',
            'launch',
            'reviews',
            'words'
        ]
        self.collection_names_in_vpet_mode = [
            'appUsageTime',
            'buy',
            'dailyUserWordLearningStatus',
            'goal',
            'item',
            'launch',
            'reviews',
            'wordPoint',
            'words'
        ]

    '''
    PROJECT_user_id.csv (ユーザIDを各行1列目に納めたcsv) に記述された
    全ユーザのデータを設定したプロジェクトの FireStore からダウンロードする
    '''

    def download_data_of_all_users_as_json(self):
        # userInfoをダウンロード
        user_info_collection_ref = self.firebase_db.collection('userInfo')
        proj_dir_path = Path(__file__).parent / 'data' / \
            str(self.project.value)
        # フォルダが存在しない場合作成して続行
        if proj_dir_path.exists() == False:
            proj_dir_path.mkdir(parents=True)
        user_info_json_file_path = proj_dir_path / 'user_info.json'
        if user_info_json_file_path.exists():
            print("already exists: {}".format(proj_dir_path))
            return
        data = self.get_dict_array_from_docs(user_info_collection_ref.stream())
        self.output_json_data(user_info_json_file_path, data)

        # user_id を格納したファイルの path を取得
        user_id_csv_path = Path(__file__).parent / \
            str(self.project.value + '_user_id.csv')

        with open(user_id_csv_path) as f:
            # csv ファイルを読み込む
            reader = csv.reader(f)

            for row in reader:
                # ユーザID(ハッシュ値)を取得
                user_id = row[0]

                # データをダウンロード
                self.download_data_of(user_id)
    '''
    FireStore から設定したコレクションの全要素をダウンロードする
    '''

    def download_data_of(self, user_id):
        print("start download data of {}".format(user_id))

        # ユーザデータへの参照を得る
        doc_user_ref = self.firebase_db.document('users/' + user_id)

        # データ保存用フォルダへの path を生成
        data_dir_path = Path(__file__).parent / 'data'
        project_dir_path = data_dir_path / self.project.value
        user_dir_path = project_dir_path / user_id
        output_json_dir_path = user_dir_path / 'json'

        # フォルダが存在しない場合作成して続行
        if output_json_dir_path.exists() == False:
            output_json_dir_path.mkdir(parents=True)

        # コレクションごとにデータを保存
        for collection_name in self.collection_names_in_user:
            # コレクションへの参照を取得
            collection_ref = doc_user_ref.collection(collection_name)

            # コレクションの各要素への参照を取得
            docs = collection_ref.stream()

            # 各要素の読み込み
            if collection_name == 'currentMode':
                self.output_current_mode_as_json(user_dir_path, docs)
            elif collection_name == 'modeLog':
                self.output_mode_log_as_json(user_dir_path, docs)
            elif collection_name == 'modes':
                # normal モードのドキュメントを取得
                normal_mode_doc_ref = collection_ref.document(
                    'normal')
                self.output_normal_mode_log_as_json(
                    user_dir_path, normal_mode_doc_ref)

                # vpet モードのドキュメントを取得
                vpet_mode_doc_ref = collection_ref.document(
                    'vPet')
                self.output_vpet_mode_log_as_json(
                    user_dir_path, vpet_mode_doc_ref)
            else:
                print("invalid collection name: {}".format(collection_name))

    # currentModeを json ファイルに保存
    def output_current_mode_as_json(self, user_dir_path, docs):
        output_json_file_path = user_dir_path / 'json' / 'current_mode.json'
        # データをダウンロード済みである場合はダウンロードを行わない
        # (アクセス過多による課金を抑えるため)
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        # json ファイルを出力
        self.output_json_data(output_json_file_path, data)

    def output_mode_log_as_json(self, user_dir_path, docs):
        output_json_file_path = user_dir_path / 'json' / 'mode_log.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_normal_mode_log_as_json(self, user_dir_path, normal_mode_doc_ref):
        # 各モードのデータを保存するディレクトリの path を取得
        out_put_json_path = user_dir_path / 'json' / 'normal'
        # フォルダが存在しない場合作成して続行
        if out_put_json_path.exists() == False:
            out_put_json_path.mkdir(parents=True)
        for collection_name in self.collection_names_in_normal_mode:
            if collection_name == 'appUsageTime':
                self.output_app_usage_time_as_json(
                    out_put_json_path, normal_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'dailyUserWordLearningStatus':
                self.output_daily_user_word_learning_status_as_json(
                    out_put_json_path, normal_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'goal':
                self.output_goal_as_json(
                    out_put_json_path, normal_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'launch':
                self.output_launch_as_json(
                    out_put_json_path, normal_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'reviews':
                self.output_reviews_as_json(
                    out_put_json_path, normal_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'words':
                self.output_words_as_json(
                    out_put_json_path, normal_mode_doc_ref.collection(collection_name).stream())
            else:
                print("Unprocessed data exists.")

    def output_vpet_mode_log_as_json(self, user_dir_path, vpet_mode_doc_ref):
        # 各モードのデータを保存するディレクトリの path を取得
        output_json_path = user_dir_path / 'json' / 'vpet'
        # フォルダが存在しない場合作成して続行
        if output_json_path.exists() == False:
            output_json_path.mkdir(parents=True)
        for collection_name in self.collection_names_in_vpet_mode:
            if collection_name == 'appUsageTime':
                self.output_app_usage_time_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'dailyUserWordLearningStatus':
                self.output_daily_user_word_learning_status_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'goal':
                self.output_goal_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'launch':
                self.output_launch_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'reviews':
                self.output_reviews_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'words':
                self.output_words_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'buy':
                self.output_buy_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'item':
                self.output_item_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            elif collection_name == 'wordPoint':
                self.output_word_point_as_json(
                    output_json_path, vpet_mode_doc_ref.collection(collection_name).stream())
            else:
                print("Unprocessed data exists.")

    def output_app_usage_time_as_json(self, mode_path, docs):
        # データを保存する json ファイルの path を取得
        output_json_file_path = mode_path / 'app_usage_time.json'
        # データをダウンロード済みである場合はダウンロードを行わない
        # (アクセス過多による課金を抑えるため)
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        # json ファイルを出力
        self.output_json_data(output_json_file_path, data)

    def output_daily_user_word_learning_status_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'word_learning_status.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(self, output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_goal_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'goal.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(self, output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_launch_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'launch.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_reviews_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'reviews.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_words_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'words.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_buy_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'buy.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_item_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'item.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_word_point_as_json(self, mode_path, docs):
        output_json_file_path = mode_path / 'word_point.json'
        if output_json_file_path.exists():
            print("already exists: {}".format(output_json_file_path))
            return
        data = self.get_dict_array_from_docs(docs)
        self.output_json_data(output_json_file_path, data)

    def output_json_data(self, output_json_file_path, data):
        with open(output_json_file_path, mode='wt', encoding='utf-8') as f:
            json.dump(data, f, indent=4, default=str)
        print("save: {}".format(output_json_file_path))

    def get_dict_array_from_docs(self, docs, dict=[]):
        data = []
        for doc in docs:
            doc_dict = doc.to_dict()
            data.append(doc_dict)
        return data


'''
ダウンロード済みのデータを変換するためのクラス
'''


class Converter:
    project = None

    '''
    初期化:
    プロジェクトの設定
    '''

    def __init__(self, project):
        # アクセスするプロジェクトを検証
        if project not in [Project.debug, Project.experiment]:
            print("Error: '{}' is invalid project".format(project))
            exit(1)
        self.project = project

    '''
    PROJECT_user_id.csv (ユーザIDを各行1列目に納めたcsv) に記述された
    全ユーザのデータについて json ファイルを csv ファイルに変換

    元の json ファイルは削除されず残る
    '''

    def convert_json_data_of_all_users_to_csv(self):
        # user_info.json を csv に変換
        user_info_json_file_path = Path(__file__).parent / \
            'data' / str(self.project.value) / 'user_info.json'
        with open(user_info_json_file_path) as f:
            df_json = pd.read_json(f)
            user_info_csv_file_path = Path(__file__).parent / \
                'data' / str(self.project.value) / 'user_info.csv'
            df_json.to_csv(user_info_csv_file_path,
                           encoding='utf-8', index=False)
        print("save:", user_info_csv_file_path)

        # user_id を格納したファイルの path を取得
        user_id_csv_path = Path(__file__).parent / \
            str(self.project.value + '_user_id.csv')
        with open(user_id_csv_path) as f:
            # ファイルの読み込み
            reader = csv.reader(f)
            for row in reader:
                # ユーザごとにデータを変換
                user_id = row[0]
                self.convert_json_data_of(user_id)

    '''
    指定ユーザの json を csv に変換して出力
    '''

    def convert_json_data_of(self, user_id):
        print("start convert data of {}".format(user_id))

        # ユーザデータを格納したフォルダへの path を取得
        data_dir_path = Path(__file__).parent / 'data'
        project_dir_path = data_dir_path / self.project.value
        user_dir_path = project_dir_path / user_id

        # json データを格納したフォルダへの path を取得
        original_json_dir_path = user_dir_path / 'json'
        # フォルダが存在しない場合は終了
        if original_json_dir_path.exists() == False:
            print("does not exist: {}".format(original_json_dir_path))
            return

        # csv ファイルを格納するフォルダへの path を取得
        output_csv_dir_path = user_dir_path / 'csv'
        # フォルダが存在しない場合は作成して続行
        if output_csv_dir_path.exists() == False:
            output_csv_dir_path.mkdir()
            print("made directory: {}".format(output_csv_dir_path))

        # フォルダ内の json を全て csv に変換して出力
        for f in original_json_dir_path.iterdir():
            print(f)
            if f.suffix == '.json':
                # padnas で読み込むと json <-> csv 変換が楽
                df_json = pd.read_json(f)

                # 出力ファイルの path を生成
                output_csv_file_path = output_csv_dir_path / \
                    str(f.stem + '.csv')

                # csv ファイルを出力
                df_json.to_csv(output_csv_file_path,
                               encoding='utf-8', index=False)
                print("save:", output_csv_file_path)

            elif os.path.basename(os.path.normpath(f)) == 'normal':
                # normal モードの csv ファイルを保存するフォルダを作成
                output_normal_mode_csv_dir_path = user_dir_path / 'csv' / 'normal'
                # フォルダが存在しない場合は作成して続行
                if output_normal_mode_csv_dir_path.exists() == False:
                    output_normal_mode_csv_dir_path.mkdir()
                    print("made directory: {}".format(
                        output_normal_mode_csv_dir_path))
                self.output_specified_mode_csv(
                    f, output_normal_mode_csv_dir_path)
            elif os.path.basename(os.path.normpath(f)) == 'vpet':
                # normal モードの csv ファイルを保存するフォルダを作成
                output_vpet_mode_csv_dir_path = user_dir_path / 'csv' / 'vpet'
                # フォルダが存在しない場合は作成して続行
                if output_vpet_mode_csv_dir_path.exists() == False:
                    output_vpet_mode_csv_dir_path.mkdir()
                    print("made directory: {}".format(
                        output_vpet_mode_csv_dir_path))
                self.output_specified_mode_csv(
                    f, output_vpet_mode_csv_dir_path)

    def output_specified_mode_csv(self, folder_path, output_csv_dir_path):
        for f in folder_path.iterdir():
            if f.suffix == '.json':
                df_json = pd.read_json(f)
                output_csv_file_path = output_csv_dir_path / \
                    str(f.stem + '.csv')
                df_json.to_csv(output_csv_file_path,
                               encoding='utf-8', index=False)
                print("save:", output_csv_file_path)


def main():
    # 引数の取得
    args = sys.argv
    if len(args) < 2:
        print("argument is required (please look at the code)")
        exit(1)

    # 引数よりアクセスするプロジェクトを設定
    arg_project = args[1]
    if arg_project == 'debug':
        project = Project.debug
    elif arg_project == 'experiment':
        project = Project.experiment
    else:
        print('invalid arguments')
        exit(1)

    # データをダウンロード
    downloader = Downloader(project)
    downloader.download_data_of_all_users_as_json()

    # データを変換
    converter = Converter(project)
    converter.convert_json_data_of_all_users_to_csv()


if __name__ == '__main__':
    main()
