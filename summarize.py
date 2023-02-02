from enum import Enum, auto
from pathlib import Path
import csv
import json
import pandas as pd
import sys
import os
from ast import literal_eval


class Project(Enum):
    debug = 'debug'
    experiment = 'experiment'


class Summarizer():
    project = None
    # データ保存用フォルダへの path を生成
    project_data_dir_path = None
    output_dir_path = None
    collection_names_in_user = None
    collection_names_in_modes = None
    collection_names_in_vpet_mode = None
    collection_names_in_normal_mode = None
    # modes = ['vpet']
    modes = ['normal', 'vpet']

    def __init__(self, project):
        # アクセスするプロジェクトを設定
        if project not in [Project.debug, Project.experiment]:
            print("Error: '{}' is invalid project".format(project))
            exit(1)
        self.project = project

        # データベースのうちアクセスするコレクションを設定
        if self.project == Project.debug:
            pass
        elif self.project == Project.experiment:
            pass
        else:
            print("Error: '{}' is invalid project".format(self.project))
            exit(1)

        # 生データがあるフォルダへの path を生成
        self.project_data_dir_path = Path(
            __file__).parent / 'data' / self.project.value
        # 要約したデータの保存先へのパスを取得
        self.output_dir_path = Path(__file__).parent / 'summarized_data'
        # フォルダが存在しない場合作成して続行
        if self.output_dir_path.exists() == False:
            self.output_dir_path.mkdir(parents=True)

    '''
    PROJECT_user_id.csv (ユーザIDを各行1列目に納めたcsv) に記述された
    全ユーザのデータを集計する
    '''

    def summarize_data(self):
        # user_id を格納したファイルの path を取得
        user_id_csv_path = Path(__file__).parent / \
            str(self.project.value + '_user_id.csv')

        with open(user_id_csv_path) as f:
            # csv ファイルを読み込む
            reader = csv.reader(f)
            users = [row[0] for row in reader]
        self.summarize_app_usage_time(users)
        self.summarize_daily_user_word_learning_status(users)
        self.summarize_launch(users)
        self.summarize_words(users)
        self.summarize_reviews(users)

    def summarize_app_usage_time(self, users):
        print("start summarizing app usage time")
        for mode in self.modes:
            li_summarized_data = []
            for user in users:
                user_csv_path = self.project_data_dir_path / \
                    user / 'csv' / mode / 'app_usage_time.csv'
                # pandas で csv を読み込み
                df_input = pd.read_csv(user_csv_path)
                # 合計を取得
                sum_app_usage_time = df_input['appUsageTime'].sum()
                # df にユーザ名とデータを追加
                li_summarized_data.append([user, sum_app_usage_time])
            # list を df に変換
            df_output = pd.DataFrame.from_records(li_summarized_data, columns=[
                'user', 'app_usage_time'])
            # データ保存用フォルダへの path を生成
            output_file_path = self.output_dir_path / \
                '{}_app_usage_time.csv'.format(mode)
            # csv に出力
            df_output = df_output.set_index('user')
            df_output.to_csv(output_file_path)
            print("save: {}".format(output_file_path))

    def summarize_daily_user_word_learning_status(self, users):
        print("start summarizing daily user word learning status")
        df_output = pd.DataFrame()
        for mode in self.modes:
            for user in users:
                user_csv_path = self.project_data_dir_path / \
                    user / 'csv' / mode / 'word_learning_status.csv'
                # pandas で csv を読み込み
                df_input = pd.read_csv(user_csv_path)
                # フィルタで利用するために日を表す date を time というカラムからも利用できるようにする
                df_input['time'] = df_input['date'].values
                # 利用期間でフィルタ
                df_input = self.filter_df(user, df_input)
                # 各状態をカウントする
                df_s_status_count = df_input['wordLearningStatus'].value_counts(
                )
                df_s_status_count['user'] = user
                df_s_status_count['unused'] = 14 - \
                    df_input['wordLearningStatus'].count()
                df_output = pd.concat(
                    [df_output, pd.DataFrame(df_s_status_count).T]).reset_index(drop=True)

            # データ保存用フォルダへの path を生成
            output_file_path = self.output_dir_path / \
                '{}_word_learning_status.csv'.format(mode)
            # csv に出力
            df_output = df_output.set_index('user')
            print(df_output)
            df_output.to_csv(output_file_path)
            print("save: {}".format(output_file_path))
            df_output = pd.DataFrame()

    def summarize_launch(self, users):
        print("start summarizing app launch count")
        for mode in self.modes:
            li_summarized_data = []
            for user in users:
                user_csv_path = self.project_data_dir_path / \
                    user / 'csv' / mode / 'launch.csv'
                # pandas で csv を読み込み
                df_input = pd.read_csv(user_csv_path)
                # 利用期間でフィルタ
                df_input = self.filter_df(user, df_input)
                # 起動回数を取得
                launch_count = df_input.shape[0]
                # df にユーザ名とデータを追加
                li_summarized_data.append([user, launch_count])
            # list を df に変換
            df_output = pd.DataFrame.from_records(li_summarized_data, columns=[
                'user', 'launch_count'])
            # データ保存用フォルダへの path を生成
            output_file_path = self.output_dir_path / \
                '{}_launch_count.csv'.format(mode)
            # csv に出力
            df_output = df_output.set_index('user')
            df_output.to_csv(output_file_path)
            print("save: {}".format(output_file_path))

    def summarize_words(self, users):
        print("start summarizing words")
        for mode in self.modes:
            li_summarized_data = []
            for user in users:
                user_csv_path = self.project_data_dir_path / \
                    user / 'csv' / mode / 'words.csv'
                # pandas で csv を読み込み
                df_input = pd.read_csv(user_csv_path)
                # フィルタで利用するために追加日を表す timeAdded の値 time というカラムからも利用できるようにする
                df_input['time'] = df_input['timeAdded'].values
                # 利用期間でフィルタ
                df_input = self.filter_df(user, df_input)
                # ユーザが追加した単語数を取得
                word_count = df_input.shape[0]
                # df にユーザ名とデータを追加
                li_summarized_data.append([user, word_count])
            # list を df に変換
            df_output = pd.DataFrame.from_records(li_summarized_data, columns=[
                'user', 'word_count'])
            # データ保存用フォルダへの path を生成
            output_file_path = self.output_dir_path / \
                '{}_word_count.csv'.format(mode)
            # csv に出力
            df_output = df_output.set_index('user')
            df_output.to_csv(output_file_path)
            print("save: {}".format(output_file_path))

    def summarize_reviews(self, users):
        print("start summarizing reviews")
        for mode in self.modes:
            li_summarized_data = []
            for user in users:
                user_csv_path = self.project_data_dir_path / \
                    user / 'csv' / mode / 'reviews.csv'
                # pandas で csv を読み込み
                df_input = pd.read_csv(user_csv_path)
                # 利用期間でフィルタ
                df_input = self.filter_df(user, df_input)
                # ユーザが追加した単語数を取得
                li_word_ids = df_input['wordId'].to_list()
                # 単語のidのリストが文字列として読み込まれているので変換
                # DataFrame.to_pickle() で保存すればこの手順は要らなくなる
                review_count_array = [len(literal_eval(word_ids))
                                      for word_ids in li_word_ids]
                review_count = sum(review_count_array)
                # df にユーザ名とデータを追加
                li_summarized_data.append([user, review_count])
            # list を df に変換
            df_output = pd.DataFrame.from_records(li_summarized_data, columns=[
                'user', 'review_count'])
            # データ保存用フォルダへの path を生成
            output_file_path = self.output_dir_path / \
                '{}_review_count.csv'.format(mode)
            # csv に出力
            df_output = df_output.set_index('user')
            df_output.to_csv(output_file_path)
            print("save: {}".format(output_file_path))
    # アプリの利用期間でデータをフィルタする

    def filter_df(self, user, df_input):
        df_input['time'] = pd.to_datetime(df_input['time'])
        df_info = pd.read_csv(
            self.project_data_dir_path / 'user_info.csv')
        df_info['start'] = pd.to_datetime(df_info['start'])
        df_info['end'] = pd.to_datetime(df_info['end'])
        df_info = df_info.set_index('id')
        start = df_info.at[user, 'start']
        end = df_info.at[user, 'end']
        # print(start)
        # print(end)
        df_filtered = df_input[(df_input['time'] >= start)
                               & (df_input['time'] <= end)]
        return df_filtered


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

    # データを要約
    summarizer = Summarizer(project)
    summarizer.summarize_data()


if __name__ == '__main__':
    main()
