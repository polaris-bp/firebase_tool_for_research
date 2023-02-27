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


class DailyAmoutOfLearningCalculater():
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
        self.output_dir_path = Path(__file__).parent / 'daily_data'
        # フォルダが存在しない場合作成して続行
        if self.output_dir_path.exists() == False:
            self.output_dir_path.mkdir(parents=True)

    '''
    PROJECT_user_id.csv (ユーザIDを各行1列目に納めたcsv) に記述された
    全ユーザのデータを集計する
    '''

    def output_daily_amout_of_learning(self):
        # user_id を格納したファイルの path を取得
        user_id_csv_path = Path(__file__).parent / \
            str(self.project.value + '_user_id.csv')

        with open(user_id_csv_path) as f:
            # csv ファイルを読み込む
            reader = csv.reader(f)
            users = [row[0] for row in reader]
            # self.output_daily_number_of_words_added(users)
            self.output_daily_number_of_reviews(users)

    def output_daily_number_of_words_added(self, users):
        print("start output_daily_number_of_words_added")
        for mode in self.modes:
            print("==== {} ====".format(mode))
            li_summarized_data = []
            for user in users:
                print(user)
                user_csv_path = self.project_data_dir_path / \
                    user / 'csv' / mode / 'words.csv'
                # pandas で csv を読み込み
                df_input = pd.read_csv(user_csv_path)
                # フィルタで利用するために追加日を表す timeAdded の値 time というカラムからも利用できるようにする
                df_input['time'] = df_input['timeAdded'].values
                # 利用期間でフィルタ
                df_input = self.filter_df(user, df_input)
                # タイムゾーンを変更
                df_input = self.get_df_utc9(df_input)
                # カウント用にコラムを追加
                df_input['for_count'] = 1
                # ユーザが追加した単語数を日毎の配列で取得
                daily_word_count = df_input.groupby(
                    pd.Grouper(key='time', freq='D')).sum(numeric_only=true)
                print(daily_word_count)
                # exit()
                # df にユーザ名とデータを追加
                # line = [user] + daily_word_count
                # li_summarized_data.append(line)
            # list を df に変換
            # df_output = pd.DataFrame.from_records(li_summarized_data, columns=[
            #     'user', 'word_count'])
            # データ保存用フォルダへの path を生成
            # output_file_path = self.output_dir_path / \
            #     '{}_daily_word_count.csv'.format(mode)
            # csv に出力
            # df_output = df_output.set_index('user')
            # df_output.to_csv(output_file_path)
            # print("save: {}".format(output_file_path))

    def output_daily_number_of_reviews(self, users):
        print("start output_daily_number_of_reviews")
        for mode in self.modes:
            print("==== {} ====".format(mode))
            li_summarized_data = []
            for user in users:
                print(user)
                user_csv_path = self.project_data_dir_path / \
                    user / 'csv' / mode / 'reviews.csv'
                # pandas で csv を読み込み
                df_input = pd.read_csv(user_csv_path)
                # 利用期間でフィルタ
                df_input = self.filter_df(user, df_input)
                # タイムゾーンを変更
                df_input = self.get_df_utc9(df_input)
                # カウント用にコラムを追加・更新
                df_input['for_count'] = 0
                for index, row in df_input.iterrows():  # df で for を回すのはアンチパターンらしいので要改善　https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
                    review_count = len(literal_eval(row['wordId']))
                    df_input.at[index, 'for_count'] = review_count
                # ユーザが追加した単語数を日毎の配列で取得
                daily_review_count = df_input.groupby(
                    pd.Grouper(key='time', freq='D')).sum(numeric_only=True)
                print(daily_review_count)
                # ？ユーザが追加した単語数を日毎に取得
                # li_word_ids = df_input['wordId'].to_list()
                # 単語のidのリストが文字列として読み込まれているので変換
                # DataFrame.to_pickle() で保存すればこの手順は要らなくなる
                # review_count_array = [len(literal_eval(word_ids))
                #                       for word_ids in li_word_ids]
                # review_count = sum(review_count_array)
                # df にユーザ名とデータを追加
                # li_summarized_data.append([user, review_count])
            # list を df に変換
            # df_output = pd.DataFrame.from_records(li_summarized_data, columns=[
                # 'user', 'review_count'])
            # データ保存用フォルダへの path を生成
            # output_file_path = self.output_dir_path / \
            #     '{}_daily_review_count.csv'.format(mode)
            # csv に出力
            # df_output = df_output.set_index('user')
            # df_output.to_csv(output_file_path)
            # print("save: {}".format(output_file_path))
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
        # print("before: {}".format(df_input.size))
        # print("after: {}".format(df_filtered.size))
        return df_filtered

    def get_df_utc9(self, df_filtered):
        # time のタイムゾーン（東京）に変更
        df_index_changed = df_filtered.set_index('time')
        df_utc9 = df_index_changed.tz_convert('Asia/Tokyo')
        df_utc9 = df_index_changed.tz_convert('Asia/Tokyo').reset_index()
        return df_utc9


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

    # 日毎の学習量を算出
    calculater = DailyAmoutOfLearningCalculater(project)
    calculater.output_daily_amout_of_learning()


if __name__ == '__main__':
    main()
