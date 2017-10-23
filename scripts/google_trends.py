#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""get data from trends.google.co.jp.

Usage:
    google_trends.py
        --conf_file_path=<conf_file_path>
        --output_dir_path=<output_dir_path>
    google_trends.py -h | --help

Options:
    -h --help  show this screen and exit.
"""

from datetime import date
from datetime import datetime
from datetime import timedelta
from docopt import docopt
import pandas as pd
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pytrends.request import TrendReq
import sys
import yaml

class GetDataFromGTrends(object):
    """get data from trends.google.co.jp."""

    def run(
        self,
        project_name: str,
        start_date: datetime.date,
        end_date: datetime.date,
        keywords: list,
        gd_folder_id: str,
        output_dir_path: str,
    ) -> bool:
        """execute."""
        # output file
        utf8_output_file_name = '%s_utf8.csv' % (project_name)
        sjis_output_file_name = '%s.csv' % (project_name)
        now = datetime.now().strftime('%Y%m%d%H%M%S')

        # term
        term = str(start_date) + ' ' + str(end_date)

        # pytrend
        pytrend = TrendReq(tz=-540)

        # google drive
        gauth = GoogleAuth()
        gauth.CommandLineAuth()
        drive = GoogleDrive(gauth)

        # make directoy
        target_folder_id = self.__search_folder(
            drive,
            gd_folder_id,
            now,
        )
        if target_folder_id == '':
            f = drive.CreateFile(
                {
                    'title': now,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [
                        {
                            'kind': 'drive#fileLink',
                            'id': gd_folder_id,
                        }
                    ],
                }
            )
            f.Upload()

            # get folder_id
            target_folder_id = self.__search_folder(
                drive,
                gd_folder_id,
                now,
            )

            if target_folder_id == '':
                raise SystemError('can\'t make directory.')

        df = ''
        for keyword in keywords:
            # get data from google trend
            pytrend.build_payload(kw_list=[keyword], geo='JP', timeframe=term)
            df_part = pytrend.interest_over_time()[keyword]

            # merge dataframe
            if isinstance(df, pd.core.series.Series):
                df = pd.concat([df, df_part], axis=1)
            else:
                df = df_part

        # rename index
        indexes = df.index
        new_indexes = []
        for i, v in enumerate(indexes):
            j = i + 1
            try:
                w = indexes[j] - timedelta(1)
            except IndexError:
                w = v + timedelta(6)
            new_index = v.strftime('%Y-%m-%d') \
                + ' - ' \
                + w.strftime('%Y-%m-%d')
            new_indexes.append(new_index)

        df.index = new_indexes

        # set file path
        utf8_output_file_path = output_dir_path + utf8_output_file_name
        sjis_output_file_path = output_dir_path + sjis_output_file_name

        # write to csv
        df.to_csv(utf8_output_file_path, index=True, header=True)
        # encode
        with open(sjis_output_file_path, 'w', encoding='cp932') as f_out:
            with open(utf8_output_file_path, 'r', encoding='utf-8') as f_in:
                f_out.write(f_in.read())

        # send to google drive
        f = drive.CreateFile(
            {
                'title': sjis_output_file_name,
                'mimeType': 'text/csv',
                'parents': [
                    {
                        'kind': 'drive#fileLink',
                        'id': target_folder_id,
                    }
                ],
            }
        )
        f.SetContentFile(sjis_output_file_path)
        f.Upload()

        return True

    def __search_folder(
        self,
        drive: GoogleDrive,
        parent_folder_id: str,
        title: str,
    ) -> str:
        """search folder from google drive."""
        query_fmt = 'parents=\'%s\' ' \
            'and title=\'%s\' ' \
            'and mimeType=\'application/vnd.google-apps.folder\' ' \
            'and trashed=false'

        # search
        folder_id = ''
        query = query_fmt % (
            parent_folder_id,
            title,
        )
        search_results = drive.ListFile({'q': query}).GetList()
        for search_result in search_results:
            folder_id = search_result['id']
            break

        return folder_id

if __name__ == '__main__':
    print('%s %s start.' % (datetime.today(), __file__))

    # get parameters
    args = docopt(__doc__)
    conf_file_path = args['--conf_file_path']
    output_dir_path = args['--output_dir_path']

    # settings
    with open(conf_file_path) as f:
        conf_data = yaml.load(f)
    gd_folder_id = conf_data['gd_folder_id']
    start_date = conf_data['start_date']
    end_date = conf_data['end_date']
    if end_date == 'yesterday':
        end_date = date.today() - timedelta(1)
    keywords = conf_data['keywords']
    project_name = conf_data['project_name']

    # run
    gdfgt = GetDataFromGTrends()
    gdfgt.run(
        project_name,
        start_date,
        end_date,
        keywords,
        gd_folder_id,
        output_dir_path,
    )

    print('%s %s end.' % (datetime.today(), __file__))

    sys.exit(0)
