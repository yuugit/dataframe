# -*- coding: utf-8 -*-
 
import dataframe

import unittest

import numpy as np


# note: 列の長さが揃っていないとエラーになることを確かめる -- エラークラスを定義する。

class TestDataFrame_basic(unittest.TestCase):

    def setUp(self):
        dic = {'foo': [83, 72, 94, 61], 'bar': ['apple', 'banana', 'git', 'gist']}
        self.df = dataframe.DataFrame(dic, ['foo', 'bar'])

    def test_getitem(self):
        u"""__getitem__ で列を取得できることを確認する。"""

        bar = np.array(['apple', 'banana', 'git', 'gist'])

        self.assertTrue(np.alltrue(self.df['bar'] == bar))

    def test_sort_by(self):
        u"""sort_by でデータフレームの行を並べ替えられることを確認する。"""

        df_sorted = self.df.sort_by('foo')

        bar_sorted = np.array(['gist', 'banana', 'apple', 'git'])

        self.assertTrue(np.alltrue(df_sorted['bar'] == bar_sorted))

# note: test suite について(？) http://docs.python.jp/2/library/unittest.html を読み、
#       すべてのテストケースクラスのテストを実行させる。
# ,,, クラス名が被って再代入が起きてただけでした。マヌケ。

class TestDataFrame_groupby(unittest.TestCase):

    def setUp(self):
        dic = {'foo': [83, 72, 94, 61, 365, 128, 999],
               'bar': ['apple', 'banana', 'git', 'gist', 'tick', 'tack', 'toe'],
               'baz': [0, 1, 1, 0, 1, 0, 0]}
        self.df = dataframe.DataFrame(dic, ['foo', 'bar', 'baz'])

    def test_nrow(self):
        u"""nrow で行数が取得できることを確認する。"""

        self.assertEqual(self.df.nrow(), 7)

    def test_gen_row(self):
        u"""gen_row で行のジェネレータを取得できることを確認する。"""

        gr = self.df.gen_row()
        row = gr.next()
        self.assertEqual(row['foo'], 83)
        self.assertEqual(row['bar'], 'apple')
        self.assertEqual(row['baz'], 0)

        gr.next(); gr.next(); gr.next();

        row = gr.next()
        self.assertEqual(row['foo'], 365)
        self.assertEqual(row['bar'], 'tick')
        self.assertEqual(row['baz'], 1)

    def test_groupby(self):
        u"""groupby でグループごとにデータを取り出せることを確認する。"""

        # vvv DataFrame.groupby の使い方 vvv
        for lv, df_subset in self.df.groupby('baz'): # lv :abbrev: level
            self.assertTrue(lv in (0,1))

            if lv == 0:
                self.assertTrue(np.alltrue(df_subset['foo'] == [83, 61, 128, 999]))
                self.assertTrue(np.alltrue(df_subset['bar'] == ['apple', 'gist', 'tack', 'toe']))
            elif lv == 1:
                self.assertTrue(np.alltrue(df_subset['foo'] == [72, 94, 365]))
                self.assertTrue(np.alltrue(df_subset['bar'] == ['banana', 'git', 'tick']))


if __name__ == '__main__':
    unittest.main()
