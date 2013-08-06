# -*- coding: utf-8 -*-

import csv
import numpy as np


def cbind(*dfseq):
    u"""行数が同じである複数のデータフレームを渡された順番に結合した
    1つのデータフレームにして返す。
    """
    pass

def from_gen(gen, colnameseq):
    u"""タプルのジェネレータ gen からデータフレームを作る。
    colnameseq で列名を指定する。
    タプルは colnameseq と同じ長さで、
    つまりタプルの各要素が colnameseq で指定される列の要素になる。

    DataFrame.groupby で出てきた各サブセットに対し集計を行って、
    その集計値たちをジェネレータの形にしてこれに渡す、という風に使うつもり。
    """
    pass


class DataFrame(object):
    def __init__(self, dic, colnameseq): # colnameseq は列名の順序を保持するのに必要

        self.body = dict((k, np.array(v)) for k, v in dic.iteritems())
        self.colnameseq = colnameseq

    def __getitem__(self, colname):
        return self.body[colname]

    def sort_by(self, colname):
        pass

    def groupby(self, colname):
        u"""colname で指定される列の値でデータフレームをグループ分けする。
        グループ分けされたサブセットをジェネレータの形で返す。
        たぶん itertools.groupby で書ける。
        """

    def add_column(self, colname, newcol):
        pass

    def nrow(self):
        pass


class DataFrame_fromCSV(DataFrame):
    def __init__(self, csv_path, coltypeseq):
        u"""一行目が列名になっているCSVファイルを読み込んで
        データフレームを作る。
        """

        with open(csv_path) as f:
            reader = csv.reader(f)

            colnameseq = reader.next()
            dic = dict((cn, []) for cn in colnameseq)

            for row in reader:
                for j, cn in enumerate(colnameseq):
                    conv = coltypeseq[j]
                    elem = row[j]
                    dic[cn].append(conv(elem))

        DataFrame.__init__(self, dic, colnameseq)

