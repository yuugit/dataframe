# -*- coding: utf-8 -*-

import csv, itertools
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
    u"""データフレームのクラス。
    データは辞書の形で保持される。
    各キーと値のペアは、各列を表している。
    キーが列名に、値が列の内容(配列)になる。
    列の順序は colnameseq によって保持される。
    """

    def __init__(self, dic, colnameseq): # colnameseq は列名の順序を保持するのに必要

        # 長さのチェック -- すべての列は同じ長さでなければならない。
        lenseq = [len(col) for col in dic.values()]
        for l in lenseq:
            if l != lenseq[0]:
                return 2013     # てきとう -- note: ちゃんと class E(Exception) 書く

        #print 'debugwrite in DataFrame.__init__: dic =', dic
        
        self.body = dict((k, np.array(v)) for k, v in dic.iteritems())

        #print 'debugwrite in DataFrame.__init__: self.body =', self.body

        self.colnameseq = colnameseq

        #print 'debugwrite in DataFrame.__init__: self.colnameseq =', self.colnameseq

    def __getitem__(self, colname):
        u"""辞書風の書式で書く列にアクセスできる。"""

        #try:

        return self.body[colname]

        #except Exception:
        #    import pdb
        #    pdb.set_trace()


    def sort_by(self, colname):
        u"""列名を与えて、その列に関して各データ(行)をソートしたデータフレームを返す。
        非破壊的ソートとして書く。
        """

        idx = np.argsort(self.body[colname]) # Rのorder関数にあたるもの

        # ソート
        dic = {}
        for cn in self.colnameseq:
            dic[cn] = self.body[cn][idx]

        return DataFrame(dic, self.colnameseq)

    def groupby(self, colname):
        u"""colname で指定される列の値でデータフレームをグループ分けする。
        グループ分けされたサブセット(isa データフレーム)をジェネレータの形で返す。
        たぶん itertools.groupby で書ける。
        """

        df = self.sort_by(colname) # あらかじめ colname でソートしておく。
        
        def keyfunc(row):
            return row[colname]

        for k, g in itertools.groupby(df.gen_row(), keyfunc):
            dic = dict((cn, []) for cn in self.colnameseq)

            for row in g:
                for cn in self.colnameseq:
                    dic[cn].append(row[cn])

            yield k, DataFrame(dic, self.colnameseq)

            
    def gen_row(self):
        u"""各行を列名をキーとする辞書の形で返すジェネレータを返す。"""

        for j in range(self.nrow()):
            row_dic = dict((cn, self.body[cn][j]) for cn in self.colnameseq)
            #print 'debugwrite in DataFrame.gen_row: row_dic =', row_dic
            yield row_dic

    def nrow(self):
        return len(self.body[self.colnameseq[0]])


    def add_column(self, colname, newcol):
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

