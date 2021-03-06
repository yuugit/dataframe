# -*- coding: utf-8 -*-

import csv, itertools
import numpy as np


def cbind(*dfseq):
    u"""行数が同じである複数のデータフレームを渡された順番に結合した
    1つのデータフレームにして返す。
    """
    df_res = DataFrame({}, [])  # 渡さないとだめっぽい -- デフォルト引数が効いてない？ note

    for df in dfseq:
        for cn in df.colnameseq:
            df_res.add_column(cn, df[cn])

    return df_res


def from_gen(gen, colnameseq):
    u"""タプルのジェネレータ gen からデータフレームを作る。
    colnameseq で列名を指定する。
    タプルは colnameseq と同じ長さで、
    つまりタプルの各要素が colnameseq で指定される列の要素になる。

    DataFrame.groupby で出てきた各サブセットに対し集計を行って、
    その集計値たちをジェネレータの形にしてこれに渡す、という風に使うつもり。
    """

    dic = dict((cn, []) for cn in colnameseq)
    for tpl in gen:
        for j, cn in enumerate(colnameseq):
            dic[cn].append(tpl[j])

    return DataFrame(dic, colnameseq)


class DataFrame(object):
    u"""データフレームのクラス。
    データは辞書の形で保持される。
    各キーと値のペアは、各列を表している。
    キーが列名に、値が列の内容(配列)になる。
    列の順序は colnameseq によって保持される。
    """

    def __init__(self, dic = {}, colnameseq = []):
        # 長さのチェック -- すべての列は同じ長さでなければならない。
        lenseq = [len(col) for col in dic.values()]
        for l in lenseq:
            if l != lenseq[0]:
                return 2013     # てきとう -- note: ちゃんと class E(Exception) 書く

        self.body = dict((k, np.array(v)) for k, v in dic.iteritems())
        self.colnameseq = colnameseq

    def __str__(self):

        # 各列ごとに最大文字列長を計算する。
        maxlenseq = []
        for cn in self.colnameseq:
            maxlen = len(cn)    # 列名の文字列長も考慮する。
            for elem in self[cn]:
                l = len(str(elem))
                if maxlen < l:
                    maxlen = l
            maxlenseq.append(maxlen)

        # フォーマットの規則
        # - 各列は半角スペースで区切る。
        # - 数値の場合は右寄せ、そうでなければ左寄せ。

        # まず列名の行の文字列表現を作る。
        sep = ' '
        acc = [sep.join([cn.ljust(l) for cn, l in zip(self.colnameseq, maxlenseq)])]

        # 各列の右寄せ or 左寄せを決める
        just_seq = [('rjust' if np.alltrue(np.isreal(self.body[cn])) else 'ljust') \
                        for cn in self.colnameseq]

        # 各行の文字列表現を作る。
        for row in self.gen_row():
            line = []
            for cn, l, just in zip(self.colnameseq, maxlenseq, just_seq):
                justed = getattr(str(row[cn]), just)(l)
                line.append(justed)

            acc.append(' '.join(line))

        # 各行をつなげて返す。
        return '\n'.join(acc)


    def __getitem__(self, colname):
        u"""辞書風の書式で書く列にアクセスできる。"""

        return self.body[colname]

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
        
        for k, g in itertools.groupby(df.gen_row(), lambda row: row[colname]):
            dic = dict((cn, []) for cn in self.colnameseq)

            for row in g:
                for cn in self.colnameseq:
                    dic[cn].append(row[cn])

            yield k, DataFrame(dic, self.colnameseq)

            
    def gen_row(self):
        u"""各行を列名をキーとする辞書の形で返すジェネレータを返す。"""

        for j in range(self.nrow()):
            row_dic = dict((cn, self.body[cn][j]) for cn in self.colnameseq)
            yield row_dic

    def nrow(self):
        u"""データフレームの行数を返す。"""

        return len(self.body[self.colnameseq[0]])


    def add_column(self, newcolname, newcol):
        u"""self に新しい列 newcol を加える。列名は newcolname.
        """

        self.colnameseq.append(newcolname)
        self.body[newcolname] = newcol # np.array を渡すべし note: 改良の余地あり



class DataFrame_fromCSV(DataFrame):
    def __init__(self, fileobj, coltypeseq):
        u"""一行目が列名になっているCSVファイルを読み込んで
        データフレームを作る。
        """

        reader = csv.reader(fileobj)

        colnameseq = reader.next()
        dic = dict((cn, []) for cn in colnameseq)

        for row in reader:
            for j, cn in enumerate(colnameseq):
                conv = coltypeseq[j]
                elem = row[j]
                dic[cn].append(conv(elem))

        DataFrame.__init__(self, dic, colnameseq)

