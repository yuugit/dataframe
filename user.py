# -*- coding: utf-8 -*-
 
import dataframe

import itertools, StringIO
import numpy as np


def main():
    (options, args) = parse_options()
 
    try_summary()

    #try_basic()
    #try_groupby()
    #try_fromCSV()
    #try_from_gen()
    
def try_summary():
    u"""Rで
      write.csv(iris, file = 'iris.csv', quote = F, row.names = F)
    して作ったファイルを読んで、各種類ごとに
      Sepal.Length, Sepal.Width, Petal.Length, Petal.Width
    の平均値を計算する。"""

    with open('iris.csv') as f:
        df = dataframe.DataFrame_fromCSV(f, [float, float, float, float, str])

    def g():
        for level, df_subset in df.groupby('Species'):
            yield (np.mean(df_subset['Sepal.Length']),
                   np.mean(df_subset['Sepal.Width']),
                   np.mean(df_subset['Petal.Length']),
                   np.mean(df_subset['Petal.Width']),
                   level)
                   
    summary_colnameseq = [
        'mean.Sepal.Length',
        'mean.Sepal.Width',
        'mean.Petal.Length',
        'mean.Petal.Width',
        'Species']

    df_summary = dataframe.from_gen(g(), summary_colnameseq)

    print df_summary


def try_from_gen():

    colnameseq = ['num', 'num*num', 'name']
    words = ['a', 'b', 'c', 'd', 'e']
    tg = ((n, n*n, str(words[(n*n)%len(words)])) for n in range(30))

    print dataframe.from_gen(tg, colnameseq).body


def try_basic():

    dic = {'foo': [83, 72, 94, 61], 'bar': ['apple', 'banana', 'git', 'gist']}
    df = dataframe.DataFrame(dic, ['foo', 'bar'])

    colname = 'foo'
    dfs = df.sort_by(colname)

    print dfs['bar']


def try_groupby():

    dic = {'foo': [83, 72, 94, 61, 365, 128, 999],
           'bar': ['apple', 'banana', 'git', 'gist', 'tick', 'tack', 'toe'],
           'baz': [0, 1, 1, 0, 1, 0, 0]}
    df = dataframe.DataFrame(dic, ['foo', 'bar', 'baz'])

    colname = 'baz'

    dfs = df.sort_by(colname)

    print dfs.body              # note: バグ -- sort_by がおかしい

    for k, g in itertools.groupby(dfs.gen_row(), lambda row: row[colname]):
        print
        print 'k =', k
        print 'list(g) =', list(g)


def try_fromCSV():

    s = u"""language,nchar
    Python,6
    Ruby,4
    Perl,4
    JavaScript,10
    C,1
    Haskell,7
    C++,3
    Common Lisp,11
    Brainf*ck,9"""

    s = '\n'.join([l.strip() for l in s.split('\n')])

    print s, type(s)

    f = StringIO.StringIO(s)

    #for line in f:
    #    print line

    print dataframe.DataFrame_fromCSV(f, [str, int]).body


 
def parse_options():
    import optparse
     
    parser = optparse.OptionParser()
    parser.add_option("--foo", type = "str", default = "FOO", help = "fooooooo!",
                      dest = "foo")
    parser.add_option("--bar", type = "int", default = 83, help = "83",
                      dest = "bar")
    return parser.parse_args()
     
 
if __name__ == '__main__':
    main()
