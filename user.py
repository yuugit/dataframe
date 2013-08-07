# -*- coding: utf-8 -*-
 
import dataframe

import itertools, StringIO


def main():
    (options, args) = parse_options()
 
    #try_basic()
    #try_groupby()
    try_fromCSV()
    
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
