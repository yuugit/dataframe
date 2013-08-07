# -*- coding: utf-8 -*-
 
import dataframe

import itertools


def main():
    (options, args) = parse_options()
 
    dic = {'foo': [83, 72, 94, 61], 'bar': ['apple', 'banana', 'git', 'gist']}
    df = dataframe.DataFrame(dic, ['foo', 'bar'])

    colname = 'foo'
    dfs = df.sort_by(colname)

    print dfs['bar']

    try_groupby()
    
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
