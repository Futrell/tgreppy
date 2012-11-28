""" tgreppy
Maybe some day this will be some sort of comprehensive 
python wrapper for tgrep2. For now, it just takes a 
query with multiple points to print and organizes the 
output into a dataframe.
"""
import os
import functools
from subprocess import Popen, PIPE, STDOUT
import pandas as pd

DEFAULT_MATCH_FLAGS = "afi"
DEFAULT_OUTPUT_FLAGS = "t u".split()

START_MACRO = "@"
END_MACRO = ";"
MACRO_DELIM = "\t"
SUB_DELIM = " "
PRINT_MARKER = "`"
COMMENT_MARKER = "#"

QUERY_INDEX_NAME = "query_index"

class TGrep2Queries(object):
    """ TGrep2Queries

    This contains a list of queries and the macros
    that apply to them, as well as the number of
    fields to be printed for queries.
    The number of fields is assumed to be constant
    across queries. 
    The object provides an iterable that behaves as if
    each query were in an individual file with all
    appropriate macros.
    """
    def __init__(self, q=None, macros=None):
        if not q:
            self.q = []
        else:
            self.q = q
        if not macros:
            self.macros = []
        else:
            self.macros = macros
        if q:
            self.n_fields = self._print_how_many(self.q[0])
        else:
            self.n_fields = 0

    @staticmethod
    def _print_how_many(query):
        count = query.count(PRINT_MARKER)
        if not count:
            return 1
        return count

    def read_from_file(self, filename):
        infile = open(filename)
        for line in infile:
            line = self._remove_comments(line)
            line = line.strip()
            if line:
                if self._is_macro(line):
                    self.macros.append(line)
                else:
                    self.q.append(line)
        infile.close()
        self.n_fields = self._print_how_many(self.q[0]) 

    @staticmethod
    def _remove_comments(line):
        try:
            return line[:line.index(COMMENT_MARKER)]
        except ValueError:
            return line
        
    @staticmethod
    def _is_macro(line):
        return (line[0] == START_MACRO 
                and line[-1] == END_MACRO)

    def __iter__(self):
        return (self._append_macros(x) for x in self.q)

    def __len__(self):
        return self.n_fields

    def _append_macros(self, query):
        macroString = "\n".join(self.macros)
        return "\n".join([macroString, query])

class TGrep2(object):
    """ TGrep2 object

    This thing does what tgrep2 does. Initialize it with
    a corpus and the flags to control matching. Then 
    use it to query that corpus with the various query
    methods. Query results are returned as dataframes.
    """

    def __init__(self, corpus_file, 
                 flags=DEFAULT_MATCH_FLAGS):
        self.corpus = corpus_file.strip()
        self.flags = flags + "c"

    def query_from_file(self, filename, 
                        flags=DEFAULT_OUTPUT_FLAGS):
        """ Query from file.

        Read queries from a file; the queries may contain
        macros and comments. Run those queries and 
        return a dataframe with the result. 
        
        Specify output parameters with flags; each
        single flag will be run.
        """

        queries = TGrep2Queries()
        queries.read_from_file(filename)
        return self.query(queries, flags)

    def query_str(self, s, flags=DEFAULT_OUTPUT_FLAGS):
        """ Query string.

        Run tgrep2 with the given single query string;
        return results in a dataframe.
        Specify output parameters with flags; each single
        flag will be run. For instance if you specify
        "tu", tgrep2 will be run with -t and then with
        -u. 
        """
        queries = TGrep2Queries(s)
        return self.query([s], flags)


    def query(self, queries, flags=DEFAULT_OUTPUT_FLAGS):
        """ Query.

        This method takes a list of strs or  
        TGrep2Queries object and 
        returns a dataframe of its results. It is
        usually accessed through query_str and 
        query_from_file, which produce TGrep2Queries
        objects.
        """
        
        r = self._run_queries(queries, flags)        
        return self._to_df(r, len(queries)) 

    def _run_queries(self, queries, flags=DEFAULT_OUTPUT_FLAGS):
        """ Run queries.
        
        Open tgrep2 with each output flag and collect
        the output. Iterate through the queries and
        through the flags specified.

        Return a list of lists of results from different
        flag configurations. For instance the results
        are structured like [query-1 [flag-1 flag-2]
        query-2 [flag-1 flag-2]]. The 
        results can then be appended together 
        in a dataframe using to_db. 
        """
        r = []
        cmd = "tgrep2"
        for q in queries:
            queryR = []
            for flag in flags:
                flagsToRun = "-" + "".join([flag, self.flags])
                p = Popen([cmd, flagsToRun, self.corpus, "-"],
                          stdout=PIPE, stdin=PIPE, stderr=STDOUT)
                out = p.communicate(input=q)
                queryR.append(out[0])
            r.append(queryR)
        return r

    def _to_df(self, r, n_fields, col_names=None):
        """ Convert tgrep2 output to a data frame.

        This method takes tgrep2 output--a list of lists of
        strings sent to stdout by tgrep2 after running it
        possibly multiple times--and converts it to a
        pandas dataframe. The method has to know how many
        fields have been printed for each tgrep2 query
        (the number is assumed to be the same for each
        query). 
        
        By default, indices are just the order in which
        a line is printed by tgrep2. Column names are 
        assigned numerical names by default, but you
        can alternatively pass in a list of column 
        names.
        """
        df = pd.DataFrame()
        for queryIndex, queryR in enumerate(r):
            d = self._query_result_to_df(queryR, n_fields, 
                                         queryIndex, col_names)
            df = df.append(d, ignore_index=True) 
        return df

    @staticmethod
    def _query_result_to_df(r, n_fields, q_index, col_names=None):
        numFlags = len(r) # r is a list of strings
        df = [list() for _ in range(numFlags * n_fields)]
        for flagIndex, flagR in enumerate(r): # strings
            for j, line in enumerate(flagR.split("\n")):
                colIndex = (flagIndex * n_fields) + (j % n_fields)
                df[colIndex].append(line)
        numLines = len(df[0])
        
        # Assign column names.
        if not col_names:
            col_names = range(len(df)) # numerical col names
        df = dict(zip(col_names, df)) 
        df[QUERY_INDEX_NAME] = [q_index]  * numLines

        # Convert to pandas.
        return pd.DataFrame(df)
