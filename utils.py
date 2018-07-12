import sys

import pandas as pd


def read_file(filename):
    """Reads file and process it using panda dataframes.
    
    @param name of the file
    @return dataframe
    """
    try:
        df = pd.read_csv(filename)
        return df
    except IOError:
        print('File "%s" could not be read' % filename)
        sys.exit()

def do_join(df1, df2, index):
    """Joins two dataframes by 'index' key they have in
    common.

    @param df1 dataframe with column 'index' present 
    @param df2 another dataframe with column 'index' present
    @index column that will be used to join

    @return dataframe of joined df1 and df2
    """
    return df1.join(df2.set_index(index), on=index)

def join_by_index(file1, file2, index):
    """Joins two csv files by 'index' key they have in
    common.

    @param file1, file2 csv files
    @index column that will be used to join

    @return dataframe of joined df1 and df2
    """
    df1 = read_file(file1)
    df2 = read_file(file2)

    return do_join(df1, df2, index)

def join_to_csv(df, output):
    """Writes a csv file with joined dataframe

    @param df dataframe that has been previously joined
    @param output name of the csv file to be written
    """
    return df.to_csv(output)

if __name__ == '__main__':
    df = join_by_index('Data/Vacadata1.csv', 'Data/Vacadata2.csv', 'ID')
    join_to_csv(df, 'Data/Vacadata_1_2s.csv')