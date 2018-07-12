import sys

import pandas as pd

def drop_column(df, column):
    """Drop from dataframe 'column'

    @param df dataframe to drop column
    @param column column to be dropped
    @return dataframe with dropped column
    """
    try:
        df = df.drop(column, axis=1)
    except ValueError as err:
        print(err)

    return df

def fix_missing_with_mode(df):
    """Fixes missing value from all columns using the mode.
    
    @param df dataframe
    @return dataframe    
    """
    return df.fillna(df.mode().iloc[0])

def dummies(df, list=None):
    """Set dummie variables to the dataframe
    for the columns stated in 'list parameter
    
    @param df dataframe
    @list list of columns to set dummies
    @return dataframe with dummies variables
    """
    return pd.get_dummies(df, columns=list)

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

def do_join(df1, df2, index, how):
    """Joins two dataframes by 'index' key they have in
    common.

    @param df1 dataframe with column 'index' present 
    @param df2 another dataframe with column 'index' present
    @index column that will be used to join

    @return dataframe of joined df1 and df2
    """
    return df1.join(df2.set_index(index), how=how, on=index)

def join_by_index(file1, file2, index, how='left'):
    """Joins two csv files by 'index' key they have in
    common.

    @param file1, file2 csv files
    @index column that will be used to join

    @return dataframe of joined df1 and df2
    """
    df1 = read_file(file1)
    df2 = read_file(file2)

    return do_join(df1, df2, index, how)

def join_to_csv(df, output):
    """Writes a csv file with joined dataframe

    @param df dataframe that has been previously joined
    @param output name of the csv file to be written
    """
    return df.to_csv(output)

if __name__ == '__main__':
    #df = join_by_index('Data/Vacadata1.csv', 'Data/Vacadata2.csv', 'ID')
    #join_to_csv(df, 'Data/Vacadata_1_2s.csv')
    df = read_file('Data/animales.csv')
    df = fix_missing_with_mode(df)
    df.loc[df['INTERPARTO'] >= 1000, 'INTERPARTO'] = 0
    df = drop_column(df, 'MADRE')

    #join_to_csv(df, 'Data/Vacadata_animal.csv')

    df_joined = join_by_index('Data/Vacadata_disease.csv', 'Data/Vacadata_Goal.csv', 'ID', 'inner')

    df_joined_2 = do_join(df, df_joined, 'ID', 'inner')

    df_dim = read_file('Data/Vacadata_DIM.csv')

    df_joined_3 = do_join(df_joined_2, df_dim, 'ID', 'inner')

    #join_to_csv(df_joined, 'joined_disease_goal2.csv')
    #join_to_csv(df_joined2, 'joined_pregnancy_dim.csv')
    
    df_dummies = dummies(df_joined_3, ['RAZA', 'PADRE'])
    print("NUMERO DE COLUMNAS")
    print(len(df.columns))
    join_to_csv(df_dummies, 'joined_final_dummies.csv')
