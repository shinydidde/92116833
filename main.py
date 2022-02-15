import sqlite3
import pandas as pd
import unittest
from bokeh.layouts import gridplot
from bokeh.palettes import Spectral4
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource
from utilities.sqlite_utils import SQLLiteUtils

# connection to the DB
SQLITE_CONN_STRING = "sqlite:///data.db"


# function to get four points from the given 50 ideal functions
def get_ideal_func(train_data, ideal_data):
    """
    Description of get_ideal_func:

    Parameters:
    argument1 : Training data
    argument2 : Ideal data

    Returns:
    dict[str, Dataframe]: {arg1: 4 selected ideal functions (DataFrame), arg2: max deviations of the 4 selected functions(DataFrame)}
    """
    # replacing the column names for uniqueness
    train_data.columns = train_data.columns.str.replace('y', 'train_y')
    ideal_data.columns = ideal_data.columns.str.replace('y', 'ideal_y')
    # merging both the tables based on x using pandas
    merge_df = pd.merge(train_data, ideal_data, left_on='x', right_on='x', how='inner')
    # creating dataframes to store our final values
    gen_ideal_functions = pd.DataFrame()
    gen_max = pd.DataFrame()
    # looping through the merged data table
    for c_, i in enumerate([col for col in merge_df.columns if 'train_' in col]):
        # creating a temporary dataframe
        temp_ = pd.DataFrame()
        for j in [col for col in merge_df.columns if 'ideal_' in col]:
            temp_["{1}_ls".format(i, j)] = (merge_df[i] - merge_df[j]) ** 2
        winner = str(temp_.sum().idxmin()).split("_")[1]
        gen_ideal_functions[[winner]] = merge_df[["ideal_" + winner]]
        # getting the maximum deviation
        gen_max[winner] = [temp_["ideal_" + winner + "_ls"].max() ** (1 / 2)]
    gen_ideal_functions.insert(loc=0, column='x', value=merge_df['x'])
    return {'ideal': gen_ideal_functions, 'max': gen_max}


def test_func(test, ideal, max_d):
    """
    Description of test_func:

    Parameters:
    argument1 : Training data
    argument2 : Ideal data
    argument3 : Max deviations data

    Returns:
    DataFrame[str, Dataframe] : The database table of the test-data, with mapping and y-deviation(Table: 4)
    """
    # merging test and generated ideal functions tables
    merge_df = test.merge(ideal, on=['x'], how='left')
    test_df['ideal_func'] = None

    for _i, row in merge_df.iterrows():
        i_func = None
        delta_y_min = float('inf')
        for _j, _row in max_d.T.iterrows():
            delta_y = abs(row['y'] - row[_j])
            # for a function to be assigned the delta should not be more than 
            # the maximum deviation by a factor of sqrt(2) 
            if _row[0] * (2 ** (1 / 2)) >= delta_y and delta_y_min > delta_y:
                delta_y_min = delta_y
                i_func = _j
        test_df.at[_i, 'delta_y'] = delta_y_min if delta_y_min < float('inf') else None
        test_df.loc[_i, 'ideal_func'] = i_func
        test_df.at[_i, 'ideal_y'] = merge_df[i_func][_i] if i_func else None
    return test


# function to plot the graphs
def _plot(f_df, trn_df, dev_df, t_df):
    """
    Description of _plot: Generate a plot.

    Render the data in the Series as a bokeh gridplot.

    Parameters
    ----------
    f_df : DataFrame
        Ideal data
    trn_df : DataFrame
        Training Data
    dev_df : DataFrame
        Max deviation data
    t_df : DataFrame
        Test Data
    """
    # training function 1 graph
    f1 = figure(title="Training Function 1")
    f1.circle(f_df.x.to_list(), f_df[f_df.columns[1]].to_list(), color='red', legend_label='Ideal Function ' + f_df.columns[1])
    f1.line(trn_df.x.to_list(), trn_df.train_y1.to_list(), color='black', legend_label='Training 1')
    # training function 2 graph
    f2 = figure(title="Training Function 2")
    f2.circle(f_df.x.to_list(), f_df[f_df.columns[2]].to_list(), color='yellowgreen', legend_label='Ideal Function ' + f_df.columns[2])
    f2.line(trn_df.x.to_list(), trn_df.train_y2.to_list(), color='teal', legend_label='Training 2')
    # training function 3 graph
    f3 = figure(title="Training Function 3")
    f3.circle(f_df.x.to_list(), f_df[f_df.columns[3]].to_list(), color='blue', legend_label='Ideal Function ' + f_df.columns[3])
    f3.line(trn_df.x.to_list(), trn_df.train_y3.to_list(), color='red', legend_label='Training 3')
    # training function 4 graph
    f4 = figure(title="Training Function 4")
    f4.circle(f_df.x.to_list(), f_df[f_df.columns[4]].to_list(), color='orange', legend_label='Ideal Function ' + f_df.columns[4])
    f4.line(trn_df.x.to_list(), trn_df.train_y4.to_list(), color='green', legend_label='Training 4')
    # Test Vs Training Ideal Data graph
    f5 = figure(title="Test Vs Training Ideal Data")
    f5.circle(t_df.x.to_list(), t_df.y.to_list(), color='black', legend_label='Test')
    c_list = Spectral4
    for i_, col in enumerate(dev_df.columns):
        t_copy = t_df.copy()
        t_copy.loc[t_df.ideal_func != col, 'ideal_y'] = None
        f5.circle(t_df.x.to_list(), t_copy['ideal_y'].to_list(), color=c_list[i_], legend_label='Ideal function {0}'.format(col))
    # Max Deviations graph
    f6 = figure(title="Max. Deviations")
    y = dev_df.values.tolist()[0]
    source = ColumnDataSource(data=dict(left=[1, 2, 3, 4], counts=y, color=Spectral4))
    f6.vbar(x='left', top='counts', width=0.5, color='color', legend_field="counts", source=source)

    # make a grid
    grid = gridplot([[f1, f2], [f3, f4], [f5, f6]], width=1200, height=600)
    show(grid)

# Unit test to check whether the connection to the db was successful or not
class DbConnection(unittest.TestCase):
    def test(self):   
        try:
            self.connection = sqlite3.connect('data.db')
        except:
            print('Error')
        finally:
            pass

if __name__ == "__main__":
    sqlite_utils = SQLLiteUtils(conn_string=SQLITE_CONN_STRING)

    # Table 1: The training data's database table:
    train_df = pd.read_csv("data/train.csv")
    sqlite_utils.put_df(df=train_df, table='train', conn_string=SQLITE_CONN_STRING)

    # Table 2: The ideal functions database table:
    ideal_df = pd.read_csv("data/ideal.csv")
    sqlite_utils.put_df(df=ideal_df, table='ideal', conn_string=SQLITE_CONN_STRING)

    # Table 3: The test data's database table:
    test_df = pd.read_csv("data/test.csv")
    sqlite_utils.put_df(df=test_df, table='test', conn_string=SQLITE_CONN_STRING)

    # getting 4 generated ideal functions and max deviation
    func_df = get_ideal_func(train_df, ideal_df)

    # test data mapping
    test_map_df = test_func(test_df, func_df['ideal'], func_df['max'])

    # plotting the graph
    _plot(func_df['ideal'], train_df, func_df['max'], test_map_df)

    # Table 4: The database table of the test-data, with mapping and y-deviation
    test_map_df = test_map_df[['x', 'y', 'delta_y', 'ideal_func']]
    sqlite_utils.put_df(df=test_map_df, table='test_map', conn_string=SQLITE_CONN_STRING)
    unittest.main()
