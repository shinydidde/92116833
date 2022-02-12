import pandas as pd
from bokeh.layouts import gridplot
from bokeh.palettes import Spectral4
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource
from utilities.sqlite_utils import SQLLiteUtils

# connection to the DB
SQLITE_CONN_STRING = "sqlite:///data.db"


# function to get four points from the given 50 ideal functions
def get_ideal_func(train_data, ideal_data):
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
        gen_max[winner] = [temp_["ideal_" + winner + "_ls"].max() ** (1 / 2)]
    gen_ideal_functions.insert(loc=0, column='x', value=merge_df['x'])
    return {'ideal': gen_ideal_functions, 'max': gen_max}


def test_func(test, ideal, max_d):
    # merging test and generated ideal functions tables
    merge_df = test.merge(ideal, on=['x'], how='left')
    test_df['ideal_func'] = None

    for _i, row in merge_df.iterrows():
        i_func = None
        delta_y_min = float('inf')
        for _j, _row in max_d.T.iterrows():
            delta_y = abs(row['y'] - row[_j])

            if _row[0] ** (1 / 2) >= delta_y and delta_y_min > delta_y:
                delta_y_min = delta_y
                i_func = _j

        test_df.at[_i, 'delta_y'] = delta_y_min if delta_y_min < float('inf') else None
        test_df.loc[_i, 'ideal_func'] = i_func
        test_df.at[_i, 'ideal_y'] = merge_df[i_func][_i] if i_func else None
    return test


# function to plot the graph
def _plot(f_df, trn_df, dev_df, t_df):
    # training function 1
    f1 = figure(title="Training function 1")
    f1.circle(f_df.x.to_list(), f_df[f_df.columns[1]].to_list(), color='red', legend_label='Ideal Function 1')
    f1.line(trn_df.x.to_list(), trn_df.train_y1.to_list(), color='black', legend_label='Training 1')
    # training function 2
    f2 = figure(title="Training function 2")
    f2.circle(f_df.x.to_list(), f_df[f_df.columns[2]].to_list(), color='yellowgreen', legend_label='Ideal Function 2')
    f2.line(trn_df.x.to_list(), trn_df.train_y2.to_list(), color='teal', legend_label='Training 2')
    # training function 3
    f3 = figure(title="Training function 3")
    f3.circle(f_df.x.to_list(), f_df[f_df.columns[3]].to_list(), color='blue', legend_label='Ideal Function 3')
    f3.line(trn_df.x.to_list(), trn_df.train_y3.to_list(), color='red', legend_label='Training 3')
    # training function 4
    f4 = figure(title="Training function 4")
    f4.circle(f_df.x.to_list(), f_df[f_df.columns[4]].to_list(), color='orange', legend_label='Ideal Function 4')
    f4.line(trn_df.x.to_list(), trn_df.train_y4.to_list(), color='green', legend_label='Training 4')

    f5 = figure(title="Test Vs Training Ideal Data")
    f5.circle(t_df.x.to_list(), t_df.y.to_list(), color='black', legend_label='Test')
    c_list = Spectral4
    for i_, col in enumerate(dev_df.columns):
        t_copy = t_df.copy()
        t_copy.loc[t_df.ideal_func != col, 'ideal_y'] = None
        f5.circle(t_df.x.to_list(), t_copy['ideal_y'].to_list(), color=c_list[i_], legend_label='Ideal function {0}'.format(col))

    f6 = figure(title="Max. Deviations")
    y = dev_df.values.tolist()[0]
    source = ColumnDataSource(data=dict(left=[1, 2, 3, 4], counts=y, color=Spectral4))
    f6.vbar(x='left', top='counts', width=0.5, color='color', legend_field="counts", source=source)

    # make a grid
    grid = gridplot([[f1, f2], [f3, f4], [f5, f6]], width=1200, height=600)
    show(grid)


if __name__ == "__main__":
    sqlite_utils = SQLLiteUtils(conn_string=SQLITE_CONN_STRING)

    # adding training data to the DB table 1
    train_df = pd.read_csv("data/train.csv")
    sqlite_utils.put_df(df=train_df, table='train', conn_string=SQLITE_CONN_STRING)

    # adding ideal data to the DB table 2
    ideal_df = pd.read_csv("data/ideal.csv")
    sqlite_utils.put_df(df=ideal_df, table='ideal', conn_string=SQLITE_CONN_STRING)

    # adding test data to the DB
    test_df = pd.read_csv("data/test.csv")
    sqlite_utils.put_df(df=test_df, table='test', conn_string=SQLITE_CONN_STRING)

    # adding 4 generated ideal functions and max deviation data to the DB
    func_df = get_ideal_func(train_df, ideal_df)
    # sqlite_utils.put_df(df=func_df['ideal'], table='generated_data', conn_string=SQLITE_CONN_STRING)
    # sqlite_utils.put_df(df=func_df['max'], table='max_deviation', conn_string=SQLITE_CONN_STRING)

    # test data mapping
    test_map_df = test_func(test_df, func_df['ideal'], func_df['max'])

    # plotting the graph
    _plot(func_df['ideal'], train_df, func_df['max'], test_map_df)

    # finally storing the table 3
    test_map_df = test_map_df[['x', 'y', 'delta_y', 'ideal_func']]
    sqlite_utils.put_df(df=test_map_df, table='test_map', conn_string=SQLITE_CONN_STRING)
