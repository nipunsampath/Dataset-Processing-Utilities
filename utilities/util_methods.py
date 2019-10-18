import os
import pandas as pd
import glob
import json


def run_filter(source, label, value, output_path, filter_method):
    """Executes a method provided as the argument for every csv file in source directory and creates the result in  output directory. Filter method should return a dataframe"""
    os.chdir(source)

    for file in glob.glob("*.csv"):
        print("Extracting data from " + file)
        data = pd.read_csv(source + "/" + file, encoding="ISO-8859-1")
        filter_method(data, label, value).to_csv(os.path.join(output_path, file))

    print("All done")


def filter_traffic_by_column(df, label, value):
    """
    Filter records in a dataframe according to a value in a column

    :param df: dataframe
    :param label: Column Name
    :param value: Value to be filtered

    """
    filtered = df[df[label] == value]
    return filtered


def json_to_csv(source, output):
    """
    :param source: path to json file
    :param output: path to csv file
    """
    data = []
    for line in open(source, "r"):
        data.append(json.loads(line))
    df = pd.DataFrame(data)
    df.to_csv(output)
    print("All Done")


if __name__ == "__main__":
    json_to_csv("/home/nipuna/Documents/Projects/Dataset/DataSets/HTTP GET/2019-09-25-1569425820-http_get_7077.json","test2.csv")
