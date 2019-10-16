# This module reads all csv files in a directory and filters out matching records
# for a label

import os
import pandas as pd
import glob


def filter_traffic(df, label, filter):
    filtered = df[df[label] == filter]
    return filtered


def run_filter(source,label,filter,output_directory):
    os.chdir(source)
    out_path = os.path.join(source, output_directory)
    try:
        os.mkdir(out_path)
    except FileExistsError:
        print("Directory already exists, filtered file will be created inside it")

    for file in glob.glob("*.csv"):
        print("Extracting data from " + file)
        data = pd.read_csv(source + "/" + file, encoding="ISO-8859-1")
        filter_traffic(data, label, filter).to_csv(os.path.join(out_path, file))


if __name__ == "__main__":
    # source = "/home/nipuna/Documents/Projects/Dataset/DataSets/CSE-CIC-IDS/"

    #inputs
    source = "/home/nipuna/Documents/Projects/Dataset/DataSets/Pcaps"
    output_directory = "Filtered"
    label = "Protocol"
    filter = "HTTP"

    run_filter(source,label,filter,output_directory)

    print("All done")