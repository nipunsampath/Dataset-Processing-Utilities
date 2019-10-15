import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import os
import pandas as pd
from os import listdir
from os.path import isfile, join
import glob


def filter_traffic(df,label):

    normal = df[df['Label'] == label]
    
    return normal


if __name__ == "__main__":
    source = "/home/nipuna/Documents/Projects/Dataset/DataSets/CSE-CIC-IDS/"

    os.chdir(source)

    for file in glob.glob("*.csv"):
        print("Extracting data from " + file)
        data = pd.read_csv(source+file, encoding="ISO-8859-1")


        filter_traffic(data).to_csv("/home/nipuna/Documents/Projects/Dataset/DataSets/CSE-CIC-IDS/Normal Traffic/{}".format(file))


