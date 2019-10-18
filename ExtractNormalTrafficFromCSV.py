# This module reads all csv files in a directory and filters out matching records for a label

from utilities.util_methods import run_filter


def filter_traffic_by_column(df, label, filter):
    filtered = df[df[label] == filter]
    return filtered


# def run_filter(source, label, filter, output_directory, filter_method):
#     os.chdir(source)
#     out_path = os.path.join(source, output_directory)
#     try:
#         os.mkdir(out_path)
#     except FileExistsError:
#         print("Directory already exists, filtered file will be created inside it")
#
#     for file in glob.glob("*.csv"):
#         print("Extracting data from " + file)
#         data = pd.read_csv(source + "/" + file, encoding="ISO-8859-1")
#         #filter_traffic_by_column(data, label, filter).to_csv(os.path.join(out_path, file))
#         filter_method(data,label,filter).to_csv(os.path.join(out_path, file))
#
#     print("All done")


if __name__ == "__main__":
    # inputs
    source_directory = "/home/nipuna/Documents/Projects/Dataset/DataSets/CSE-CIC-IDS"
    output_directory = "JustTest"
    column_name = "Label"
    filter_value = "Benign"

    run_filter(source_directory, column_name, filter_value, output_directory, filter_traffic_by_column)
