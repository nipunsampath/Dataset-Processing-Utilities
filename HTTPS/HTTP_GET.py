import base64
import json
import pandas as pd
import pendulum
import datetime
from dateutil import parser
from pytz import timezone


def json_to_csv(source, output):
    """
    loads a json file, decodes the data field, append Timestamp, IP and Content-type into a csv file

    :param source: path to json file
    :param output: path to csv file
    """

    data_list = []
    count = 1
    skipped = 0
    filter_date = datetime.date(2019, 9, 25)

    for line in open(source, "r"):

        print("Converting line: " + str(count))
        temp = json.loads(line)
        data_encoded = temp['data']

        try:
            data_decoded = base64.b64decode(data_encoded).decode("utf-8").splitlines()

            # Filtering values from decoded data field
            data_decoded_dict = {}

            for i in range(len(data_decoded)):

                try:
                    key, value = data_decoded[i].split(":", 1)
                    data_decoded_dict[key] = value.strip()
                except ValueError as e:
                    # print(e)
                    pass

            if "Date" in data_decoded_dict:
                try:

                    date_obj = parser.parse(data_decoded_dict['Date'])
                    if date_obj.tzinfo is not None:

                        date_obj.astimezone(timezone('UTC'))
                        if date_obj.date() == filter_date:
                            data = {"Time stamp": date_obj.strftime('%d %b %Y %H:%M:%S'), "IP": temp['ip']}
                            if "Content-Type" in data_decoded_dict:
                                data["Content-Type"] = data_decoded_dict["Content-Type"]
                            else:
                                data["Content-Type"] = ""
                            count += 1
                            data_list.append(data)
                except ValueError:
                    skipped += 1
        except UnicodeDecodeError:
            print("Could not decode line" + str(count))
            skipped += 1

    df = pd.DataFrame(data_list, index=False)

    df.to_csv(output)
    print("All Done!\n Skipped: {}".format(skipped))


def get_time_difference(time, base_time):
    """
    :param time: A date time object to be compared with a base time
    :param base_time: A pendulum time object
    :return: Time difference in seconds
    """
    pendulum_time_obj = pendulum.instance(time)
    return pendulum_time_obj.diff(base_time).in_seconds()


def process_csv_timestamp(source, output):
    """
    Sorts the HTTPS dataset, calculate the time difference of each record with the first record

    :param source:
    :param output:
    :return:
    """
    data = pd.read_csv(source, index_col=0)

    try:
        data["Time stamp"] = pd.to_datetime(data["Time stamp"], format='%d %b %Y %H:%M:%S')
    except ValueError:
        pass
    data.sort_values(by=['Time stamp'], inplace=True)

    base_time = data['Time stamp'].iloc[0]

    pendulum_basetime_obj = pendulum.instance(base_time)

    data["Time stamp"] = data["Time stamp"].map(lambda x: get_time_difference(x, pendulum_basetime_obj))
    data.rename(columns={"Time stamp": "Elapsed Time"}, inplace=True)

    data.to_csv(output, index=False)
    print("All done!")


if __name__ == "__main__":
    source_path = "/home/nipuna/Documents/Projects/Dataset/DataSets/HTTPS/2019-09-25-1569432580-https_get_60443.json"
    output_path = "/home/nipuna/Documents/Projects/Dataset/DataSets/HTTPS/filtered/2019-09-25-1569432580-https_get_60443.csv"
    filtered_output = "/home/nipuna/Documents/Projects/Dataset/DataSets/HTTPS/testFiltered/2019-09-25-1569432580-https_get_60443.csv"

    # json_to_csv(source_path, output_path)

    process_csv_timestamp(output_path, filtered_output)
