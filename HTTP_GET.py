# from utilities.util_methods import json_to_csv
import base64
import json
import pandas as pd


def json_to_csv(source, output):
    """
    :param source: path to json file
    :param output: path to csv file
    """

    data_list = []
    count = 1
    skipped = 0

    for line in open(source, "r"):

        print("Converting line: " + str(count))
        temp = json.loads(line)
        data_encoded = temp['data']

        try:
            data_decoded = base64.b64decode(data_encoded).decode("utf-8").splitlines()
        except UnicodeDecodeError:
            print("Could not decode line" + str(count))
            skipped += 1

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
            data = {}
            data["Time stamp"] = data_decoded_dict['Date']
            # else:
            #     data["Time stamp"] = ""

            data["IP"] = temp['ip']

            if "Content-Type" in data_decoded_dict:
                data["Content-Type"] = data_decoded_dict["Content-Type"]
            else:
                data["Content-Type"] = ""
            count += 1
            data_list.append(data)

    df = pd.DataFrame(data_list)

    df.to_csv(output)
    print("All Done\n Skipped: {}".format(skipped))


if __name__ == "__main__":
    source_directory = "/home/nipuna/Documents/Projects/Dataset/DataSets/HTTPS/2019-09-25-1569432580-https_get_60443.json"
    output_directory = "/home/nipuna/Documents/Projects/Dataset/DataSets/HTTPS/2019-09-25-1569432580-https_get_60443.csv"

    json_to_csv(source_directory, output_directory)
