"""
Tools for analysing release-versions from maven central.
Tailored to format of the output of rclone(https://github.com/rclone/rclone)s lsl

@date Jan. 2022
"""
import csv
import sys
import time
from collections import Counter

import pandas as pd
from matplotlib import pyplot as plt

from process_mdg_csv import counter_from_gav, counts_to_csv


def convert_lsl_to_csv(filename, dates=False):
    package_list = []
    with open(filename, newline='') as csvfile, open("lsl_filtered_with_dates.csv", "x") as result_csv:
        reader = csv.DictReader(csvfile, delimiter=' ', fieldnames=['size', 'date', 'time', 'path'],
                                skipinitialspace=True)  # ugly thing to use because size is right-aligned
        if dates == True:
            result_csv.write("artifactname,version\n")  # headers are nice
        else:
            result_csv.write("artifactname,version,date\n")  # headers are nice
        for line in reader:
            # print(line)
            try:
                groupid, artifactname, version, jarname = line['path'].rsplit('/', 3)

                # filter out javadoc- and sources-jars
                the_slice = jarname[-11:-4]
                if not (the_slice == "javadoc" or the_slice == "sources"):
                    package_list.append(artifactname)
                    if dates == True:
                        line_to_write = groupid.replace('/', '.') + ':' + artifactname + ',' + version + ',' \
                                        + line['date'] + '\n'
                    else:
                        line_to_write = groupid.replace('/', '.') + ':' + artifactname + ',' + version + '\n'
                    result_csv.write(line_to_write)
                # print(groupid, artifactname, version, jarname)
            except ValueError as err:
                print(f"ValueError {err}")
                print(line)
    print(len(package_list))
    return


def count_packages(filename):
    counts_to_csv(counter_from_gav(filename, 'artifactname'), "artifactcount.csv")


def count_dates(filename):
    package_list = []
    count = 0
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for entry in reader:
            if int(entry['date'][:4]) > 2000:
                package_list.append(entry['date'][:4])
            else:
                count += 1
    print(len(package_list))
    print(f"ommited because broken: {count}")
    return Counter(package_list)


def validate_artifactnames():
    pass


def plot_counter(sorted_package_counter: Counter, name=""):
    df = pd.DataFrame.from_records(sorted_package_counter.most_common(), columns=['year', 'count'])
    print(df.head)

    df.plot(kind='barh')
    plt.ylabel('Jahr')
    plt.xlabel('#Jars');

    plt.show()


def main(filename="lsl-full.txt"):
    # convert_lsl_to_csv(filename, dates=True)
    # count_packages("lsl_filtered_with_dates.csv")
    # count_dates("lsl_filtered_with_dates.csv")
    plot_counter(count_dates("lsl_filtered_with_dates.csv"))


if __name__ == "__main__":
    print(f"Arguments count: {len(sys.argv)}")
    starttime = time.time();
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        main(filename)
    main()
    print((time.time() - starttime) * 1000, "ms")
