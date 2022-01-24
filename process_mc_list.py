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
    print("**Filter lsl file and convert to csv")
    with open(filename, newline='') as csvfile, \
            open("lsl_filtered_with_dates.csv") as result_csv:
        reader = csv.DictReader(csvfile, delimiter=' ', fieldnames=['size', 'date', 'time', 'path'],
                                skipinitialspace=True)  # ugly thing to use because size is right-aligned
        print(f"Lines to process: {len(list(reader))}")

        if dates:
            writer = csv.writer(result_csv,
                                fieldnames=['artifactname', 'version'])  # headers are nice
        else:
            writer = csv.writer(result_csv,
                                fieldnames=['artifactname', 'version', 'date'])

        javadoc_count = 0
        sources_count = 0
        tests_count = 0
        for line in reader:
            try:
                # split the line
                groupid, artifactname, version, jarname = line['path'].rsplit('/', 3)

                # filter out javadoc- and sources-jars
                the_slice = jarname[-11:-4]
                if the_slice == "javadoc":
                    javadoc_count += 1
                elif the_slice == "sources":
                    sources_count += 1
                elif the_slice == "-tests":
                    tests_count += 1
                else:
                    groupid_clean = groupid.replace('/', '.')
                    if dates:
                        writer.writerow([groupid_clean, artifactname, version, line['date']])
                    else:
                        writer.writerow([groupid_clean, artifactname, version])
                # print(groupid, artifactname, version, jarname)
            except ValueError as err:
                print(f"ValueError {err}")
                print(line)
    print(f" Javadoc: {javadoc_count}, Sources: {sources_count}, Tests: {tests_count}")
    print(f" Total filtered: {javadoc_count + sources_count + tests_count}")
    print()
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
