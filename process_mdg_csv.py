"""
Tools for analysing release-versions from maven central.
Tailored to format of the csv provided by MDG18 (Maven Dependency Graph 2018):
https://zenodo.org/record/1489120

@date Dec. 2021
"""

import csv
import os
import re
import shutil
from collections import Counter

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def counter_from_gavjd(filename: str):
    package_list = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for line in reader:
            artifactname = line['artifact'].rsplit(':', 1)[0]
            version = line['artifact'].rsplit(':', 1)[1]
            package_list.append(artifactname)
    print(len(package_list))
    return Counter(package_list)


def counter_from_gav(filename: str, fieldname='artifactname'):
    package_list = []
    with open(filename, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for entry in reader:
            package_list.append(entry[fieldname])
    print(len(package_list))
    return Counter(package_list)


def split(filename: str):
    """Split the releases list into nice and naughty depending on whether they adhere to SemVer principles"""
    with open(filename, newline='') as csvfile, \
            open("all_nice.csv", "x") as nice, \
            open("all_naughty.csv", "x") as naughty:
        nice.write("artifactname, version\n")  # headers are nice
        naughty.write("artifactname, version\n")

        reader = csv.DictReader(csvfile, delimiter=',')
        for line in reader:
            artifactname = line['artifact'].rsplit(':', 1)[0]
            version = line['artifact'].rsplit(':', 1)[1]
            line_to_write = artifactname + ',' + version + '\n'
            if re.fullmatch(pattern=
                            "^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$",
                            string=version) is not None:
                nice.write(line_to_write)
            else:
                naughty.write(line_to_write)


def plot_counter(sorted_package_counter: Counter, name=""):
    df = pd.DataFrame.from_records(sorted_package_counter.most_common(), columns=['package', 'count'])
    print(df.head)
    max_val = df['count'].max()
    bins1 = max_val // 30
    bins2 = max_val // 10

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, sharey=True)
    fig.suptitle(f"{name}")

    # plt.grid(which="both")
    df.hist(bins=10, ax=ax1, log=True)
    ax1.set_title(f"10 Bins")
    ax1.set_xlabel("#versions")
    ax1.set_ylabel("#packages")

    df.hist(bins=bins1, ax=ax2, log=True)
    ax2.set_title(f"{bins1} Bins")
    ax2.set_xlabel("#versions")
    df.hist(bins=bins2, ax=ax3, log=True)
    ax3.set_title(f"{bins2} Bins")
    ax3.set_xlabel("#versions")

    plt.show()
    df.boxplot(column=['count'], sym='+')
    plt.show()
    df_f = df.loc[df['count'] > 1]
    df_f.boxplot(column=['count'], sym='+')
    plt.show()
    df_f = df.loc[df['count'] > 20]
    df_f.boxplot(column=['count'], sym='+')
    plt.show()

    sns.violinplot(data=df)
    plt.show()


def counts_to_csv(package_counter: Counter, filename="temp/versions_per_package_sorted_all.csv"):
    with open(filename, "x") as f:
        for c, v in package_counter.most_common():
            s = '\n' + c + ', ' + str(v)
            f.write(s)


def cleanup():
    print("Remove extracted files")
    try:
        shutil.rmtree('temp/')
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
    try:
        os.mkdir('temp')
    except OSError as error:
        print(error)


if __name__ == "__main__":
    print(f"process_mdg_csv")
    cleanup()
    # packageCounter = counter_from_gavjd('all_naughty.csv')
    # sorted_package_counter = packageCounter.most_common()

    # plot nice and naughty
    # package_counter = counter_from_gav('all_nice.csv')
    # plot_counter(package_counter, "Packages adhering to SemVer syntactically")
    package_counter = counter_from_gav('all_naughty.csv')
    plot_counter(package_counter, "Packages not adhering to SemVer syntactically")

    # sorted_package_counter = packageCounter.most_common()
    # list_GA = [artifact for artifact, version in sorted_package_counter]
    # split('release_all.csv')
    # counts_to_csv(packageCounter)
