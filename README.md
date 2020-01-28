# Spark Mails Test

A little application to study the correlation between sent mails and distinct inbound contacts using Spark.

## Requirements

This solution is based on Spark, so two strict requirements are to be met:

 - Pyton 3.7
 - JAVA_HOME pointing to Java Runtime Environment 8

## Installation

First, you need to have a Java JDK 8 installed on your machine somewhere.
If you don't have the JRE 8, you can install it with (depending on the OS):
 - Arch: `pacman -S jdk8-openjdk`
 - Ubuntu-based OS: `apt install openjdk-8-jre`
 - Fedora: `yum install java-1.8.0-openjdk`
 
If the JDK 8 is not your JAVA_HOME's JDK, prepend the main command with: `JAVA_HOME=/path/to/jre8`

The project can then be built using (may need priviledges): `pip install .` (or prefixed for python 3.7: `python3.7 -m pip install .`

## Running

The program can then be run with:
```python summarize-enron.py [your-input-csv-file-path]```

If you need to prepend JAVA_HOME and python version:
```JAVA_HOME=/usr/lib/jvm/java-8-openjdk/ python3.7 summarize-enron.py /path/to/input/csv```

By default, you will find the output in the output folder:
 - CSV file(s): `output/csv`
 - graph: `output/plot.png`
