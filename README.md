# car_analyser
Spark analysis for car traffic


## Problem statement
An automated traffic counter sits by a road and counts the number of cars that go past. Every half-hour the counter outputs the number of cars seen and resets the counter to zero. You are part of a development team that has been asked to implement a system to manage this data - the first task required is as follows:
Write a program that reads a file, where each line contains a timestamp (in yyyy-mm-ddThh:mm:ss format, i.e. ISO 8601) for the beginning of a half-hour and the number of cars seen that half hour. An example input is included below. You can assume clean input, as these files are machine-generated.
The program should output:
The number of cars seen in total
A sequence of lines where each line contains a date (in yyyy-mm-dd format) and the number of cars seen on that day (eg. 2016-11-23 289) for all days listed in the input file.
The top 3 half hours with most cars, in the same format as the input file
The 1.5 hour period with least cars (i.e. 3 contiguous half hour records)
Requirements
The program must use Spark.
A solution in Databricks/Jupyter notebooks IS NOT acceptable.
The program can be written in Python or Scala, and with any libraries you are familiar with. You are encouraged to use modern versions of each language and make use of their features.
The program must be accompanied with reasonable levels of unit tests.
The solution should be developed to professional standards, the code may be used and extended by your teammates.
The solution should be deliverable within a couple of hours - please do not spend excessive amounts of time on this.
Include the instructions on how to test/run your program as well as any assumption you make for your implementation.
Assessment
Your submission will be assessed on the following:
Correctness of solution
Readability and fluency of your code (including tests)
Effectiveness of your tests
Project structure
Sample input
2021-12-01T05:00:00 5
2021-12-01T05:30:00 12
2021-12-01T06:00:00 14
2021-12-01T06:30:00 15
2021-12-01T07:00:00 25
2021-12-01T07:30:00 46
2021-12-01T08:00:00 42
2021-12-01T15:00:00 9
2021-12-01T15:30:00 11
2021-12-01T23:30:00 0
2021-12-05T09:30:00 18
2021-12-05T10:30:00 15
2021-12-05T11:30:00 7
2021-12-05T12:30:00 6
2021-12-05T13:30:00 9
2021-12-05T14:30:00 11
2021-12-05T15:30:00 15
2021-12-08T18:00:00 33
2021-12-08T19:00:00 28
2021-12-08T20:00:00 25
2021-12-08T21:00:00 21
2021-12-08T22:00:00 16
2021-12-08T23:00:00 11
2021-12-09T00:00:00 4

## Instructions to run the program
1. Ensure you have Apache Spark installed and properly set up on your machine.
2. Save the  code as is.
3. Run the program using the command line, providing the path to the input file as an argument. For example:
   ```
   spark-submit car_analyser.py path/to/input_file.txt
   ```
4. The program will output the total number of cars, the daily counts, the top 3 half hours with the most cars, and the 1.5 hour period with the least cars to the console.
5. Alter log4j.properties to set the logging level to appropriate level to troubleshoot Spark processing.

## Development environment
- Apache Spark 3.0 or higher
- Python 3.13 or higher
- PySpark library for Python
- The wrapper script `go.sh` is provided to simplify running the program and tests. Ensure it has execute permissions.
- Options for go.sh:
  - To run unit tests: `./go.sh run-unit-tests`
  - To set up the environment, i.e. install dependencies: `./go.sh setup`

## Unit tests
Unit tests are included in the `tests` directory. To run the tests, use a following command:
```./go.sh run-unit-tests```

## Assumptions
- The input file is well-formatted and does not contain any errors or inconsistencies.
- The timestamps in the input file are in ISO 8601 format and represent the start of a half-hour period.
- The number of cars is a non-negative integer.
- The program is designed to run in a Spark environment and may not work correctly in a non-Spark environment.
- The program does not handle time zones and assumes all timestamps are in the same time zone.
- The program does not account for daylight saving time changes, which may affect the calculation of daily counts and the 1.5 hour period with the least cars.
- The program assumes that the input file is not excessively large and can be processed in memory. For very large files, additional optimizations may be necessary.
- The program does not include error handling for file I/O operations, so it assumes that the specified input file exists and is accessible.
- The program does not include functionality for writing output to a file or database, and instead outputs results to the console.
- The program does not include functionality for handling duplicate timestamps or overlapping half-hour periods, and assumes that each timestamp in the input file is unique and represents a distinct half-hour period.

