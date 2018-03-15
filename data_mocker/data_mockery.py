#!/usr/bin/env pyspark
"""Creates mock data for something

This script generates data for consumption by OAC (our BI tool)

Basically what I am doing is sampling from not Q3 2017
    and using these values to populate Q4 2017

Data Schema: 
root
 |-- Week.Day: integer (nullable = true)            - [10/1/2017 - 12/31/2017] 
 |-- Calendar.Year: integer (nullable = true)       - 2017
 |-- Year.Half: string (nullable = true)            - 2017 HY2
 |-- Half.Quarter: string (nullable = true)         - 2017 Q4
 |-- Quarter.Week: string (nullable = true)         - 2017 Week [40-53]
 |-- ProductType.Product: string (nullable = true)
 |-- LOB.ProductType: string (nullable = true)
 |-- Brand.LOB: string (nullable = true)
 |-- Products.Brand: string (nullable = true)
 |-- State.City: string (nullable = true)
 |-- [skip]10: string (nullable = true)
 |-- Country.State: string (nullable = true)
 |-- Currency: string (nullable = true)
 |-- Country[uda]: string (nullable = true)
 |-- Area.Country: string (nullable = true)
 |-- MFGRegion.Area: string (nullable = true)
 |-- MFG.MFGRegion: string (nullable = true)
 |-- Departments.Department: string (nullable = true)
 |-- Department.Office: string (nullable = true)
 |-- Segment.Customer: integer (nullable = true)
 |-- CustRegion.Segment: string (nullable = true)
 |-- Customer.CustomerType[attr]: string (nullable = true)
 |-- Customers.CustRegion: string (nullable = true)
 |-- [skip]23: string (nullable = true)
 |-- Profit Ratio: double (nullable = true)
 |-- Profit Margin: double (nullable = true)
 |-- Final Revenue: double (nullable = true)
 |-- Final Discount: double (nullable = true)

"""
from __future__ import print_function 
from __future__ import division

# std lib
import config as cfg 

# datetime
from datetime import timedelta
from datetime import date

# pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction as udf
from pyspark.sql.functions import col
from pyspark.sql.types import * 

class Date_Generator:
    """A date generator Factory 
    """
    def __init__(self, start_date, records_per_increment):
        """Returns a Date Generator Factory
        
        Args:
            start_date (date): start date as valid python date, e.g. `date(2018,01,06)`
            records_per_increment (int | (date) -> int): # records to return before incrementing date
        """
        self.start_date = start_date
        self.records_per_increment = records_per_increment
        self.indx = 0
        self.current_date = start_date

    def date_generator(self):
        if 'function' in str(type(self.records_per_increment)):
            increment_bar = self.records_per_increment(self.current_date)
        else:
            increment_bar = self.records_per_increment

        num_days = int(self.indx / increment_bar)
        current_date = self.start_date + timedelta(days=num_days)

        if current_date != self.current_date:
            self.start_date = current_date
            self.current_date = current_date
            self.indx = 0
        self.indx += 1
        return self.current_date

    def get_date_generator(self):
        """Allows you to bind generator to variable
        
        Returns:
            function: date generator 
        """
        return self.date_generator

def get_string_generator(string):
    def string_generator():
        return string
    
    return string_generator
    
def get_week_generator(string, date_gen):
    def week_generator():
        current_date = date_gen()
        return str(string) + str(current_date.isocalendar()[1])
    return week_generator

def main(num=cfg.NUMBER):
    print("Deubg is {}".format(DEBUG))
    print("Number is {}".format(num))
    spark = SparkSession.builder \
        .appName("DataMocker") \
        .master("local") \
        .getOrCreate()

    df = spark.read.format("csv").options(inferschema="true", header="true").load("data/sales.csv")

    # rename columns to avoid error
    dfcols = df.columns
    for column in dfcols:
        df = df.withColumnRenamed(column, column.replace(".", "_"))
    
    rest_of_teh_columns = df.columns
    rest_of_teh_columns.remove("Week_Day")
    rest_of_teh_columns.remove("Calendar_Year")
    rest_of_teh_columns.remove("Year_Half")
    rest_of_teh_columns.remove("Half_Quarter")
    rest_of_teh_columns.remove("Quarter_Week")

    print("new columns are {}".format(", ".join(rest_of_teh_columns)))

    start_date = date(2017, 10, 1)
    number_of_days = 92
    records_per_day = 59
    total_new_records = number_of_days * records_per_day

    df_population = df.filter(df.Half_Quarter != "2017 Q3")
    fraction_to_sample = total_new_records / df_population.count()
    print(fraction_to_sample)
    df_sample = df_population.sample(False, fraction_to_sample)


    week_day_gen = udf(Date_Generator(
            start_date, 
            records_per_day
        ).get_date_generator(), 
        DateType()
    )
    week_day_gen_for_qtr = Date_Generator(
        start_date, 
        records_per_day
    ).get_date_generator()
    cal_year_gen = udf(get_string_generator(2017), IntegerType())
    yr_half_gen = udf(get_string_generator("2017 HY2"), StringType())
    half_qtr_gen = udf(get_string_generator("2017 Q4"), StringType())
    qtr_wk_gen = udf(get_week_generator("2017 Week ", week_day_gen_for_qtr), 
        StringType()
    )

    df_new = df_sample.select(
        week_day_gen().alias("Week.Day"),
        cal_year_gen().alias("Calendar.Year"),
        yr_half_gen().alias("Year.Half"),
        half_qtr_gen().alias("Half.Quarter"),
        qtr_wk_gen().alias("Quarter.Week"),
        *[col(x) for x in rest_of_teh_columns]
    )
    
    if DEBUG:
        df_new.show()
    else:
        df_new.write.csv("new_data.csv")

if __name__ == "__main__":
    from sys import argv

    # global variables 
    global NUMBER
    NUMBER = cfg.NUMBER
    global DEBUG

    try:
        DEBUG = True if "-t" in argv else cfg.DEBUG

        if "-n" in argv:
            n_indx = argv.index("-n") + 1
            NUMBER = int(argv[n_indx])

        main(NUMBER)
    except Exception as e:
        from sys import exc_info
        print("\n {} \n {} \n".format(e, exc_info()))
        print("""

        USAGE python data_mockery.py [-t] [-n <#>]
        Run to generate data with optional flags:
        -t      to run tests
        -n #    to produce # records

        Run without arguments to generate data
        or add  to run tests

        """)