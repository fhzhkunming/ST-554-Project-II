"""
SparkDataCheck.py

This module define a class that works 
on Spark SQL style data frames.
"""
# import modules needed
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

class SparkDataCheck:
    """
    A data qulity class for Spark SQL style data frames
    """
    
    def __init__(self, df: DataFrame):
        # create a .df attribute
        self.df = df
    
    #==============================================================
    # Classmethod 1: create instance by reading a CSV file
    @classmethod
    # create method for creating an instance from a pandas dataframe
    def from_csv(cls, spark, path):
        df = (spark.read
                   .format("csv")
                   .option("header", True)
                   .option("inferSchema", True)
                   .load(path))
        return cls(df)
   
  
    #==============================================================
    # Classmethod 2: create instance from a pandas DataFrame
    def from_pandas(cls,spark, pandas_df):
        df = spark.createDataFrame(pandas_df)
        return cls(df)
    
    
    
    
    
    #############
    # validation methods
    
    # create boolean column based on numberic bounds
    def check_numeric_range(self, column: str, lower: str = None, upper: str = None):
        """
         Append Boolean column indicating whether numeric values fall within bounds.
        """
        