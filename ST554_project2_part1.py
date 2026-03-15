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
    def from_csv(cls, spark, path):
        df = (spark.read
                   .format("csv")
                   .option("header", True)
                   .option("inferSchema", True)
                   .option("sep", sep)
                   .load(path))
        return cls(df)
   
  
    #==============================================================
    # Classmethod 2: create instance from a pandas DataFrame
    @classmethod
    def from_pandas(cls,spark, pandas_df):
        df = spark.createDataFrame(pandas_df)
        return cls(df)
    

    #============================================
    # 1. Validation methods
    #============================================

    # 1.1 create boolean column based on numberic bounds
    from pyspark.sql.functions import col as spark_col

    def check_numeric_range(self, col: str, lower: float = None, upper: float = None):
        """
        Append a Boolean column indicating whether values in a numeric column
        fall within user-defined lower and/or upper bounds (inclusive).
        NULL values remain NULL.
        Modifies self.df and returns self for method chaining.
        """
        # -----------------------------------
        # check if the column exists
        # -----------------------------------
        if col not in self.df.columns:
            print(f"Column '{col}' does not exist.")
            return self

        # -----------------------------------
        # check if the columin is numeric
        #------------------------------------
        dtype = self.df.schema[col].dataType
        if not isinstance(dtype, NumericType):
            print(f"Column '{col}' is not numeric.")
            return self

        #--------------------------------------
        # ensure at least one bound is provided
        #--------------------------------------
        if lower is None and upper is None:
            print("No bounds provided. Please provide at least one bound.")
            return self

        #--------------------------------------------------------
        # build the Boolean condition
        # Spark automatically returns NULL when the input is NULL
        #--------------------------------------------------------
        if lower is not None and upper is not None:
            # use Spark's between() when both bounds exist
            condition = spark_col(col).between(lower, upper)
        elif lower is not None:
            # only lower bound provided
            condition = spark_col(col) >= lower
        elif upper is not None:
            # only upper bound provided
            condition = spark_col(col) <= upper

        #---------------------------------
        # append Boolean column to dataframe
        #---------------------------------
        new_col_name = f"{col}_in_range"
        self.df = self.df.withColumn(new_col_name, condition)
        return self
    
    #----------------------------------------------------------------
    # 1.2 create a method checking values fall within a set of levels
    #----------------------------------------------------------------
    # import modules needed
    from pyspark.sql.functions import col as spark_col
    from pyspark.sql.types import StringType

    def check_value_levels(self, col: str, levels):
        """
        Check whether values in a string column fall within 
        a user-specified set of allowed levels. 
        Appends a Boolean column. NULL values remain NULL.
        Modifies self.df and returns self for method chaining. 
        """
        # -----------------------------------
        # check if the column exists
        # -----------------------------------
        if col not in self.df.columns:
            print(f"Column '{col}' does not exist.")
            return self

        # -----------------------------------
        # check if the columin is string
        #------------------------------------
        dtype = self.df.schema[col].dataType
        if not isinstance(dtype, StringType):
            print(f"Column '{col}' is not a string column.")
            return self


        # build the Boolean condition
        condition = spark_col(col).isin(levels)
        # append the new Boolean column
        new_col_name = f"{col}_in_levels"
        self.df = self.df.withColumn(new_col_name, condition)
        return self
    
    
    #======================================
    # 2. Summarization methods
    #======================================
    
    # ----------------------------------------------------------------------------------
    # 2.1 define a method to report min and max of a numeric coulumn supplied by the user
    # ----------------------------------------------------------------------------------
    # import modules needed
    from pyspark.sql.functions import min, max  
    from pyspark.sql.types import NumericType   
    from functools import reduce
    import pandas as pd

    def min_max(self, col = None, group = None):
        """
        Report min and max for:
        1. a user-supplied numeric column (grouped if provided)
        2. or all numeric columns if no column is supplied (grouped if provided)
        """
        #-------------------------------------
        # scenario 1: User supplies a column
        #-------------------------------------
        if col is not None:
            # check column exists
            if col not in self.df.columns:
                print(f"Column {col} don't exist.")
                return None

            # get the data type of the column
            dtype = self.df.schema[col].dataType

            # check if column is numeric
            if not isinstance(dtype, NumericType):
                print(f"Column '{col}' is not numeric.")
                return None

            # grouped version for a single numeric column
            if group is not None:
                return(
                    self.df.groupBy(group)
                           .agg(min(col).alias(f"{col}_min"),
                               max(col).alias(f"{col}_max"))
                )

            # ungrouped version for a single numeric column      
            return self.df.select(
                min(col).alias(f"{col}_min"),
                max(col).alias(f"{col}_max")
            )

        #----------------------------------------------------------------
        # scenario 2: no column supplied: compute for all numeric columns
        #----------------------------------------------------------------
        # identify all numeric columns
        numeric_cols = [
            field.name
            for field in self.df.schema.fields
            if isinstance(field.dataType, NumericType)]

        # if no numeric columns exist, print a message
        if not numeric_cols:
            print("No numeric columns found.")
            return None

        # grouped version for all numeric columns
        if group is not None:
            # setup an enpty list
            dfs = []
            # compute grouped min/max for each numeric column 
            for c in numeric_cols:
                df_c = (
                    self.df.groupBy(group)
                           .agg(min(c).alias(f"{c}_min"),
                               max(c).alias(f"{c}_max"))
                ).toPandas()
                dfs.append(df_c)

            # merge all grouped results into on DataFrame
            merged = reduce(lambda left, right: pd.merge(left, right, on = group),dfs)
            return merged

        # ungrouped scenartion for all numeric columns
        agg_exprs = []
        for c in numeric_cols:
            agg_exprs.extend([min(c).alisa(f"{c}_min"),
                             max(c).alisa(f"{c}_max")])
        # Return a DataFrame with all min/max values
        return self.df.select(*agg_exprs)    


        