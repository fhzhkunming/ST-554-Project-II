# ST-554-Project-II

This repo is for ST554 project 2.

In Part I, I developed a custom Python class, **SparkDataCheck**, which includes two `classmethods` for creating class instances, three validation methods that append Boolean columns to a Spark DataFrame, and two summarization methods that return pandas DataFrames. To demonstrate that the class works correctly on real data, I will use the [Air Quality dataset](https://archive.ics.uci.edu/dataset/360/air+quality) from the UCI Machine Learning Repository.

I began by importing my Python script and reading the downloaded air quality CSV file. Using my `from_csv` classmethod, I created a `SparkDataCheck` object and applied each of my methods to this dataset. For each method, I provided four to five examples, including cases where the method prints warning messages (e.g., when a column does not exist or is the wrong type).

Next, I read the same dataset using pandas and used my `from_pandas` classmethod to create a second instance of the class. I then demonstrated one method on this pandas‑based object to confirm that both classmethods work as intended.

In Part II, I used `pandas`‑on‑Spark and Spark SQL to perform a brief exploratory analysis of NFL weekly data. After loading and inspecting the dataset, I focused on quarterback (QB) statistics from the 2005–2023 regular seasons, computed season‑level summaries for each player, created two derived performance metrics, and ranked players based on these measures. I then repeated the workflow using Spark SQL to compare results across the two APIs. The specific steps and instructions are outlined below. 

- Read in the weekly nfl data (csv file is available at the project page and needs to upload it to JupyterHub)
- Check out the first 5 rows of the `DataFrame`
- Report all of the column names
- We want to only look at QB stats for the seasons 2005 to 2023 (inclusive).\
    – Subset the rows of the data to only include the position “QB”, the regular season (“REG”), and season in the range noted above\
    – Subset the columns to only include the `player_display_name`, `season`, `week`, `completions`, `attempts`, `passing_yards`, `passing_tds`, and `interceptions`\
    – For each `player_display_name` and `season` combination, fine the _sum_ and *mean* of each of the statistical quantities (the rest of the columns we chose above)\
    – Create two new variables (by season/player combination):\
        ∗ `completion_percentage` = (sum of completions)/(sum of attempts)\
        ∗ `td_int_ratio` = (sum passing tds)/(sum interceptions)
- Save the result of above as an object. With that object
    – Subset the rows to only include player/season combinations wher ethe sum of attempts is at least 50.\
    – Sort the rows descending by `completion_percentage` and report the first 40 values!\
    – Sort the rows descending by `td_int_ratio` and report the first 40 values!
- Repeat the above completely using te Spark SQL DataFrame, including reading in the data!\
    – Note: the `td_int_ratio` values are treated differently between `pandas`-on-Spark and Spark SQL. Note this difference when it happens!

Throughout the analysis, I compared the behavior of pandas‑on‑Spark and `Spark SQL`, noting differences in syntax, column creation, error handling, and sorting behavior for missing values. Both methods produced consistent results, but Spark SQL required more explicit transformations and followed SQL semantics for `NULL` handling.
