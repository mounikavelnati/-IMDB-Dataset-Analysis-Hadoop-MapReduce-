Project Overview
This project implements MapReduce for analyzing an IMDb dataset and utilizes SQL query processing to extract meaningful insights. The goal is to process movie data, categorize it based on year and genre, and analyze trends using visualization techniques.

Technologies Used
  Hadoop MapReduce
  HDFS (Hadoop Distributed File System)
  Java for MapReduce implementation
  SQL Query Processing
  Data Visualization (Excel/Spreadsheet Analysis)
  
Project Implementation
Task 1 – MapReduce Analysis
Developed a MapReduce job to process the IMDb dataset.
Categorized movie count based on year range:
[2000-2006], [2007-2013], [2014-2020]
Grouped movies by genre combinations:
Comedy;Romance, Action;Drama, Adventure;Sci-Fi

Generated trend analysis graphs based on the MapReduce output.
Action;Drama had the highest movie count during the 2014-2020 period.
Comedy;Romance was the most popular genre across all time ranges.
Adventure;Sci-Fi had the lowest movie count in the 2000-2006 period.

Histogram: Action;Drama growth over the years.
Pie Chart: Genre distribution (2000-2006).
Stacked Graph: Genre trends across different periods.
Task 2 – SQL Query Processing
Created SQL queries to extract key data from the IMDb dataset.
Generated an output spool file to store query results.
