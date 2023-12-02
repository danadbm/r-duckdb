#######################################-
# Analysis of Flight Data using DuckDB
# DAPT 621 Intro to R
#######################################-

### Objective ----
# We'll uncover delay trends with flights in 2022 and early 2023 using a 
# large dataset of airline on-time performance from the 
# [Bureau of Transportation Statistics](https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK&QO_fu146_anzr=b0-gvzr) 
# (compiled dataset from [Kaggle](https://www.kaggle.com/datasets/arvindnagaonkar/flight-delay)).

## Load data ----

### Load needed libraries
# We'll again use the `tidyverse` suite of packages like `dplyr`, `readxl`, and `ggplot2` and others to work with the flight data.

library(dplyr) # data manipulation
library(readxl) # work with Excel data
library(arrow) # work with parquet data
library(lubridate) # work with dates
library(duckdb) # create and use a database
library(DBI) # functions to interface with database, already loaded with duckdb
library(dbplyr) # translate dplyr code to SQL
library(ggplot2) # visualization

### Load lookup tables from excel file

# `read_excel` is a function within the `readxl` package designed to read data from Excel workbooks.
# There are other R packages designed for this (`openxlsx` is another good one for importing and creating workbooks).

 
filepath <- 'lookup_tables.xlsx'
sample <- read_excel(path = filepath, sheet = 'sample')
carriers <- read_excel(path = filepath, sheet = 'carriers')
holidays <- read_excel(path = filepath, sheet = 'holidays')
 

### Load Parquet file
#[Parquet](https://parquet.apache.org/) is an open source, column-oriented data format with efficient data compression created by Apache.

 
flight_data <- read_parquet('flight_delay_2022_2023.parquet')
 

## Data Wrangling on Sample ----

### Join data with `left_join()`
# Use a join to bring in carrier names and identify holidays. 
# A left join will retain all rows from the first listed table.

#### Add Carrier Names

sample <- left_join(
    x = sample,
    y = carriers,
    by = join_by(Marketing_Airline_Network == Code)
)
 
sample %>% select(Marketing_Airline_Network, CarrierName) %>% head(5)
 

#### Holidays

sample <- left_join(
    x = sample,
    y = holidays,
    by = join_by(FlightDate == Date)
)
 

sample %>% 
    group_by(Holiday) %>% 
    summarise(
        num_flights = n()
    )
 

### Add day of week with `lubridate`

# The `lubridate` package has many good functions to make working with dates easier. 
# The `wday()` function will identify the day of the week from a date.

#### Example

# get today's date
date_today <- today()

# return the numeric day of the week
wday(date_today)

# return the labeled day of the week
wday(date_today, label = TRUE)
 

#### Add DayofWeek column to the data

sample <- sample %>% 
    mutate(
        DayofWeek = wday(FlightDate, label = TRUE)
    )

sample %>% 
    select(FlightDate, DayofWeek) %>% 
    head(5)
 

## Create a database of the data ----

### Set up database connection

# DuckDB and SQLite only needs a path to the database. Here, ":memory:" is a special path that 
# creates an in-memory database. Other database drivers will require more details (like user, password, host, port, etc.).

 
drv <- duckdb() # driver
con <- DBI::dbConnect(drv) # connection
 

### Write the carriers, holidays, and flight_data tables

 
DBI::dbWriteTable(conn = con, name = 'carrier_tbl', value = carriers)
DBI::dbWriteTable(conn = con, name = 'holiday_tbl', value = holidays)

DBI::dbWriteTable(conn = con, name = 'flights', value = flight_data)
# if error, try flight_data <- as.data.frame(flight_data) before writing table to convert from tibble to data.frame object
 

### List tables in the database

 
dbListTables(con)
 

### Read a table from the database

# View as a data frame.

 
con %>% 
    dbReadTable('flights') %>% 
    head(5)
 

# View as a tibble (tidyverse data frame format).

 
con %>% 
    dbReadTable('flights') %>% 
    as_tibble() %>% 
    head(5)
 

## Use SQL to query ----

# Now that tables exist in a database, SQL can be used to query the data.

 
sql <- "
    SELECT * 
    FROM flights 
    WHERE OriginCityName = 'Richmond, VA'
    LIMIT 10
"
 

 
dbGetQuery(con, sql)
 

 
dbGetQuery(con, sql) %>% as_tibble()
 

## Use `dbplyr` to query ----

# `dbplyr` is a `dplyr` backend that allows you to write with dplyr syntax while the backend translates this R code to SQL.

### Step 1 - `tbl()`

# In order to use `dbplyr`, a database table first needs to be converted to a database `tbl()` object in R.

 
flights_db <- tbl(con, 'flights')
 

### Use dplyr syntax to query

 
flights_db %>% 
    filter(OriginCityName == 'Richmond, VA') %>% 
    head(10)
 

### Use dplyr syntax to query & show generated SQL

# Query to return full table.

 
flights_db %>% 
    show_query()
 

# Query from above.

 
flights_db %>% 
    filter(OriginCityName == 'Richmond, VA') %>% 
    head(10) %>% 
    show_query()
 

### Save query results back into R as data frame/tibble

# To save the results back into R, use the `collect()` function. 
# This is calling the `dbGetQuery()` function used earlier in order to extract the results.

 
flights_ric <- flights_db %>% 
    filter(OriginCityName == 'Richmond, VA') %>%
    collect()
 

## Analysis ----

### View average departure delay by carrier code

# First, let's see what the SQL query would look like.

 
flights_db %>% 
  group_by(Marketing_Airline_Network) %>% 
  summarise(
    avg_dep_delay = mean(DepDelayMinutes, na.rm = TRUE)
  ) %>% 
  arrange(desc(avg_dep_delay)) %>% 
  show_query()
 

# Now let's view the results in a table.

 
flights_db %>% 
    group_by(Marketing_Airline_Network) %>% 
    summarise(
        avg_dep_delay = mean(DepDelayMinutes, na.rm = TRUE)
    ) %>% 
    arrange(desc(avg_dep_delay))
 

# View with a visualization.

 
flights_db %>%
    group_by(Marketing_Airline_Network) %>%
    summarise(avg_dep_delay = mean(DepDelayMinutes, na.rm = TRUE)) %>%
    arrange(desc(avg_dep_delay)) %>%
    ggplot(aes(x = reorder(Marketing_Airline_Network, avg_dep_delay), y = avg_dep_delay)) +
    geom_bar(stat = "identity", fill = 'darkblue') +
    geom_text(aes(label = round(avg_dep_delay, 1)),  # Add rounded labels
              position = position_dodge(width = 0.9),  # Adjust position to align with bars
              hjust = -0.1,  # Adjust horizontal position to be outside of the bar
              size = 3) +  # Set text size
    coord_flip() +  # Flips the coordinates to make horizontal bars
    theme_minimal() +
    labs(title = "Average Departure Delay by Airline", 
         x = "Carrier Code", 
         y = "Average Departure Delay (minutes)")
 

### View average departure delay by carrier name

# In order to bring in carrier name, we need to join to the carriers table like we did with the sample data.

#### Use SQL code to query

 
sql <- "
SELECT c.CarrierName, AVG(f.DepDelayMinutes) AS avg_dep_delay
FROM flights f
LEFT JOIN carrier_tbl c ON f.Marketing_Airline_Network = c.Code
GROUP BY CarrierName
ORDER BY avg_dep_delay DESC
"
 

 
dbGetQuery(con, sql)
 

#### Use `dbplyr` to query

# In order to join on the carriers table, we want to convert the carriers table into a database object using `tbl()`.

 
carriers_db <- tbl(con, 'carrier_tbl')
 

# Next, we can use `dplyr` syntax to join and calculate average departure delay by carrier name. 
# Let's write our code and look at our query.

 
flights_db %>% left_join(
    x = flights_db,
    y = carriers_db,
    by = join_by(Marketing_Airline_Network == Code)
    ) %>% 
  group_by(CarrierName) %>%
  summarise(avg_dep_delay = mean(DepDelayMinutes, na.rm = TRUE)) %>%
  arrange(desc(avg_dep_delay)) %>% 
  show_query()
 

# Now let's execute to look at the results as a table and then as a visualization.

 
flights_db %>% left_join(
    x = flights_db,
    y = carriers_db,
    by = join_by(Marketing_Airline_Network == Code)
) %>% 
    group_by(CarrierName) %>%
    summarise(avg_dep_delay = mean(DepDelayMinutes, na.rm = TRUE)) %>%
    arrange(desc(avg_dep_delay))
 

 
flights_db %>% left_join(
    x = flights_db,
    y = carriers_db,
    by = join_by(Marketing_Airline_Network == Code)
) %>% 
    group_by(CarrierName) %>%
    summarise(avg_dep_delay = mean(DepDelayMinutes, na.rm = TRUE)) %>%
    arrange(desc(avg_dep_delay)) %>% 
    ggplot(aes(x = reorder(CarrierName, avg_dep_delay), y = avg_dep_delay)) +
    geom_bar(stat = "identity", fill = 'darkblue') +
    geom_text(aes(label = round(avg_dep_delay, 1)),  # Add rounded labels
              position = position_dodge(width = 0.9),  # Adjust position to align with bars
              hjust = -0.1,  # Adjust horizontal position to be outside of the bar
              size = 3) +  # Set text size
    coord_flip() +  # Flips the coordinates to make horizontal bars
    theme_minimal() +
    labs(title = "Average Departure Delay by Airline", 
         x = "Carrier Name", 
         y = "Average Departure Delay (minutes)")
 

### Class Exercise

# Create a visualization showing average departure delay by month in 2022.

## Disconnect from DB ----

# When finished with querying the database, disconnect.

 
dbDisconnect(con)
 