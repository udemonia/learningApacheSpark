# Scala Notes

Databricks Fundamentals Apache Spark form Udemy

## Dataframes

> the Spark dataframes are remarkably similar to Pandas

> databricks workbooks are remarkably similar to jupyter notebooks

### Reading in a Dataframe

`var dataframeName = spark.read.option("header", true).csv("path-to-file.csv")`

`var customerDfFormatMethod = spark.read.format("csv").load("dbfs:/FileStore/tables/retailer/data/customer.csv")`

### Popular methods and attributes

Show the first 10k rows of a dataframe

`display(dataframe)`

show the columns and datatypes for a dataframe

`dataframeName.show`

print the columns for a dataframe

`fromDatDf.columns`

print how many columns are in the dataframe

`fromDatDf.columns.size`

`dataframe.count`

### filter

you can filter a dataframe with a $ column criteria format

`var sameBirthMonth = customer.filter($"c_birth_month" === 10)`

`var justMyBirthDate = dataframe.filter($"c_birth_day" === 4)`

### File Explore in Databricks

%fs ls /FileStore/tables/retailer/data/customer.csv

### Reading in a CSV as a Dataframe with options

Headers = true

val customerDataFrameWithHeaders = spark.read
.option("header", true) // read the first row as the headers
.csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

%fs head /FileStore/tables/retailer/data/customer.csv

%fs head /FileStore/tables/retailer/data/customer.dat

### Reading in a pipe delimited .dat file

```scala
val fromDatDf = spark.read
.option("header", true)
.option("sep", "|") // THIS IS THE KEY - SEPERATOR = |
.csv("/FileStore/tables/retailer/data/customer.dat")
```

We can also bring in many options with Map

```Scala
val itemDf = spark.read
  .format("csv")
  .options(Map("header" -> "true"
             , "delimiter" -> "|"
             , "inferSchema" -> "true"))
  .load("/FileStore/tables/retailer/data/item.dat")
```

### Selecting Columns

`val customerBirthdays = customerDataFrameWithHeaders.select("c_customer_id", "c_first_name", "c_last_name", "c_birth_year", "c_birth_month", "c_birth_day")`

`import org.apache.spark.sql.functions.{col, column}`

`val customerWithBDayColomnObject = customerDataFrameWithHeaders.select(col("c_birth_day"), col("c_last_name"), column("c_first_name"), $"c_birth_year", 'c_birth_month)`

with the import - import org.apache.spark.sql.functions.{col, column} you can bring in columns with the below

- col("string name of column")

- column("string name of column")

- $"string name of column"

- 'column_name - - - - - one single quote character

### Print the Schema

Show the data schema for the dataframe

`customerDataFrameWithHeaders.printSchema`

By default, the schema will just read in everything as a string.... we can see this by the output of printSchema

```text
root
 |-- c_customer_sk: string (nullable = true)
 |-- c_customer_id: string (nullable = true)
 |-- c_current_cdemo_sk: string (nullable = true)
 |-- c_current_hdemo_sk: string (nullable = true)
 |-- c_current_addr_sk: string (nullable = true)
 |-- c_first_shipto_date_sk: string (nullable = true)
 |-- c_first_sales_date_sk: string (nullable = true)
 |-- c_salutation: string (nullable = true)
 |-- c_first_name: string (nullable = true)
 |-- c_last_name: string (nullable = true)
 |-- c_preferred_cust_flag: string (nullable = true)
 |-- c_birth_day: string (nullable = true)
 |-- c_birth_month: string (nullable = true)
 |-- c_birth_year: string (nullable = true)
 |-- c_birth_country: string (nullable = true)
 |-- c_login: string (nullable = true)
 |-- c_email_address: string (nullable = true)
 |-- c_last_review_date: string (nullable = true)

```

we can either let apache spark infer the schema or set it manually

to let apache spark infer the schema, lets pass an option on reading in a df

```scala
val df1 = spark.read
.option("header", "true") // headers included in the dataframe
.option("inferSchema", true) // infer the schema of the columns - data type inferance
.csv("/FileStore/tables/retailer/data/customer.csv")
```

```text
root
 |-- c_customer_sk: integer (nullable = true)
 |-- c_customer_id: string (nullable = true)
 |-- c_current_cdemo_sk: integer (nullable = true)
 |-- c_current_hdemo_sk: integer (nullable = true)
 |-- c_current_addr_sk: integer (nullable = true)
 |-- c_first_shipto_date_sk: integer (nullable = true)
 |-- c_first_sales_date_sk: integer (nullable = true)
 |-- c_salutation: string (nullable = true)
 |-- c_first_name: string (nullable = true)
 |-- c_last_name: string (nullable = true)
 |-- c_preferred_cust_flag: string (nullable = true)
 |-- c_birth_day: integer (nullable = true)
 |-- c_birth_month: integer (nullable = true)
 |-- c_birth_year: integer (nullable = true)
 |-- c_birth_country: string (nullable = true)
 |-- c_login: string (nullable = true)
 |-- c_email_address: string (nullable = true)
 |-- c_last_review_date: double (nullable = true)
```

**THIS CAN LEAD TO ISSUES:** IN PROD, ALWAYS SET THE SCHEMA MANUALLY

### Manually setting dataframe column schema info with data Description Language strings within Scala

```Scala
val addDfSchemaString = "ca_address_sk long, ca_street_number string, ca_address_id string
```

we can see its one string, with column and data type comma seperated

```Scala
var addDf = spark.read
  .option("sep", "|")
  .option("header", true)
  .option("inferSchema", true)
  .schema(addDfSchemaString) // pass the DDL string to the spark read schema method
  .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
```

👆 we can then pass the ddl string as the argument ot the schema method on spark.read

❗ if you don't pass all the columns in the DDL above they will be omitted.... ❗

❗ running dataframe.printSchema will only show the three columns types we declared above ❗

#### LIFE HACK

We can print the current schema ddl formatted string for any dataframe by calling the .schema.toDDL
attribute

- Dataframes are the most important API

- Dataframes are immutable - we have to copy to change them

- Adding columns to an immutable dataframe

### Adding Columns to a Dataframe

We have to use the .withColumn() method while creating a new dataframe

> in this example, we're creating three boolean fields based on the value of ib_upper_bound

```scala
val incomeBandWithGroups = incomeDF
  .withColumn("isFirstIncomeGroup", incomeDF.col("ib_upper_bound") <= 60000)
  .withColumn("isSecondIncomeGroup", incomeDF.col("ib_upper_bound") > 60000 and incomeDF.col("ib_upper_bound") < 120001)
  .withColumn("isThirdIncomeGroup", incomeDF.col("ib_upper_bound") > 120001 && incomeDF.col("ib_upper_bound") < 200000)

```

## renaming dataframe columns

Because DataFrames are immutable - we can't directly rename a column

Instead, we have to create a new Dataframe while updating the column

.withColumnRenamed(old value, new value)

```Scala
var updatedIncomeDf = incomeBandWithGroups.withColumnRenamed("Unc Sam", "uncSamsCut")
```

### Dropping a Column from a dataframe

Because the dataframes are immutable - we have to create a new dataframe without the column in question

```scala
val NoDemoDf = updatedIncomeDf.drop("demo")
```

```Scala
val droppedMultipleColumns = NoDemoDf.drop("ib_lower_bound", "isFirstIncomeGroup")
```

### Filtering a Dataframe

we can use the filter method with a dollar sign and column expressions

`val customersWithBirthDays = Df1.filter($"c_birth_day" > 0 && $"c_birthday" <= 31)`

You can also stack

```scala
`val customersWithBirthDays = Df1
  .filter($"c_birth_day" > 0)
  .filter($"c_birthday" <= 31)
  .filter($"c_birth_month > 0")
  .filter($"c_birth_month <= 12")
  .filter('c_birth_year.isNotNull)
  .filter('c_birth_year < 0>)`
```

of note, we can use all three methods of referencing a column object.

also, we can change .filter to .where for the same result

```scala
`val customersWithBirthDays = Df1
  .filter($"c_birth_day" > 0)
  .where($"c_birthday" <= 31)
  .filter($"c_birth_month > 0")
  .filter($"c_birth_month <= 12")
  .where('c_birth_year.isNotNull)
  .filter('c_birth_year < 0>)`
```

### Equality Operators in Scala Apache Spark === ( = = = )

Triple equals like JavaScript

`brandonLambert === "person"`

`1 === 1`

### Inequality Operators =!= ( = ! = )

`"Brandon" =!= "Women"`

`1 =!= 2`

## JOINING DATAFRAMES TOGETHER

Joins work similar to SQL

Customer has a c_current_addr_sk column which is the primary key for customer address
we can perform sql like joins on that - inner join to return those two df row values on a match
to do this, we will declare a 'JOIN EXPRESSION' which returns the rows that evaluate to true

Read in two dataframes - _here we'll assume the customer address key is a foreign key in customerDf_

```scala
val customerAddress = spark.read
  .option("inferSchema", true)
  .option("header", true)
  .option("sep", "|")
  .csv("/FileStore/tables/retailer/data/customer_address.dat")

val customerDf = spark.read
  .option("header", true)
  .option("sep", "|")
  .csv("/FileStore/tables/retailer/data/customer.dat")
```

Now we will create an expression to pass to the join method

`val joinExpression = customerDf.col("c_current_addr_sk") === customerAddress.col("ca_address_sk")`

The join expression takes three arguments - the right table (we call the method off the left), the join expression, the join type

the join will evaluate each row against the expression, returning those which evaluate to true

`val customersWithAddressesDf = customerDf.join(customerAddress, joinExpression, "inner")`

optionally, we could call **.select with the join and pass the columns to return from either dataframe**

### Aggregation

df.count to count the records in the dataframe

bring in the count function

`import org.apache.spark.sql.functions.count`

#### count

we have to call .select on the dataframe, in this example, we're passing \* as all, count all

`customerDf.select(count("*")).show`

the two examples above are identical

count how many c_first_name have values

`customerDf.select(count("c_first_name")).show`

would be roughly the same as filter where a value isNotNull

`customerDf.filter($"c_first_name".isNotNull).count`

#### Count Distinct

lets bring in count distinct

`import org.apache.spark.sql.functions.countDistinct`

then we can call .select(countDistinct)

`customerDf.select(countDistinct($"c_first_name")).show`

```text
+----------------------------+
|count(DISTINCT c_first_name)|
+----------------------------+
|                        4131|
+----------------------------+
```

#### min value

Find the product with the smallest price

lets bring in the min function

`import org.apache.spark.sql.functions.min`

similar to the count and countDistinct methods, we call .select(min($"column"))

`itemDf.select(min($"i_wholesale_cost")).show`

we can add on a rename to get a better presentation - this returns a dataframe, we'll store it

`val minDf = itemDf.select(min($"i_wholesale_cost")).withColumnRenamed("min(i_wholesale_cost)", "minCost")`

After pulling back the min cost in the dataframe, we can join the dateframe with the minCost value of the new dataframe, returning just the results w/ that cost

```Scala
val cheapestItemDf = itemDf
  .join(minDf, itemDf.col("i_wholesale_cost") === minDf.col("minCost"), "inner")
```

#### Get the Max Value

bring it in

`import org.apache.spark.sql.functions.max`

call the .select(max($"column_name"))

I've added rename as it tends to give the column a funky name

`val maxItem = itemDf.select(max($"i_wholesale_cost")).withColumnRenamed("max(i_wholesale_cost)", "maxValue")`

now, we can join the original w/ an inner join to display all the records with that value

```scala
val mostExpensiveItem = itemDf
  .join(maxItem, itemDf.col("i_wholesale_cost") === maxItem.col("maxValue"))
```

#### the Sum Function

bring the sum function in

`import org.apache.spark.sql.functions.sum`

call it with .select(sum($"column name"))

Lets add up the entire current price for all items

`itemDf.select(sum($"i_current_price")).show`

#### Sum Distinct

import sum distinct, **we can also pass an object like js to bring in more than one sql.functions at a time**

`import org.apache.spark.sql.functions.{sum, sumDistinct }`

then we can call .select(sumDistinct($"Column name"))

In this example, we're counting how many managers are in the dataset

`itemDf.select(sumDistinct($"i_manager_id")).show`

#### Average and Mean

bring in average and mean

`import org.apache.spark.sql.functions.{avg, mean}`

same syntax as above - notice the .as("New Column Name")

`itemDf.select(avg($"i_current_price").as("Average Price"), mean($"i_current_price").as("Mean Price")).show`

```text
+-----------------+-----------------+
|    Average Price|       Mean Price|
+-----------------+-----------------+
|9.523071010860495|9.523071010860495|
+-----------------+-----------------+
```

we could also get average by dividing the sum by the count

`itemDf.select(sum("i_current_price") / count("i_current_price")).show`

### Group By w/ Aggregates

Group by is an amazing way to format data

For instance, what if we wanted to know how many managers where in each of the divisions?

we could group by division and count managers

```spark
storeDf.groupBy("s_division_id").agg(
  countDistinct("s_manager").as("How Many Managers")
).show
```

```txt
+-------------+-----------------+
|s_division_id|How Many Managers|
+-------------+-----------------+
|            1|                7|
+-------------+-----------------+
```

### User defined functions

In our customer dataframe we don't have a string for birthday - if we wanted to get at birthday, we'd need to query 3 columns

To get around this, we can create a user defined function

```scala
def getCustomerBirthday(year: Int, month: Int, day: Int) : String = {
  return java.time.LocalDate.of(year,month,day).toString
}
```

we can see here that scala in static typed

In order to use the Scala function within the Dataframe, **we have to declare it as a user defined function**

It's best practice to _declare the parameter & return types_ when registering a udf method within databricks/spark session

```Scala
val getCustomerBirthday_udf = udf(getCustomerBirthday(_: Int, _: Int, _:Int): String)
```

lets prepare a selection of the customer dataframe to run the UDF against

```Scala
customerDatDf.select($"c_customer_sk"
                     , concat($"c_first_name", $"c_last_name") as ("c_name")
                    , $"c_birth_year"
                    , $"c_birth_month"
                    ,$"c_birth_day"
                    ).show()
```

```text
+-------------+--------------------+------------+-------------+-----------+
|c_customer_sk|              c_name|c_birth_year|c_birth_month|c_birth_day|
+-------------+--------------------+------------+-------------+-----------+
|            1|Javier           ...|        1936|           12|          9|
|            2|Amy              ...|        1966|            4|          9|
|            3|Latisha          ...|        1979|            9|         18|
|            4|Michael          ...|        1983|            6|          7|
|            5|Robert           ...|        1956|            5|          8|
|            6|Brunilda         ...|        1925|           12|          4|
|            7|Fonda            ...|        1985|            4|         24|
|            8|Ollie            ...|        1938|           12|         26|
|            9|Karl             ...|        1966|           10|         26|
|           10|Albert           ...|        1973|           10|         15|
|           11|Betty            ...|        1963|           12|         18|
|           12|Margaret         ...|        1956|            6|          2|
|           13|Rosalinda        ...|        1970|            3|          1|
|           14|Jack             ...|        1937|            3|         30|
+-------------+--------------------+------------+-------------+-----------+
```

Now, we can get the year, month, day in the same column with our UDF

```Scala
customerDatDf.select(
  getCustomerBirthday_udf($"c_birth_year", $"c_birth_month", $"c_birth_day").as("BirthDay")
  ,$"c_customer_sk"
  , concat($"c_first_name", $"c_last_name") as ("c_name")
  , $"c_birth_year"
  , $"c_birth_month"
  ,$"c_birth_day"
  ).show()
```

```text
+----------+-------------+--------------------+------------+-------------+-----------+
|  BirthDay|c_customer_sk|              c_name|c_birth_year|c_birth_month|c_birth_day|
+----------+-------------+--------------------+------------+-------------+-----------+
|1936-12-09|            1|Javier           ...|        1936|           12|          9|
|1966-04-09|            2|Amy              ...|        1966|            4|          9|
|1979-09-18|            3|Latisha          ...|        1979|            9|         18|
|1983-06-07|            4|Michael          ...|        1983|            6|          7|
|1956-05-08|            5|Robert           ...|        1956|            5|          8|
|1925-12-04|            6|Brunilda         ...|        1925|           12|          4|
|1985-04-24|            7|Fonda            ...|        1985|            4|         24|
|1938-12-26|            8|Ollie            ...|        1938|           12|         26|
|1966-10-26|            9|Karl             ...|        1966|           10|         26|
|1973-10-15|           10|Albert           ...|        1973|           10|         15|
|1963-12-18|           11|Betty            ...|        1963|           12|         18|
+----------+-------------+--------------------+------------+-------------+-----------+
```

## Infra of Spark
