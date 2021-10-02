# Scala Notes

## Dataframes

var dataframeName = spark.read.option("header", true).csv("path-to-file.csv")

var cusotmerDfFormatMethod = spark.read.format("csv").load("dbfs:/FileStore/tables/retailer/data/customer.csv")

display(dataframe)

dataframeName.show

fromDatDf.columns

fromDatDf.columns.size

dataframe.count

### filter

var sameBirthMonth = customer.filter($"c_birth_month" === 10)


var justMyBirthDate = dataframe.filter($"c_birth_day" === 4)

%fs ls /FileStore/tables/retailer/data/customer.csv

val customerDataFrameWithHeaders = spark.read
  .option("header", true) // read the first row as the headers
  .csv("dbfs:/FileStore/tables/retailer/data/customer.csv")

%fs head /FileStore/tables/retailer/data/customer.csv

%fs head /FileStore/tables/retailer/data/customer.dat

val fromDatDf = spark.read
  .option("header", true)
  .option("sep", "|") // THIS IS THE KEY - SEPERATOR = |
  .csv("/FileStore/tables/retailer/data/customer.dat")

### Selecting Columns

val customerBirthdays = customerDataFrameWithHeaders.select("c_customer_id", "c_first_name", "c_last_name", "c_birth_year", "c_birth_month", "c_birth_day")

import org.apache.spark.sql.functions.{col, column}
val customerWithBDayColomnObject = customerDataFrameWithHeaders.select(col("c_birth_day"), col("c_last_name"), column("c_first_name"), $"c_birth_year", 'c_birth_month)

// with the import - import org.apache.spark.sql.functions.{col, column}
// you can bring in columns with the below
// col("string name of column")
// column("string name of column")
// $"string name of column"
// 'column_name - - - - - one single quote character

### Print the Schema

customerDataFrameWithHeaders.printSchema

By default, the schema will just read in everything as a string.... we can see this by the output of printSchema

```
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

val df1 = spark.read
.option("header", "true") // headers included in the dataframe
.option("inferSchema", true) // infer the schema of the columns - data type inferance
.csv("/FileStore/tables/retailer/data/customer.csv")

```
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

THIS CAN LEAD TO ISSUES: IN PROD, ALWAYS SET THE SCHEMA MANUALLY

Manaully setting dataframe column schema info with data Description Langague strings within Scala


```
val addDfSchemaString = "ca_address_sk long, ca_street_number string, ca_address_id string
```
we can see its one string, with column and data type comma seperated

```
var addDf = spark.read
  .option("sep", "|")
  .option("header", true)
  .option("inferSchema", true)
  .schema(addDfSchemaString) // pass the DDL string to the spark read schema method
  .csv("dbfs:/FileStore/tables/retailer/data/customer_address.dat")
```

👆 we can then pass the ddl string as the arguement ot the schema method on spark.read

⚠️   if you don't pass all the columns in the DDL above they will be omitted....

running dataframe.printSchema will only show the three columns types we declared above  ⚠️

#### LIFE HACK

We can print the current schema ddl formatted string for any dataframe by calling the .schema.toDDL
attribue

## Dataframes are the most important API

## Dataframes are immutable - we have to copy to change them

## Adding columns to an immutable dataframe

We have to use the .withColumn() method while creating a new dataframe

> in this example, we're creating three boolean fields based on the value of ib_upper_bound

```
val incomeBandWithGroups = incomeDF
  .withColumn("isFirstIncomeGroup", incomeDF.col("ib_upper_bound") <= 60000)
  .withColumn("isSecondIncomeGroup", incomeDF.col("ib_upper_bound") > 60000 and incomeDF.col("ib_upper_bound") < 120001)
  .withColumn("isThirdIncomeGroup", incomeDF.col("ib_upper_bound") > 120001 && incomeDF.col("ib_upper_bound") < 200000)

```


## renaming dataframe columns

Because DataFrames are immutable - we can't directly rename a column

Instead, we have to create a new Dataframe while updating the column

.withColumnRenamed(old value, new value)

```
var updatedIncomeDf = incomeBandWithGroups.withColumnRenamed("Unc Sam", "uncSamsCut")
```
















