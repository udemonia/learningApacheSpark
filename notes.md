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
