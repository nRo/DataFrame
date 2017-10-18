# Java DataFrame
An easy-to-use DataFrame Library for Java.


![travis](https://travis-ci.org/nRo/DataFrame.svg?branch=master)
[![codecov](https://codecov.io/gh/nRo/DataFrame/branch/master/graph/badge.svg)](https://codecov.io/gh/nRo/DataFrame)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/a03214f7198f4461ab341adecb75e0da)](https://www.codacy.com/app/nRo/DataFrame?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=nRo/DataFrame&amp;utm_campaign=Badge_Grade)

Documentation
-------
[![Javadocs](http://javadoc.io/badge/de.unknownreality/dataframe.svg?color=blue)](http://javadoc.io/doc/de.unknownreality/dataframe)

Install
-------

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.unknownreality/dataframe/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.unknownreality/dataframe)


Add this to you pom.xml

```xml
<dependencies>
...
    <dependency>
        <groupId>de.unknownreality</groupId>
        <artifactId>dataframe</artifactId>
        <version>0.7.2</version>
    </dependency>
...
</dependencies>
```

Build
-----
To build the library from sources:

1) Clone github repository

    $ git clone https://github.com/nRo/DataFrame.git

2) Change to the created folder and run `mvn install`

    $ cd DataFrame
    
    $ mvn install

3) Include it by adding the following to your project's `pom.xml`:

```xml
<dependencies>
...
    <dependency>
        <groupId>de.unknownreality</groupId>
        <artifactId>dataframe</artifactId>
        <version>0.7.5-SNAPSHOT</version>
    </dependency>
...
</dependencies>
```
Version 0.7.5
-----
-  **direct value access for DataRow object.**

   DataRows now directly access the respective values from the columns.  
   This improves runtime and memory footprint for most DataFrame operations.
   DataRow objects are invalidated once the source DataFrame is changed.  
   Accessing an invalidated row results in an exception
- Row collections are now return as DataRows object.  
    DataRows can be converted to a new DataFrame
- improved 'groupBy' method


Version 0.7
-----
- The read and write functions have been rewritten from scratch for this version.
Some existing methods have been removed.

- Data grouping has been refactored and aggregation functions can now be applied to data groupings.
In general, data groupings can now be used like normal DataFrames.

- Java 8 is now required.

- Empty DataFrame instances are now created using DataFrame.create()

Examples
-----
Select all users called Meier or Schmitt from Germany, group by age and add column that contains the number of users with the respective age.
Then sort by age and print
```java
URL csvUrl = new URL("https://raw.githubusercontent.com/nRo/DataFrame/master/src/test/resources/users.csv");

DataFrame users = DataFrame.load(csvUrl, FileFormat.CSV);

users.select("(name == 'Schmitt' || name == 'Meier') && country == 'Germany'")
        .groupBy("age").agg("count",Aggregate.count())
        .sort("age")
        .print();

/*
    age count
    20   1
    24   2
    30   2
 */

```
Load a csv file, set a unique column as primary key and add an index for two other columns.
Select rows using the previously created index, change the values in their NAME column and join them with the original DataFrame.
```java
URL csvUrl = new URL("https://raw.githubusercontent.com/nRo/DataFrame/master/src/test/resources/data_index.csv");

DataFrame dataFrame = DataFrame.load(csvUrl, FileFormat.CSV);

dataFrame.setPrimaryKey("UID");
dataFrame.addIndex("id_name_idx","ID","NAME");

DataRow row = dataFrame.selectByPrimaryKey(1);
System.out.println(row);
//1;A;1

DataFrame idxExample = dataFrame.selectByIndex("id_name_idx",3,"A");

idxExample.print();
/*
    ID	NAME	UID
    3	A	4
    3	A	8
 */
idxExample.getStringColumn("NAME").map((value -> value + "_idx_example"));
idxExample.print();
/*
    ID	NAME	UID
    3	A_idx_example	4
    3	A_idx_example	8
 */

dataFrame.joinInner(idxExample,"UID").print();
/*
    ID.A    NAME.A	UID	ID.B	NAME.B
    3   A   4	3   A_idx_example
    3   A   8	3   A_idx_example
 */

```

Usage
-----
Load DataFrame from a CSV file.
Column types will be detected automatically. (String, Double, Integer, Boolean)
```java
File file = new File("person.csv");
DataFrame users = DataFrame.fromCSV(file, ';', true);
```

Load a DataFrame with custom options and predefined column types.
```java
File file = new File("person.csv");

CSVReader csvReader = CSVReaderBuilder.create()
                .containsHeader(true)
                .withHeaderPrefix("#")
                .withSeparator(';')
                .setColumnType("person_id", Integer.class)
                .setColumnType("first_name", String.class)
                .setColumnType("last_name", String.class)
                .setColumnType("age", Integer.class).build();

DataFrame users = DataFrame.load(file,csvReader);
        
System.out.println(users.getHeader());
for(DataRow row : users)
{
    System.out.println(row);
}
```
DataFrames can be written using default formats (CSV or TSV).
Additionally it is possible to set different options when writing DataFrames.
```java
dataFrame.write(file); // TSV per default

dataFrame.write(file, FileFormat.CSV);

dataFrame.writeCSV(file, ';',true); // use ';' as separator and include the header


CSVWriter csvWriter = CSVWriterBuilder.create()
                .withHeader(true)
                .withSeparator('\t')
                .useGzip(true).build();
dataFrame.write(file, csvWriter);
 ```
If a meta file is written for a DataFrame, it can simply be loaded by pointing at the DataFrame file.
The meta has the same path as the DataFrame file with '.dfm' extension
```java
File file = new File("dataFrame.csv");
dataFrame.write(file);
DataFrame loadedDataFrame = DataFrame.load(file);
```

Values within a DataFrame are accessed using DataRow objects.
If the source DataFrame changes after a DataRow object is created, the DataRow is invalidated and can no
longer be accessed.

```java

for(DataRow row : dataFrame){
    ... = row.getInteger("id");
}


DataRows rows = dataFrame.getRows();

//returns the value within the id column in the first row
rows.get(0).getInteger("id");

dataFrame.sort("name");

//The DataFrame was sorted after the DataRows were obtained.
//The first row can now differ. 
//To avoid these effects, a RuntimeException is thrown
//if a row that was created before the DataFrame is altered is accessed

rows.get(0).getInteger("id"); //throws exception

rows = dataFrame.getRows();

//The DataRows is now valid again and rows can be accessed
rows.get(0).getInteger("id");


//DataRows can be converted to a new independet DataFrame.
//changes to the original DataFrame have no effect on the new DataFrame.
DataFrame dataFrame2 = rows.toDataFrame();

dataFrame.sort("id");

dataFrame.getRow(0).getInteger("id"); // no exception
```

DataRows can be used to change values within a DataFrame

```java
DataRows rows = dataFrame.getRows();

//sets the value in the second row in the name column to 'A'
rows.get(1).set("name","A");

//sets the value in the second row in the first column to 'A'
rows.get(1).set(0,"A");


```

Use indices for fast row access.
```java

//set the primary key of a data frame
users.setPrimaryKey("person_id");
DataRow firstUser = users.selectByPrimaryKey(1)

//add a multi-column index

users.addIndex("name-address","last_name","address");

//returns rows containing all users with the last name Smith in the Example-Street 15
DataRows user = users.selectRowsByIndex("name-address","Smith","Example-Street 15")
```
It is possible to define and use other index types.
The following example shows interval indices.
This index type requires two number columns, start and end.
The index can then be used to find rows where start and end value overlap with a region specified
by two number values. It is also possible to find rows where the region defined by start and end contains a certain value.
```java
 DataFrame dataFrame = DataFrame.create()
                .addStringColumn("name")
                .addIntegerColumn("start")
                .addIntegerColumn("end");
dataFrame.append("A",1,3);
dataFrame.append("B",2,3);
dataFrame.append("C",4,5);
dataFrame.append("D",6,7);
IntervalIndex index = new IntervalIndex("idx",
    dataFrame.getNumberColumn("start"),
    dataFrame.getNumberColumn("end"));
dataFrame.addIndex(index);

//returns a new dataframe containing all rows where (start,end) overlaps with (1,3)
// -> A, B
DataFrame df = dataFrame.selectByIndex("idx",1,3);

//rows where (start,end) overlaps with (4,5)
// -> C
dataFrame.selectByIndex("idx",4,5);

//rows where (start,end) contains 2.5
// -> A, B
dataFrame.selectByIndex("idx",2.5);
```

Perform operations on columns.
```java
//max value of column "person_id"
users.getIntegerColumn("age").max();

DoubleColumn dc1 = ...;
DoubleColumn dc2 = ...;
//add one column to another
dc1.add(dc2);

//multiply each value with 2
dc1.multiply(2);

//Use MapFunction to convert all values in a row
dataFrame.getIntegerColumn("age").map(value -> value + 2);
```

Filter and select rows using predicates.
```java
//keep users with age between 18 and 60
users.filter(FilterPredicate.btwn("age",18,60));

//find all users with age > 18 an first_name == "Max"
DataFrame foundUsers = users.select(
      FilterPredicate.and(
              FilterPredicate.gt("age",18),
              FilterPredicate.eq("first_name","Max")
));

```
Create and compile predicates from strings \
Available value comparison operations: \
```==, !=, <, <=, >, >=, ~= (regex)``` \
Available predicate operations: \
```&&, ||, NOR, XOR, !(predicate) (negates the predicate) ```

```java
//find all users that are younger than 18 or older users with first_name == "Max"
DataFrame foundUsers = users.select("(age > 18 && first_name == 'Max') OR (age < 18)");

//Boolean column filter
//find all users that are older than 18 or the selected column is true
DataFrame foundUsers = users.select("(age > 18) OR selected");

//find all users the selected column is false
DataFrame notSelected = users.select("!selected");

//compare tow columns
//returns all rows where col1 equals col2
FilterPredicate.eqColumn("col1","col2")

//Get all users where first name does not equal the last name
//Column comparisons require '.' as prefix
DataFrame dataframe = users.select(".first_name != .last_name");


// regex filter
//find all users where the street begins with A, B or C followed by lowercase characters
DataFrame foundUsers = users.select("street ~= /[ABC][a-z]+/");
```

Sort rows by one or more columns.
```java
//sort by column "person_id" (ascending)
users.sort("person_id", SortColumn.Direction.Ascending);

//sort by "last_name" und "first_name"
users.sort(
   new SortColumn("last_name", SortColumn.Direction.Descending),
   new SortColumn("first_name", SortColumn.Direction.Descending)
);
```

Group dataframes using one or more columns.
```java

//group by "age" and "first_name"
DataGrouping grouping = users.groupBy("age","first_name");


//iterate through all found groups
for(DataRow row : grouping){
    DataGroup group = grouping.getGroup(row.getIndex());
    //print the group description (group values)
    System.out.println(group.getGroupDescription());
   
    //iterate through all rows from the respective groups
    for(DataRow groupRow : group){
            System.out.println(row);
     }
}
```
Direct access to groups in a grouping.
```java

//group by "age" and "first_name"
DataGrouping grouping = users.groupBy("age","first_name");

//Get all users that are called John and are 18 years old
DataGroup group = grouping.findByGroupValues(18, "John");
```
It is possible to apply aggregation function to data groups.
In this example, a column "max_age" is added to the grouping DataFrame. 
This column contains the maximum value of the "age" column of the respective rows in the original DataFrame.
The resulting grouping DataFrame contains two columns: "first_name" and "max_age"
```java
DataGrouping grouping = users.groupBy("first_name").agg("max_age", Aggregate.max("age"));

//some other aggregate functions
grouping.agg("mean_age", Aggregate.mean("age"));

grouping.agg("median_age", Aggregate.median("age"));

grouping.agg("25quantile_age", Aggregate.quantile("age", 0.25));

grouping.agg("older_30_count", Aggregate.filterCount("age > 30"));

//custom aggregate function
grouping.agg("org_percentage", group -> group.size() / users.size());

```
Join two dataframes using one or more columns.
```java
//join DataFrames users and visits by columns users.person_id == visits.person_id
DataFrame visitors = users.joinLeft(visits,"person_id");

//join DataFrames users and orders by columns users.person_id == orders.customer_id
DataFrame userOrders = users.joinInner(orders,new JoinColumn("person_id","customer_id"));
```

