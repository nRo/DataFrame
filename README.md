# Java DataFrame
An easy-to-use DataFrame Library for Java.


![travis](https://travis-ci.org/nRo/DataFrame.svg?branch=master)
[![codecov](https://codecov.io/gh/nRo/DataFrame/branch/master/graph/badge.svg)](https://codecov.io/gh/nRo/DataFrame)

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
        <version>0.6</version>
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
        <version>0.6.1-SNAPSHOT</version>
    </dependency>
...
</dependencies>
```

Usage
-----
Load DataFrame from a CSV file.
```java
File file = new File("person.csv");
CSVReader reader = CSVReaderBuilder.create(file)
        .withSeparator(';')
        .withHeaderPrefix("#")
        .containsHeader(true)
        .build();

DataFrame users = reader.buildDataFrame()
        .addColumn(new IntegerColumn("person_id"))
        .addColumn(new StringColumn("first_name"))
        .addColumn(new StringColumn("last_name"))
        .addColumn(new StringColumn("address"))
        .addColumn(new IntegerColumn("age"))
        .build();
        
System.out.println(users.getHeader());
for(DataRow row : users)
{
    System.out.println(row);
}
```
Load DataFrame from a file and the corresponding meta file
```java
File meta = new File("person.csv.dfm");
DataFrame dataFrame = DataFrameLoader.load(file,meta);
```
Use indices for fast row access.
Indices must always be unique.
```java

//set the primary key of a data frame
users.setPrimaryKey("person_id");
DataRow firstUser = users.findByPrimaryKey(1)

//add a multi-column index

users.addIndex("name-address","last_name","address");

//returns all users with the last name Smith in the Example-Street 15
List<DataRow> user = users.findByIndex("name-address","Smith","Example-Street 15")
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
dataFrame.getIntegerColumn("age").map(new MapFunction<Integer>() {
    @Override
    public Integer map(Integer value) {
        return value + 2;
    }
});
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
```&&, ||, NOR, XOR, ^(predicate) (negates the predicate) ```

```java
//find all users that are younger than 18 or older users with first_name == "Max"
DataFrame foundUsers = users.select("(age > 18 && first_name == 'Max') OR (age < 18)");


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
for(DataGroup group : grouping)
{
    //print the group description (group values)
    System.out.println(group.getGroupDescription());
    for(DataRow row : group){
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

Join two dataframes using one or more columns.
```java
//join DataFrames users and visits by columns users.person_id == visits.person_id
DataFrame visitors = users.joinLeft(visits,"person_id");

//join DataFrames users and orders by columns users.person_id == orders.customer_id
DataFrame userOrders = users.joinInner(orders,new JoinColumn("person_id","customer_id"));
```

