# DataUtil
DataFrame Library for Java.
## Features
- create DataFrame from CSV files
- different column types (DoubleColumn, IntegerColumn,...)
- operations on columns
  * map, median
  * mean, max, min (Number columns)
  * and, or, nor, xor (Boolean columns)
- search in DataFrame with Predicates
  * and, or, not, xor
  * equals, gt, ge, lt, le, matches(regex)
- sort by one or multiple columns
- groupBy on one or multiple columns
- join (left,right,inner) on one or multiple columns

## Usage
##create DataFrame
```java
File file = new File("person.csv");
CSVReader reader = CSVReaderBuilder.create(file)
        .withSeparator(";")
        .withHeaderPrefix("#")
        .containsHeader(true)
        .build();

DataFrame users = reader.buildDataFrame()
        .addColumn(new IntegerColumn("person_id"))
        .addColumn(new StringColumn("first_name"))
        .addColumn(new StringColumn("last_name"))
        .addColumn(new IntegerColumn("age"))
        .build();
        
System.out.println(users.getHeader());
for(DataRow row : users)
{
    System.out.println(row);
}
```
##Column operations
```java
//max value of column "person_id"
users.getIntegerColumn("age").max();



DoubleColumn dc1 = ...;
DoubleColumn dc2 = ...;
//add one column to another
dc1.add(dc2);

//multiply each value with 2
dc1.multiply(2);

```

##Filter and find
```java
//keep users with age between 18 and 60
users.filter(FilterPredicate.btwn("age",18,60));

//find all rows with age > 18 an first_name == "Max"
List<DataRow> rows = users.find(
      FilterPredicate.and(
              FilterPredicate.gt("age",18),
              FilterPredicate.eq("first_name","Max")
));
```
##Sorting
```java
//sort by column "person_id" (ascending)
users.sort("person_id", SortColumn.Direction.Ascending);

//sort by "last_name" und "first_name"
users.sort(
   new SortColumn("last_name", SortColumn.Direction.Descending),
   new SortColumn("first_name", SortColumn.Direction.Descending)
);
```

##GroupBy
```java

//group by "age" and "first_name"
DataGrouping grouping = users.groupBy("age","first_name");

//iterate through all found groups
for(DataGroup group : grouping)
{
    System.out.println(group.getGroupDescription());
    for(DataRow row : group){
        System.out.println(row);
    }
}
```

##Join
```java
//join DataFrames users and visits by columns users.person_id == visits.person_id
DataFrame visitors = users.joinLeft(visits,"person_id");

//join DataFrames users and orders by columns users.person_id == orders.customer_id
DataFrame userOrders = users.joinInner(orders,new JoinColumn("person_id","customer_id"));
```

