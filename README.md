Scqla
=====

Cassandra CQL driver for Scala.

The aim is to make use of scala features to provide a nice API.

#Highlights:
1) Directly construct objects from results.
2) Get an Either as a result of all queries. No exceptions, just Scala goodness.

#Connect to cassandra cluster.
```scala
Scqla.connect
```
#Import from Scqla object
```scala
import Scqla._
```
#Create a new keyspace
```scala
query("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}").map(_.fold(
  error => println(error),
  valid => {
    //do something
  }
))
```
#Set global keyspace

Note: This only works if you only connect to one node in Cassandra cluster. If that is not the case
use full table qualifiers as shown in the examples here.

```scala
query("use demodb").map(_.fold(
  error => println(error),
  valid => { //do something
  }
))
```
#Create a new table
```scala
    query("""CREATE TABLE demodb.emp (
    		empID int,
    		deptID int,
    		alive boolean,
    		id uuid,
    		first_name varchar,
    		last_name varchar,
    		salary double,
    		age bigint,
        PRIMARY KEY (empID, deptID))""").map(_.fold(
      error => println(error),
      valid => {
        //do something
      }
    ))
```
#Execute prepared queries
```scala
    prepare("INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").map(_.fold(
      error => println(error),
      valid => valid.execute(104, 15, true, new java.util.UUID(0, 0), "Hot", "Shot", 10000000.0, 98763L)))
```
#You can directly construct objects from the result.

#Case class list
```scala
case class Emp(empId: Int, deptId: Int, alive: Boolean, id: java.util.UUID, first: String, last: String, salary: Double, age: Long)

queryAs[Emp]("select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp").map(_.fold(
  error => println(error),
  empList => {
    empList.foreach(e => s"First name of employee id ${e.empId} is ${e.first}")
  }
))
```
#Primitives list
```scala
queryAs[Int]("select empid from demodb.emp").map(_.fold(
  error => println(error),
  idList => {
    idList.foreach(println)
  }
))
```
#Strings
```scala
queryAs[String]("select first_name from demodb.emp").map(_.fold(
  error => println(error),
  nameList => {
    nameList.foreach(println)
  }
))
```
#Execute prepared queries and get results
```scala
prepare("select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp where empid = ? and deptid = ?").map(_.fold(
  error => println(error),
  valid => valid.executeGet[Emp](104, 15).map(_.fold(
    error => println(error),
    empList => {
      empList.foreach(e => s"First name of employee id ${e.empId} is ${e.first}")
    }
  )
)))
```
#Drop keyspace
```scala
query("drop KEYSPACE demodb").map(_.fold(
  error => println(error),
  valid => {
    //do something
  }
))
```