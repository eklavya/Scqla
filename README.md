Scqla
=====

A fully asynchronous Cassandra CQL driver for Scala using Akka IO.


##Highlights:
1) Directly construct objects from results.

2) Completely stateless, no sessions required. Fire any query from anywhere, anytime.

3) Load balance between cassandra nodes.

4) Register callbacks for events.

###Connect to cassandra cluster.
```scala
Scqla.connect
```
###Import from Scqla object
```scala
import Scqla._
```

When you execute a query you get a future which can have a result or an error 
``` ErrorException(error: Error) ```
where ```Error``` is 
```scala
case class Error(code: Int, e: String)
```

###Create a new keyspace
```scala
query("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}") onComplete {
  case Success => 
  case Failure =>
}
```
###Set global keyspace

Note: This only works if you only connect to one node in Cassandra cluster. If that is not the case
use full table qualifiers as shown in the examples here.

```scala
query("use demodb") onComplete (
  case Failure(f) => println(f.error)
  case Success => 
)
```
###Create a new table
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
        PRIMARY KEY (empID, deptID))""") onComplete (
      case Failure(f) => println(f.error)
      case Success =>
    )
```
###Execute prepared queries
```scala
    prepare("INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)") onComplete (
      case Failure(f) => println(f.error)
      case Success(p) => p.execute(104, 15, true, new java.util.UUID(0, 0), "Hot", "Shot", 10000000.0, 98763L)
    )
```
###You can directly construct objects from the result.

###Case class list
```scala
case class Emp(empId: Int, deptId: Int, alive: Boolean, id: java.util.UUID, first: String, last: String, salary: Double, age: Long)

queryAs[Emp]("select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp") onComplete (
  case Failure(f) => println(f.error)
  case Success(empList) => 
    empList.foreach(e => s"First name of employee id ${e.empId} is ${e.first}")
)
```
###Primitives list
```scala
queryAs[Int]("select empid from demodb.emp") onComplete (
  case Failure(f) => println(f.error)
  case Success(idList) => {
    idList.foreach(println)
  }
)
```
###Strings
```scala
queryAs[String]("select first_name from demodb.emp") onComplete (
  case Failure(f) => println(f.error)
  case Success(nameList) => {
    nameList.foreach(println)
  }
)
```
###Execute prepared queries and get results
```scala
val f = prepare("select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp where empid = ? and deptid = ?")

val list = for {
  prepared <- f
  empList <- prepared.executeGet[Emp](104, 15)
} yield empList

list foreach {
    e => println(s"First name of employee id ${e.empId} is ${e.first}")
}
```
###Register a callback for an event
```scala
Events.registerDBEvent(CreatedEvent, (a, b) => {
  println(s"Hear hear, $a $b have come to be.")
})
```
Events:
```scala
abstract class Event

abstract class NodeEvent extends Event
case object NewNodeEvent extends NodeEvent
case object RemovedNodeEvent extends NodeEvent
case object NodeUpEvent extends NodeEvent
case object NodeDownEvent extends NodeEvent

abstract class DBEvent extends Event
case object CreatedEvent extends DBEvent
case object UpdatedEvent extends DBEvent
case object DroppedEvent extends DBEvent
```
NodeEvent handlers are of the type ``` Function[InetAddress, Unit] ```
DBEvent handlers are of the type ``` Function2[String, String, Unit] ```

###Drop keyspace
```scala
query("drop KEYSPACE demodb") onComplete (
  case Failure(f) => println(f.error)
  case Success => {
    //do something
  }
)
```
###Configuring thread pool for scqla future execution
By default Scqla makes use of the default execution context in scala.
However you can specify a manual ExecutionContext config by adding something
like this to your application.conf
```
contexts {
  scqla-pool {
    fork-join-executor {
      parallelism-factor = 1.0
      parallelism-max = 4
    }
  }
}
```