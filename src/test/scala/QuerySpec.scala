import com.eklavya.scqla._
import org.omg.PortableInterceptor.SUCCESSFUL
import org.scalatest.{ Matchers, BeforeAndAfterAll, FlatSpec }
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
/**
 * Created by eklavya on 18/2/14.
 */

case class Emp(empId: Int, deptId: Int, alive: Boolean, id: java.util.UUID, first: String, last: String, salary: Double, age: Long)

class QuerySpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  override def beforeAll {
      Scqla.connect
  }

  "Driver" should "be able to create a new keyspace" in {
    val res = Await.result(Scqla.query[SchemaChange.type]("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"), 5 seconds)
    println(s"this was executed ************************* $res")
    res.isInstanceOf[SchemaChange.type] should be(true)
  }

  "Driver" should "be able to set global keyspace" in {
    val res = Await.result(Scqla.query[SetKeyspace.type]("use demodb"), 5 seconds)
    res.isInstanceOf[SetKeyspace.type] should be(true)
  }

  "Driver" should "be able to create a new table" in {
    val res = Await.result(Scqla.query[SchemaChange.type](
      """CREATE TABLE demodb.emp (
    		empID int,
    		deptID int,
    		alive boolean,
    		id uuid,
    		first_name varchar,
    		last_name varchar,
    		salary double,
    		age bigint,
        PRIMARY KEY (empID, deptID))"""/*,
            age bigint,
    		salary double,
    		id uuid,
    		PRIMARY KEY (empID, deptID))
      """*/), 5 seconds)
    res.isInstanceOf[SchemaChange.type] should be(true)
  }

  "Driver" should "be able to execute prepared queries" in {
    val res = Await.result(Scqla.prepare("INSERT INTO demodb.emp (empID, deptID, alive, id, first_name, last_name, salary, age) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"), 5 seconds)
    val res1 = Await.result(res.execute(104, 15, true, new java.util.UUID(0, 0), "Hot", "Shot", 10000000.0, 98763L), 5 seconds)
    res1.isInstanceOf[Successful.type] should be(true)
  }

  "Driver" should "return proper case class list" in {
    val res = Await.result(Scqla.queryAs[Emp]("select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp"), 5 seconds)
    res.foreach(_.isInstanceOf[Emp] should be(true))
    res.head.asInstanceOf[Emp] should equal(Emp(104, 15, true, new java.util.UUID(0, 0), "Hot", "Shot", 10000000.0, 98763L))
  }

  "Driver" should "return proper primitives" in {
    val res = Await.result(Scqla.queryAs[Int]("select empid from demodb.emp"), 5 seconds)
    res.foreach(_.isInstanceOf[Int] should be(true))
    res.head.asInstanceOf[Int] should equal(104)
  }

  "Driver" should "return proper strings" in {
    val res = Await.result(Scqla.queryAs[String]("select first_name from demodb.emp"), 5 seconds)
    res.foreach(_.isInstanceOf[String] should be(true))
    res.head.asInstanceOf[String] should equal("Hot")
  }
  
  "Driver" should "be able to execute prepared queries and get results" in {
    val res = Await.result(Scqla.prepare("select empID, deptID, alive, id, first_name, last_name, salary, age from demodb.emp where empid = ? and deptid = ?"), 5 seconds)
    val res1 = Await.result(res.executeGet[Emp](104, 15), 5 seconds)
    res1.foreach(_.isInstanceOf[Emp] should be(true))
    res1.head.asInstanceOf[Emp] should equal(Emp(104, 15, true, new java.util.UUID(0, 0), "Hot", "Shot", 10000000.0, 98763L))
  }

  "Driver" should "be able to drop keyspace" in {
    val res = Await.result(Scqla.query[SchemaChange.type]("drop KEYSPACE demodb"), 5 seconds)
    res.isInstanceOf[SchemaChange.type] should be(true)
  }
}
