import com.eklavya.scqla._
import org.omg.PortableInterceptor.SUCCESSFUL
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
/**
 * Created by eklavya on 18/2/14.
 */

class QuerySpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  case class Emp(empId: Int, deptId: Int, first: String, last: String)

  override def beforeAll {
    Scqla.connect
  }

  "Driver" should "be able to set global keyspace" in {
    val res = Await.result(Scqla.query[SetKeyspace.type]("use demodb"), 5 seconds)
    res.isInstanceOf[SetKeyspace.type] should be(true)
  }

  "Driver" should "return proper case class list" in {
    Scqla.queryAs[Emp]("select * from emp").foreach { l =>
      l.foreach(_.isInstanceOf[Emp] should be(true))
    }
  }

  "Driver" should "return proper primitives" in {
    Scqla.queryAs[Int]("select empid from emp").foreach { l =>
      l.foreach(_.isInstanceOf[Int] should be(true))
    }
  }

  "Driver" should "return proper strings" in {
    Scqla.queryAs[String]("select first_name from emp").foreach { l =>
      l.foreach(_.isInstanceOf[String] should be(true))
    }
  }

  "Driver" should "be able to execute prepared queries" in {
    val res = Await.result(Scqla.prepare("update emp set last_name = ? where empid = ? and deptid = ?"), 5 seconds)
    val res1 = Await.result(res.execute("Parwal", 105, 16), 5 seconds)
    res1.isInstanceOf[Successful.type] should be(true)
  }
}
