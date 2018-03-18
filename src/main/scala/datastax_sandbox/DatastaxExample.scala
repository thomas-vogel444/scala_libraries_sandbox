package datastax_sandbox

import com.datastax.driver.core._
import datastax_sandbox.DatastaxExample.{idTable, keyspace}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.concurrent.duration._

sealed trait CassandraResponse[+A]
case class Applied[+A](result: A) extends CassandraResponse[A]
case object NotApplied extends CassandraResponse[Nothing]

trait CassandraConnector {

  val session: Session

  def insertIn(keyspace: String, table: String)(person: Person): CassandraResponse[Unit] = {
    val result: ResultSet =
      session.execute(
        s"INSERT INTO $keyspace.$table(id, age, name) " +
          s"VALUES (${person.id}, ${person.age}, '${person.name}')"
      )

    if (result.wasApplied()) Applied() else NotApplied

  }

  def insertInAsync(keyspace: String, table: String)(person: Person): Future[CassandraResponse[Unit]] = {
    val resultFuture = Future {
      session.executeAsync(
        s"INSERT INTO $keyspace.$table (id, age, name) " +
          s"VALUES (${person.id}, ${person.age}, '${person.name}')"
      ).get()
    }

    resultFuture.map(result => if (result.wasApplied()) Applied() else NotApplied)
  }

  def deleteIn(keyspace: String, table: String)(person: Person): CassandraResponse[Unit] = {
    val result = session.execute(s"DELETE FROM $keyspace.$table WHERE id=${person.id}")

    if (result.wasApplied()) Applied() else NotApplied
  }

  def deleteInAsync(keyspace: String, table: String)(person: Person): Future[CassandraResponse[Unit]] = {
    val resultFuture = Future {
      session.executeAsync(s"DELETE FROM $keyspace.$table WHERE id=${person.id}").get()
    }

    resultFuture.map(result => if (result.wasApplied()) Applied() else NotApplied)
  }

  def extractPerson(row: Row): Person =
    Person(id = row.getInt("id"), name = row.getString("name"), age = row.getInt("age"))

  def getPersonIn(keyspace: String, table: String)(person: Person): CassandraResponse[List[Person]] = {
    val result = session.execute(s"select * from $keyspace.$table where id=${person.id}")

    if (result.wasApplied()) {
      val rows: List[Row] = result.all().asScala.toList
      Applied(rows.map(extractPerson))
    } else NotApplied
  }

  def getStatement(person: Person): SimpleStatement =
    new SimpleStatement(s"select * from $keyspace.$idTable where id=${person.id}")

  def getAllPersonsInAsync(keyspace: String, table: String): Future[CassandraResponse[List[Person]]] = {
    val resultFuture = Future {
      session.executeAsync(s"select * from $keyspace.$table").get()
    }

    resultFuture.map(result =>
      if (result.wasApplied()) {
        val rows: List[Row] = result.all().asScala.toList
        Applied(rows.map(extractPerson))
      } else NotApplied)
  }
}

case class Person(id: Int, name: String, age: Int)

object DatastaxExample extends CassandraConnector {

  val cluster: Cluster =
    Cluster.builder()
      .addContactPoint("127.0.0.1")
      .withPort(9042)
      .build()

  val session: Session = cluster.connect()

  val tom = Person(1, "Tom", 30)
  val vlad = Person(2, "Vlad", 42)
  val aris = Person(3, "Aris", 46)

  // Should be in config
  val keyspace: String = "toms_sandbox"
  val idTable: String = "person"

  /**
   * Assuming the following has been run:
   *
   * CREATE KEYSPACE IF NOT EXISTS toms_sandbox WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
   * CREATE TABLE IF NOT EXISTS toms_sandbox.person (id int PRIMARY KEY, name text, age int);
   *
   */

  def main(args: Array[String]): Unit = {
    // Creating keyspace and table
    session.execute("CREATE KEYSPACE IF NOT EXISTS toms_sandbox WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE TABLE IF NOT EXISTS toms_sandbox.person (id int PRIMARY KEY, name text, age int);")

    // insert into the database
    println("Inserting tom, vlad and aris into Cassandra")
    val futures = Seq(
      insertInAsync(keyspace, idTable)(tom),
      insertInAsync(keyspace, idTable)(vlad),
      insertInAsync(keyspace, idTable)(aris)
    )

    Await.result(Future.sequence(futures), 5 seconds)

    println("Get tom and vlad from Cassandra")
    session.execute(getStatement(tom))
    session.execute(getStatement(vlad))

    println("Deleting tom and Roger from Cassandra")
    Await.result(deleteInAsync(keyspace, idTable)(tom), 5 seconds)
    Await.result(deleteInAsync(keyspace, idTable)(Person(12, "Roger", 65)), 5 seconds)

    val getAllResponse: CassandraResponse[List[Person]] = Await.result(getAllPersonsInAsync(keyspace, idTable), 5 seconds)

    getAllResponse match {
      case Applied(rows) =>
        rows.foreach { row =>
          println(row)
        }
    }

    session.close()
    cluster.close()

  }
}