package akka_http_sandbox

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

import scala.concurrent.ExecutionContextExecutor
import scala.io.StdIn

object ValidationSandbox {
  def main(args: Array[String]) {

    implicit val system: ActorSystem = ActorSystem("my-system")
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    case class Person(name: String, age: Int) {
      require(age < 100, "Age must be lower than 100")
      require(age > 0, "Age must be greater than 0")
    }

    val validationViaRequire =
      path("person") {
        get {
          parameter("name", "age".as[Int]).as(Person) { person =>
            complete(s"The person you inputted was ${person.name} who is ${person.age}")
          }
        }
      }

    sealed trait Territory
    case object Germany extends Territory
    case object Italy extends Territory
    case object UnitedKingdom extends Territory

    object Territory {
      def extractTerritory(abbreviation: String): Option[Territory] = {
        abbreviation match {
          case "DE" => Some(Germany)
          case "IT" => Some(Italy)
          case "UK" => Some(UnitedKingdom)
          case _ => None
        }
      }
    }

    sealed trait Proposition
    case object SKYQ extends Proposition
    case object SKYNOW extends Proposition

    object Proposition {
      def extractProposition(input: String): Option[Proposition] = {
        input match {
          case "SKYQ" => Some(SKYQ)
          case "SKYNOW" => Some(SKYNOW)
          case _ => None
        }
      }
    }

    val validateTerritory: Directive1[Territory] =
      headerValueByName("territory").as(Territory.extractTerritory).flatMap {
        case Some(territory) => provide(territory)
        case None => reject
      }

    val validateProposition: Directive1[Proposition] =
      headerValueByName("territory").as(Proposition.extractProposition).flatMap {
        case Some(proposition) => provide(proposition)
        case None => reject
      }

    val headerValidation =
      path("headerValidation") {
        get {
          (validateTerritory & validateProposition) { (territory, proposition) =>
            complete(s"The territory you chose is $territory and the proposition you chose is $proposition")
          }
        }
      }

    val allRoutes = validationViaRequire ~ headerValidation

    val bindingFuture = Http().bindAndHandle(allRoutes, "localhost", 8080)

    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}