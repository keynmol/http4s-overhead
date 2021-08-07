import cats.effect._
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.dsl.Http4sDsl
import org.http4s.circe._
import org.http4s.HttpRoutes
import org.http4s.implicits._
import io.circe.Json
import io.circe.syntax._
import cats.effect.std.Random
import org.http4s.ember.server.EmberServerBuilder
import cats.syntax.all._
import cats.effect.syntax.all._
import scala.concurrent.duration._
import org.http4s._
import org.http4s.client.Client
import org.http4s.asynchttpclient.client.AsyncHttpClient
import org.http4s.blaze.client.BlazeClientBuilder
import scala.concurrent.ExecutionContext

object Repro extends IOApp with Http4sDsl[IO] {

  def jsonGet(client: (String, Client[IO]), id: String)(implicit t: Tracer) =
    measure(
      s"jsonGet (${client._1}",
      client._2
        .expect[Json](s"http://localhost:8080/get/$id")
    )

  override def run(args: List[String]): IO[ExitCode] = {
    val limit       = args(0).toInt
    val parallelism = args(1).toInt

    val res = (
      emberClient,
      asyncClient,
      blazeClient,
      server,
      Resource
        .eval(Tracer.create(1000))
        .flatTap(_.periodicDumper(5.second).background),
      Resource.eval(Random.scalaUtilRandom[IO])
    ).parTupled

    res.use { case (ember, ahc, blaze, server, tracer, random) =>
      fs2.Stream
        .repeatEval(random.nextInt)
        .parEvalMap(parallelism) { id =>
          (
            jsonGet("ember" -> ember, id.toString)(tracer),
            jsonGet("ahc" -> ahc, id.toString)(tracer),
            jsonGet("blaze" -> blaze, id.toString)(tracer)
          ).parTupled
        }
        .take(limit)
        .mergeHaltBoth(fs2.Stream.eval(IO.readLine))
        .compile
        .drain
        .as(ExitCode.Success)
    }
  }

  def measure[T](name: String, a: IO[T])(implicit tracer: Tracer): IO[T] = {
    for {
      start    <- Clock[IO].monotonic.map(_.toMillis)
      result   <- a.attempt
      duration <- Clock[IO].monotonic.map(_.toMillis - start)
      _        <- tracer.ref.update { mp =>
                    mp.updatedWith(name) {
                      case None      => Some(Vector(duration))
                      case Some(vec) =>
                        if (vec.size == tracer.maxSize)
                          vec.appended(duration).tail.some
                        else vec.appended(duration).some
                    }
                  }
      res      <- IO.fromEither(result)
    } yield res
  }

  val emberClient = EmberClientBuilder.default[IO].build
  val blazeClient = BlazeClientBuilder[IO](ExecutionContext.global).resource
  val asyncClient = AsyncHttpClient.resource[IO]()

  def app(rand: Random[IO]) = HttpRoutes
    .of[IO] { case GET -> Root / "get" / itemId =>
      rand.betweenInt(50, 300).flatMap { dur =>
        Ok(Json.obj("id" := itemId, "delayedBy" := dur)).delayBy(dur.millis)
      }
    }
    .orNotFound

  val server =
    Resource.eval(Random.scalaUtilRandom[IO]).flatMap { rand =>
      EmberServerBuilder.default[IO].withHttpApp(app(rand)).build
    }

}

case class Tracer(
    ref: Ref[IO, Map[String, Vector[Long]]],
    maxSize: Int
)             {
  def render =
    ref.get.map(_.toList).flatMap { mp =>
      ref.get.map(_.toList).map { mp =>
        mp.map { case (n, obvs) =>
          val avg    = obvs.sum.toDouble / obvs.size
          val min    = obvs.min
          val max    = obvs.max
          val median = obvs.sorted.apply(obvs.size / 2)

          s"$n (millis): avg = $avg, min = $min, max = $max, median = $median"
        }.mkString("---------------\n", "\n", "")
      }
    }

  def periodicDumper(period: FiniteDuration = 1.second) =
    fs2.Stream
      .repeatEval(render.flatMap(IO.println))
      .metered(period)
      .compile
      .drain
}
object Tracer {
  def create(maxSize: Int = 1000) =
    IO.ref(Map.empty[String, Vector[Long]]).map(Tracer(_, maxSize))
}
