import cats.effect.{IO, IOApp}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.util.Using

object Main extends IOApp.Simple:
  val eventsUri: URI = URI.create(
    "https://content.ob.veikkaus.fi/content-service/api/v1/q/event-list?lang=fi-FI&liveNow=true"
  )

  val fetchEvents: IO[HttpResponse[String]] = IO.blocking {
    Using.resource(
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
    ) { client =>
      val request = HttpRequest.newBuilder(eventsUri)
        .header("Accept", "application/json")
        .timeout(Duration.ofSeconds(30))
        .GET()
        .build()

      client.send(request, HttpResponse.BodyHandlers.ofString())
    }
  }

  val run: IO[Unit] =
    for
      response <- fetchEvents
      _ <- IO.println(s"HTTP status: ${response.statusCode()}")
      _ <-
        if response.statusCode() == 200 then
          IO.println(s"JSON preview (first 1,000 characters):\n${response.body().take(1000)}")
        else
          IO.raiseError(new RuntimeException(s"Veikkaus returned HTTP ${response.statusCode()}"))
    yield ()
