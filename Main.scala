import cats.effect.{IO, IOApp}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.util.Using

object Main extends IOApp.Simple:
  // The event-list feed may ignore query filters. Check the response explicitly
  // before using event IDs for any later odds requests.
  def liveBasketballEvents(body: String): Vector[ujson.Value] =
    val events = ujson.read(body)("data")("events").arr.toVector
    events.filter { event =>
      event.obj.get("category").exists { category =>
        category.obj.get("code").contains(ujson.Str("BASKETBALL"))
      } && event.obj.get("liveNow").contains(ujson.Bool(true))
    }

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
          for
            events <- IO.delay(liveBasketballEvents(response.body()))
            _ <- IO.println(s"Live basketball events: ${events.size}")
            preview = ujson.write(ujson.Arr.from(events), indent = 2).take(1000)
            _ <- IO.println(s"Basketball JSON preview (first 1,000 characters):\n$preview")
          yield ()
        else
          IO.raiseError(new RuntimeException(s"Veikkaus returned HTTP ${response.statusCode()}"))
    yield ()
