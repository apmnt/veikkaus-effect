import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.syntax.all.*
import collector.Collector
import java.net.http.HttpClient
import java.nio.file.Path
import java.time.Duration
import odds.v1.{OddsChange, OutcomeSide}
import scala.concurrent.duration.*

object Main extends IOApp:
  private type Saved = Map[(String, String, OutcomeSide), OddsChange]

  def run(args: List[String]): IO[ExitCode] =
    if args.nonEmpty && args != List("--once") then
      IO.consoleForIO.errorln("Usage: scala-cli run . -- [--once]").as(ExitCode.Error)
    else
      Resource.fromAutoCloseable(IO.delay(
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
      )).use { client =>
        val output = Path.of("data/odds.jsonl").toAbsolutePath.normalize()
        IO.consoleForIO.errorln(s"Collecting Veikkaus basketball odds into $output") *>
        cats.Monad[IO].tailRecM[Saved, Unit](Map.empty) { saved =>
          val read = for
            bodies <- Collector.fetch(client)
            time <- IO.realTimeInstant
            events <- IO.fromEither(bodies.traverse(Collector.parse(_, time)).map(_.flatten))
          yield events
          read.attempt.flatMap {
            case Left(error) =>
              if args == List("--once") then IO.raiseError(error)
              else IO.consoleForIO.errorln(s"Fetch/parse failed: ${error.getMessage}").as(saved)
            case Right(events) =>
              events.foldLeftM((saved, 0)) { case ((current, count), event) =>
                val changed = event.oddsChanges.filter { change =>
                  !current.get((event.eventId, change.marketId, change.side)).contains(change.copy(observedAt = None))
                }
                if changed.isEmpty then IO.pure((current, count))
                else Collector.save(output, event.copy(oddsChanges = changed)).as((
                  current ++ changed.map(change =>
                    (event.eventId, change.marketId, change.side) -> change.copy(observedAt = None)
                  ), count + changed.size
                ))
              }.flatMap { (next, count) =>
                val activeIds = events.map(_.eventId).toSet
                IO.consoleForIO.errorln(s"Collected ${events.size} basketball events; saved $count selection changes")
                  .as(next.filter { case ((eventId, _, _), _) => activeIds(eventId) })
              }
          }.flatMap { next =>
            if args == List("--once") then IO.pure(Right(()))
            else IO.sleep(15.seconds).as(Left(next))
          }
        }
      }.as(ExitCode.Success)
