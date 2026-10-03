package collector

import cats.effect.IO
import cats.syntax.all.*
import com.google.protobuf.timestamp.Timestamp
import io.circe.{Decoder, DecodingFailure, Json}
import java.net.{URI, URLEncoder}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.{Duration, Instant}
import odds.v1.{EventOddsHistory, OddsChange, OddsType, OutcomeSide}
import scala.util.Try

object Collector:
  // Prices are numbers; signed handicap components can be strings such as "+10.5".
  private given Decoder[BigDecimal] = Decoder.decodeString.emap(value =>
    Try(BigDecimal(value)).toEither.leftMap(_ => s"Invalid decimal: $value")
  ).or(Decoder.decodeBigDecimal)

  // Basketball futures/tournament winners have no home/away participants.
  private val basketballMatch: Json => Boolean = event =>
    event.hcursor.downField("category").get[String]("code").contains("BASKETBALL") &&
      event.hcursor.get[Vector[Json]]("teams").exists(teams =>
        teams.count(_.hcursor.get[String]("side").contains("HOME")) == 1 &&
          teams.count(_.hcursor.get[String]("side").contains("AWAY")) == 1
      )

  private val available: Decoder[Boolean] = Decoder.instance { c =>
    for
      active <- c.get[Boolean]("active")
      displayed <- c.get[Boolean]("displayed")
      status <- c.get[String]("status")
      result <-
        if !active || !displayed then Right(false)
        else if status == "ACTIVE" then Right(true)
        else if Set("SUSPENDED", "INACTIVE", "CLOSED", "RESULTED", "SETTLED")(status) then Right(false)
        else Left(DecodingFailure(s"Unknown status: $status", c.history))
    yield result
  }

  // 1. Fetch metadata, select basketball IDs, then fetch only those events' odds.
  def fetch(client: HttpClient): IO[Vector[String]] =
    val base = "https://content.ob.veikkaus.fi/content-service/api/v1/q"
    def request(url: String): IO[String] =
      val request = HttpRequest.newBuilder(URI.create(url)).header("Accept", "application/json")
        .timeout(Duration.ofSeconds(30)).GET().build()
      IO.fromCompletableFuture(IO.delay(client.sendAsync(request, HttpResponse.BodyHandlers.ofString())))
        .flatMap(response =>
          if response.statusCode() == 200 then IO.pure(response.body())
          else IO.raiseError(new RuntimeException(s"Veikkaus HTTP ${response.statusCode()}"))
        )
    for
      discoveries <- Vector("liveNow=true", "started=false").parTraverse { filter =>
        for
          body <- request(s"$base/event-list?lang=fi-FI&$filter")
          json <- IO.fromEither(io.circe.parser.parse(body))
          errors <- IO.fromEither(json.hcursor.get[Option[Vector[Json]]]("errors"))
          _ <- IO.raiseWhen(errors.exists(_.nonEmpty))(new RuntimeException("Veikkaus discovery errors"))
          events <- IO.fromEither(json.hcursor.downField("data").get[Vector[Json]]("events"))
        yield events
      }
      events = discoveries.flatten
      ids <- IO.fromEither(events.filter(basketballMatch)
        .traverse { event =>
          val c = event.hcursor
          (c.get[String]("id"), c.get[Boolean]("liveNow"), c.get[Boolean]("started"),
            c.get[Boolean]("resulted"), c.get[Boolean]("settled")).mapN { (id, live, started, resulted, settled) =>
            Option.when((live || !started) && !resulted && !settled)(id)
          }
        }.map(_.flatten.distinct))
      bodies <- ids.grouped(20).toVector.traverse { batch =>
        val encoded = URLEncoder.encode(batch.mkString(","), UTF_8)
        request(s"$base/events-by-ids?lang=fi-FI&eventIds=$encoded&includeChildMarkets=true")
      }
    yield bodies

  // 2. Pure decoding: same JSON and timestamp always produce the same Protobuf values.
  def parse(body: String, observedAt: Instant): Either[io.circe.Error, Vector[EventOddsHistory]] =
    for
      json <- io.circe.parser.parse(body)
      errors <- json.hcursor.get[Option[Vector[Json]]]("errors")
      _ <- Either.cond(errors.forall(_.isEmpty), (), DecodingFailure("Veikkaus odds errors", Nil))
      events <- json.hcursor.downField("data").get[Vector[Json]]("events")
      records <- events.filter(basketballMatch)
        .traverse { event =>
          val c = event.hcursor
          for
            id <- c.get[String]("id")
            teams <- c.get[Vector[Json]]("teams")
            home <- teams.filter(_.hcursor.get[String]("side").contains("HOME")).traverse(_.hcursor.get[String]("name"))
            away <- teams.filter(_.hcursor.get[String]("side").contains("AWAY")).traverse(_.hcursor.get[String]("name"))
            _ <- Either.cond(id.nonEmpty && home.size == 1 && away.size == 1, (), DecodingFailure("Missing event ID or team names", c.history))
            eventAvailable <- available(c)
            markets <- c.get[Vector[Json]]("markets")
            changes <- markets.filter { market =>
              market.hcursor.get[String]("groupCode").exists(
                Set("MONEY_LINE", "MATCH_RESULT", "HANDICAP", "HANDICAP_2_WAY")
              )
            }.traverse { market =>
              val m = market.hcursor
              for
                marketId <- m.get[String]("id")
                _ <- Either.cond(marketId.nonEmpty, (), DecodingFailure("Missing market ID", m.history))
                group <- m.get[String]("groupCode")
                subtype <- m.get[String]("subType")
                marketAvailable <- available(m)
                outcomes <- m.get[Vector[Json]]("outcomes")
                kind = if group.startsWith("HANDICAP") then OddsType.ODDS_TYPE_HANDICAP else OddsType.ODDS_TYPE_WINNING
                selections <- outcomes.traverse { outcome =>
                  val o = outcome.hcursor
                  for
                    code <- o.get[String]("subType")
                    side <- code match
                      case "H" => Right(OutcomeSide.OUTCOME_SIDE_HOME)
                      case "A" => Right(OutcomeSide.OUTCOME_SIDE_AWAY)
                      case "D" => Right(OutcomeSide.OUTCOME_SIDE_DRAW)
                      case "L" if subtype == "MH" => Right(OutcomeSide.OUTCOME_SIDE_DRAW)
                      case _ => Left(DecodingFailure(s"Unsupported side $code in $subtype", o.history))
                    outcomeAvailable <- available(o)
                    prices <- o.get[Vector[Json]]("prices")
                    typed <- prices.traverse(p => p.hcursor.get[String]("priceType").map(_ -> p))
                    quotes = typed.collect { case ("LP", p) => p }
                    _ <- Either.cond(quotes.size <= 1, (), DecodingFailure("Multiple LP prices", o.history))
                    quoted <- quotes.headOption.traverse(_.hcursor.get[BigDecimal]("decimal"))
                    _ <- Either.cond(quoted.forall(_ >= 1), (), DecodingFailure("Odds below 1", o.history))
                    handicap <-
                      if kind == OddsType.ODDS_TYPE_WINNING || quotes.isEmpty then Right(Vector.empty[BigDecimal])
                      else (quotes.head.hcursor.get[BigDecimal]("handicapLow"), quotes.head.hcursor.get[BigDecimal]("handicapHigh"))
                        .mapN((low, high) => Vector(low, high).distinct)
                  yield OddsChange(
                    Some(Timestamp(observedAt.getEpochSecond, observedAt.getNano)), marketId, kind, side,
                    quoted.map(_.bigDecimal.stripTrailingZeros.toPlainString),
                    handicap.map(_.bigDecimal.stripTrailingZeros.toPlainString),
                    active = eventAvailable && marketAvailable && outcomeAvailable
                  )
                }
              yield selections
            }
            flattened = changes.flatten
            keys = flattened.map(change => (change.marketId, change.side))
            _ <- Either.cond(keys.distinct.size == keys.size, (), DecodingFailure("Duplicate market/side", c.history))
          yield EventOddsHistory(id, home.head, away.head, flattened)
        }
    yield records

  // 3. Append one event's changed selections. Decimal prices remain strings in ProtoJSON.
  def save(path: Path, event: EventOddsHistory): IO[Unit] = IO.blocking {
    val json = Json.obj(
      "eventId" -> Json.fromString(event.eventId),
      "homeName" -> Json.fromString(event.homeName),
      "awayName" -> Json.fromString(event.awayName),
      "oddsChanges" -> Json.fromValues(event.oddsChanges.map { change =>
        val time = change.observedAt.get
        Json.obj((Vector(
          "observedAt" -> Json.fromString(Instant.ofEpochSecond(time.seconds, time.nanos).toString),
          "marketId" -> Json.fromString(change.marketId),
          "oddsType" -> Json.fromString(change.oddsType.name),
          "side" -> Json.fromString(change.side.name),
          "active" -> Json.fromBoolean(change.active)
        ) ++ change.decimalOdds.toVector.map(odds => "decimalOdds" -> Json.fromString(odds)) ++
          Option.when(change.handicap.nonEmpty)("handicap" -> Json.fromValues(change.handicap.map(Json.fromString))).toVector)* )
      })
    )
    Option(path.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    Files.writeString(path, json.noSpaces + "\n", UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    ()
  }
