package collector

import cats.effect.unsafe.implicits.global
import io.circe.{Json, parser}
import java.nio.file.{Files, Path}
import java.time.Instant
import munit.FunSuite
import odds.v1.{EventOddsHistory, OddsType, OutcomeSide}

class CollectorSuite extends FunSuite:
  private val source = Files.readString(Path.of("examples/veikkaus-basketball-markets.json"))
  private val time = Instant.parse("2026-10-03T15:00:00Z")
  private def parsed(body: String = source): Vector[EventOddsHistory] =
    Collector.parse(body, time).toOption.get

  test("captured basketball data maps to seven winning/handicap selections with correct sides") {
    val event = parsed().head
    assertEquals(event.homeName, "AEK Athens")
    assertEquals(event.awayName, "Marousi BC")
    assertEquals(event.oddsChanges.size, 7)
    assert(event.oddsChanges.forall(_.active))
    val handicap = event.oddsChanges.filter(_.oddsType == OddsType.ODDS_TYPE_HANDICAP)
    assertEquals(handicap.find(_.side == OutcomeSide.OUTCOME_SIDE_HOME).get.handicap, Seq("-10.5"))
    assertEquals(handicap.find(_.side == OutcomeSide.OUTCOME_SIDE_AWAY).get.handicap, Seq("10.5"))
    assert(event.oddsChanges.exists(_.side == OutcomeSide.OUTCOME_SIDE_DRAW))
    assertEquals(EventOddsHistory.parseFrom(event.toByteArray), event)
  }

  test("exact decimals survive decoding; changes in numeric formatting do not change state") {
    val precise = source.replace("1.81", "1.12345678901234567890123456789")
    assert(parsed(precise).head.oddsChanges.exists(_.decimalOdds.contains("1.12345678901234567890123456789")))
    assertEquals(parsed(source.replace("1.81", "1.81000")), parsed())
  }

  test("suspension changes active without discarding the quoted prices") {
    val suspended = source.replace("\"ACTIVE\"", "\"SUSPENDED\"")
    val original = parsed().head.oddsChanges
    val inactive = parsed(suspended).head.oddsChanges
    assert(inactive.forall(change => !change.active))
    assertEquals(inactive.map(_.decimalOdds), original.map(_.decimalOdds))
    assertEquals(inactive.map(_.handicap), original.map(_.handicap))
    assertNotEquals(inactive.map(_.copy(observedAt = None)), original.map(_.copy(observedAt = None)))
    assertEquals(parsed(source.replace("BASKETBALL", "FOOTBALL")), Vector.empty)
    assert(Collector.parse(source.replace("\"ACTIVE\"", "\"UNKNOWN\""), time).isLeft)
  }

  test("inactive source flags also produce inactive selections with prices retained") {
    // Only the event flag changes; its still-active markets/outcomes must inherit it.
    val eventInactive = parsed(source.replaceFirst("\"active\": true", "\"active\": false")).head.oddsChanges
    assert(eventInactive.forall(change => !change.active))
    val inactive = parsed(source.replace("\"active\": true", "\"active\": false")).head.oddsChanges
    assert(inactive.forall(change => !change.active))
    assertEquals(inactive.map(_.decimalOdds), parsed().head.oddsChanges.map(_.decimalOdds))
    assertEquals(EventOddsHistory.parseFrom(parsed(source.replace("\"active\": true", "\"active\": false")).head.toByteArray)
      .oddsChanges.map(_.active), inactive.map(_.active))
  }

  test("futures and combination bets do not enter the home/away price contract") {
    val future = source.replace("\"HOME\"", "\"OTHER\"")
    assertEquals(parsed(future), Vector.empty)
    // Similar group prefixes are not sufficient: these bets combine two conditions.
    val combination = source.replace("\"HANDICAP_2_WAY\"", "\"HANDICAP_2_WAY_TOTAL_POINTS_OVER/UNDER\"")
    val event = parsed(combination).head
    assertEquals(event.oddsChanges.size, 5)
    assert(event.oddsChanges.forall(_.oddsType == OddsType.ODDS_TYPE_WINNING))
  }

  test("saving appends independent valid ProtoJSON lines without overwriting history") {
    val file = Files.createTempFile("veikkaus-minimal-collector-", ".jsonl")
    try
      val event = parsed().head
      Collector.save(file, event).unsafeRunSync()
      Collector.save(file, event).unsafeRunSync()
      Collector.save(file, event.copy(oddsChanges = event.oddsChanges.map(_.copy(active = false)))).unsafeRunSync()
      val lines = Files.readAllLines(file)
      assertEquals(lines.size(), 3)
      val json = parser.parse(lines.get(0)).toOption.get
      assertEquals(json.hcursor.get[String]("homeName"), Right("AEK Athens"))
      assertEquals(json.hcursor.get[Vector[Json]]("oddsChanges").toOption.get.size, 7)
      assert(json.hcursor.get[Vector[Json]]("oddsChanges").toOption.get.forall(_.hcursor.get[Boolean]("active").contains(true)))
      assertEquals(lines.get(0), lines.get(1))
      val inactive = parser.parse(lines.get(2)).toOption.get.hcursor.get[Vector[Json]]("oddsChanges").toOption.get
      assert(inactive.forall(_.hcursor.get[Boolean]("active").contains(false)))
    finally Files.deleteIfExists(file)
  }
