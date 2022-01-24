package zio.schema.codec

import zio.codec.AvroCodec
import zio.schema.{ Schema, StandardType }
import zio.stream.{ ZSink, ZStream }
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, ZIO }

import java.math.BigInteger
import java.util.UUID

object AvroCodecSpec extends DefaultRunnableSpec {

  def spec = suite("AvroCodec Spec")(
    suite("encode")(
      suite("primitives")(
        testM("boolean") {
          for {
            f <- encode(Schema.Primitive(StandardType.BoolType), false).map(toHex(_))
            t <- encode(Schema.Primitive(StandardType.BoolType), true).map(toHex(_))
          } yield assert(f)(equalTo("00")) && assert(t)(equalTo("01"))
        },
        testM("short") {
          for {
            p0  <- encode[Short](Schema.Primitive(StandardType.ShortType), 0).map(toHex(_))
            p1  <- encode[Short](Schema.Primitive(StandardType.ShortType), 1).map(toHex(_))
            n1  <- encode[Short](Schema.Primitive(StandardType.ShortType), -1).map(toHex(_))
            p64 <- encode[Short](Schema.Primitive(StandardType.ShortType), 64).map(toHex(_))
            n64 <- encode[Short](Schema.Primitive(StandardType.ShortType), -64).map(toHex(_))
          } yield assert(p0)(equalTo("00")) && assert(p1)(equalTo("02")) && assert(n1)(equalTo("01")) && assert(p64)(
            equalTo("8001")
          ) && assert(n64)(equalTo("7F"))
        },
        testM("int") {
          for {
            p0       <- encode(Schema.Primitive(StandardType.IntType), 0).map(toHex(_))
            p1       <- encode(Schema.Primitive(StandardType.IntType), 1).map(toHex(_))
            n1       <- encode(Schema.Primitive(StandardType.IntType), -1).map(toHex(_))
            p64      <- encode(Schema.Primitive(StandardType.IntType), 64).map(toHex(_))
            n64      <- encode(Schema.Primitive(StandardType.IntType), -64).map(toHex(_))
            p8124    <- encode(Schema.Primitive(StandardType.IntType), 8124).map(toHex(_))
            p1677664 <- encode(Schema.Primitive(StandardType.IntType), 1677664).map(toHex(_))
          } yield assert(p0)(equalTo("00")) && assert(p1)(equalTo("02")) && assert(n1)(equalTo("01")) && assert(p64)(
            equalTo("8001")
          ) && assert(n64)(equalTo("7F")) && assert(p8124)(equalTo("F87E")) && assert(p1677664)(equalTo("C0E5CC01"))
        },
        testM("long") {
          for {
            p0            <- encode(Schema.Primitive(StandardType.LongType), 0L).map(toHex(_))
            p1            <- encode(Schema.Primitive(StandardType.LongType), 1L).map(toHex(_))
            n1            <- encode(Schema.Primitive(StandardType.LongType), -1L).map(toHex(_))
            p64           <- encode(Schema.Primitive(StandardType.LongType), 64L).map(toHex(_))
            n64           <- encode(Schema.Primitive(StandardType.LongType), -64L).map(toHex(_))
            p8124000      <- encode(Schema.Primitive(StandardType.LongType), 8124000L).map(toHex(_))
            p167766400000 <- encode(Schema.Primitive(StandardType.LongType), 167766400000L).map(toHex(_))
          } yield assert(p0)(equalTo("00")) && assert(p1)(equalTo("02")) && assert(n1)(equalTo("01")) && assert(p64)(
            equalTo("8001")
          ) && assert(n64)(equalTo("7F")) && assert(p8124000)(equalTo("C0D9DF07")) && assert(p167766400000)(
            equalTo("80F0C0FAE109")
          )
        },
        testM("float") {
          for {
            f1 <- encode(Schema.Primitive(StandardType.FloatType), 3.1415f).map(toHex(_))
          } yield assert(f1)(equalTo("560E4940"))
        },
        testM("double") {
          for {
            d1 <- encode(Schema.Primitive(StandardType.DoubleType), 3.1415d).map(toHex(_))
          } yield assert(d1)(equalTo("6F1283C0CA210940"))
        },
        testM("bytes") {
          val c1Bytes = Chunk[Byte](-1, 0, 1, 2)
          for {
            empty <- encode(Schema.Primitive(StandardType.BinaryType), Chunk.empty).map(toHex(_))
            c1    <- encode(Schema.Primitive(StandardType.BinaryType), c1Bytes).map(toHex(_))
          } yield assert(empty)(equalTo("00")) && assert(c1)(equalTo("08FF000102"))
        },
        testM("string") {
          for {
            empty <- encode(Schema.Primitive(StandardType.StringType), "").map(toHex(_))
            s1    <- encode(Schema.Primitive(StandardType.StringType), "avro").map(toHex(_))
          } yield assert(empty)(equalTo("00")) && assert(s1)(equalTo("086176726F"))
        },
        testM("bigDecimal") {
          for {
            t1 <- encode[java.math.BigDecimal](
                   Schema.Primitive(StandardType.BigDecimalType),
                   BigDecimal("3.1415").bigDecimal
                 ).map(toHex(_))
            n1 <- encode[java.math.BigDecimal](
                   Schema.Primitive(StandardType.BigDecimalType),
                   BigDecimal("-3.1415").bigDecimal
                 ).map(toHex(_))
          } yield assert(t1)(equalTo("0C332E31343135")) && assert(n1)(equalTo("0E2D332E31343135"))
        },
        testM("bitInteger") {
          for {
            t1 <- encode[BigInteger](Schema.Primitive(StandardType.BigIntegerType), BigInt("31415").bigInteger)
                   .map(toHex(_))
            n1 <- encode[BigInteger](Schema.Primitive(StandardType.BigIntegerType), BigInt("-31415").bigInteger)
                   .map(toHex(_))
          } yield assert(t1)(equalTo("0A3331343135")) && assert(n1)(equalTo("0C2D3331343135"))
        },
        testM("char") {
          for {
            t1 <- encode(Schema.Primitive(StandardType.CharType), 'a'.charValue()).map(toHex(_))
          } yield assert(t1)(equalTo("0261"))
        },
        testM("dayOfWeek") {
          import java.time.DayOfWeek
          for {
            monday <- encode(Schema.Primitive(StandardType.DayOfWeekType), DayOfWeek.MONDAY).map(toHex(_))
            friday <- encode(Schema.Primitive(StandardType.DayOfWeekType), DayOfWeek.FRIDAY).map(toHex(_))
          } yield assert(monday)(equalTo("00")) && assert(friday)(equalTo("08"))
        },
        testM("month") {
          import java.time.Month
          for {
            jan <- encode(Schema.Primitive(StandardType.Month), Month.JANUARY).map(toHex(_))
            may <- encode(Schema.Primitive(StandardType.Month), Month.MAY).map(toHex(_))
          } yield assert(jan)(equalTo("00")) && assert(may)(equalTo("08"))
        },
        testM("monthDay") {
          import java.time.{ Month, MonthDay }
          for {
            jan4    <- encode(Schema.Primitive(StandardType.MonthDay), MonthDay.of(Month.JANUARY, 4)).map(toHex(_))
            april12 <- encode(Schema.Primitive(StandardType.MonthDay), MonthDay.of(Month.APRIL, 12)).map(toHex(_))
          } yield assert(jan4)(equalTo("0208")) && assert(april12)(equalTo("0818"))
        },
        testM("year") {
          import java.time.Year
          for {
            bc <- encode(Schema.Primitive(StandardType.Year), Year.of(-4)).map(toHex(_))
            ac <- encode(Schema.Primitive(StandardType.Year), Year.of(4)).map(toHex(_))
          } yield assert(bc)(equalTo("07")) && assert(ac)(equalTo("08"))
        },
        testM("yearMonth") {
          import java.time.{ Month, YearMonth }
          for {
            yearMonth <- encode(Schema.Primitive(StandardType.YearMonth), YearMonth.of(4, Month.OCTOBER)).map(toHex(_))
          } yield assert(yearMonth)(equalTo("0814"))
        },
        testM("period") {
          import java.time.Period
          for {
            period <- encode(Schema.Primitive(StandardType.Period), Period.of(1, 2, 3)).map(toHex(_))
          } yield assert(period)(equalTo("020406"))
        },
        testM("unit") {
          for {
            encoded <- encode(Schema.Primitive(StandardType.UnitType), ()).map(toHex(_))
          } yield assert(encoded)(equalTo(""))
        },
        testM("uuid") {
          for {
            encoded <- encode(
                        Schema.Primitive(StandardType.UUIDType),
                        UUID.fromString("d326fea5-94e4-4ba5-988e-3c4f194067a7")
                      ).map(toHex(_))
          } yield assert(encoded)(equalTo("4864333236666561352D393465342D346261352D393838652D336334663139343036376137"))
        },
        testM("zoneId") {
          import java.time.ZoneId
          for {
            encoded <- encode(Schema.Primitive(StandardType.ZoneId), ZoneId.of("CET")).map(toHex(_))
          } yield assert(encoded)(equalTo("06434554"))
        },
        testM("zoneOffset") {
          import java.time.ZoneOffset
          for {
            encoded <- encode(Schema.Primitive(StandardType.ZoneOffset), ZoneOffset.ofHoursMinutes(1, 30)).map(toHex(_))
          } yield assert(encoded)(equalTo("B054"))
        },
        testM("duration") {
          import java.time.Duration
          import java.time.temporal.ChronoUnit
          for {
            encoded <- encode(Schema.Primitive(StandardType.Duration(ChronoUnit.SECONDS)), Duration.ofNanos(2000004L))
                        .map(toHex(_))
          } yield assert(encoded)(equalTo("008892F401"))
        },
        testM("instant") {
          import java.time.Instant
          import java.time.format.DateTimeFormatter
          val formatter = DateTimeFormatter.ISO_DATE_TIME
          for {
            encoded <- encode(Schema.Primitive(StandardType.Instant(formatter)), Instant.ofEpochMilli(12L))
                        .map(toHex(_))
          } yield assert(encoded)(equalTo("18"))
        },
        testM("localDate") {
          import java.time.LocalDate
          import java.time.format.DateTimeFormatter
          val formatter = DateTimeFormatter.ISO_LOCAL_DATE
          for {
            encoded <- encode(Schema.Primitive(StandardType.LocalDate(formatter)), LocalDate.of(2020, 3, 12))
                        .map(toHex(_))
          } yield assert(encoded)(equalTo("14323032302D30332D3132"))
        },
        testM("localDateTime") {
          import java.time.LocalDateTime
          import java.time.format.DateTimeFormatter
          val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
          for {
            encoded <- encode(
                        Schema.Primitive(StandardType.LocalDateTime(formatter)),
                        LocalDateTime.of(2020, 3, 12, 2, 3, 4)
                      ).map(toHex(_))
          } yield assert(encoded)(equalTo("26323032302D30332D31325430323A30333A3034"))
        },
        testM("localTime") {
          import java.time.LocalTime
          import java.time.format.DateTimeFormatter
          val formatter = DateTimeFormatter.ISO_LOCAL_TIME
          for {
            encoded <- encode(Schema.Primitive(StandardType.LocalTime(formatter)), LocalTime.of(2, 3, 4)).map(toHex(_))
          } yield assert(encoded)(equalTo("1030323A30333A3034"))
        },
        testM("offsetDateTime") {
          import java.time.format.DateTimeFormatter
          import java.time.{ LocalDateTime, OffsetDateTime, ZoneOffset }
          val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
          for {
            encoded <- encode(
                        Schema.Primitive(StandardType.OffsetDateTime(formatter)),
                        OffsetDateTime.of(LocalDateTime.of(2020, 3, 12, 2, 3, 4), ZoneOffset.ofHours(2))
                      ).map(toHex(_))
          } yield assert(encoded)(equalTo("32323032302D30332D31325430323A30333A30342B30323A3030"))
        },
        testM("offsetTime") {
          import java.time.format.DateTimeFormatter
          import java.time.{ OffsetTime, ZoneOffset }
          val formatter = DateTimeFormatter.ISO_OFFSET_TIME
          for {
            encoded <- encode(
                        Schema.Primitive(StandardType.OffsetTime(formatter)),
                        OffsetTime.of(2, 3, 4, 0, ZoneOffset.ofHours(2))
                      ).map(toHex(_))
          } yield assert(encoded)(equalTo("1C30323A30333A30342B30323A3030"))
        },
        testM("zonedDateTime") {
          import java.time.format.DateTimeFormatter
          import java.time.{ LocalDateTime, ZoneId, ZonedDateTime }
          val formatter = DateTimeFormatter.ISO_ZONED_DATE_TIME
          for {
            encoded <- encode(
                        Schema.Primitive(StandardType.ZonedDateTime(formatter)),
                        ZonedDateTime.of(LocalDateTime.of(2020, 3, 12, 2, 3, 4), ZoneId.of("UTC"))
                      ).map(toHex(_))
          } yield assert(encoded)(equalTo("32323032302D30332D31325430323A30333A30345A5B5554435D"))
        }
      )
    ),
    suite("decode")(
      testM("boolean") {
        for {
          f <- decode(Schema.Primitive(StandardType.BoolType), "00")
          t <- decode(Schema.Primitive(StandardType.BoolType), "01")
        } yield assert(f)(equalTo(false)) && assert(t)(equalTo(true))
      },
      testM("short") {
        for {
          p0  <- decode(Schema.Primitive(StandardType.ShortType), "00")
          p1  <- decode(Schema.Primitive(StandardType.ShortType), "02")
          n1  <- decode(Schema.Primitive(StandardType.ShortType), "01")
          p64 <- decode(Schema.Primitive(StandardType.ShortType), "8001")
          n64 <- decode(Schema.Primitive(StandardType.ShortType), "7F")
        } yield assertTrue(p0 == 0.toShort) && assertTrue(p1 == 1.toShort) && assertTrue(n1 == (-1).toShort) && assertTrue(
          p64 == 64.toShort
        ) && assertTrue(n64 == (-64).toShort)
      },
      testM("int") {
        for {
          p0       <- decode(Schema.Primitive(StandardType.IntType), "00")
          p1       <- decode(Schema.Primitive(StandardType.IntType), "02")
          n1       <- decode(Schema.Primitive(StandardType.IntType), "01")
          p64      <- decode(Schema.Primitive(StandardType.IntType), "8001")
          n64      <- decode(Schema.Primitive(StandardType.IntType), "7F")
          p8124    <- decode(Schema.Primitive(StandardType.IntType), "F87E")
          p1677664 <- decode(Schema.Primitive(StandardType.IntType), "C0E5CC01")
        } yield assert(p0)(equalTo(0)) && assert(p1)(equalTo(1)) && assert(n1)(equalTo(-1)) && assert(p64)(
          equalTo(64)
        ) && assert(n64)(equalTo(-64)) && assert(p8124)(equalTo(8124)) && assert(p1677664)(equalTo(1677664))
      },
      testM("long") {
        for {
          p0            <- decode(Schema.Primitive(StandardType.LongType), "00")
          p1            <- decode(Schema.Primitive(StandardType.LongType), "02")
          n1            <- decode(Schema.Primitive(StandardType.LongType), "01")
          p64           <- decode(Schema.Primitive(StandardType.LongType), "8001")
          n64           <- decode(Schema.Primitive(StandardType.LongType), "7F")
          p8124000      <- decode(Schema.Primitive(StandardType.LongType), "C0D9DF07")
          p167766400000 <- decode(Schema.Primitive(StandardType.LongType), "80F0C0FAE109")
        } yield assert(p0)(equalTo(0L)) && assert(p1)(equalTo(1L)) && assert(n1)(equalTo(-1L)) && assert(p64)(
          equalTo(64L)
        ) && assert(n64)(equalTo(-64L)) && assert(p8124000)(equalTo(8124000L)) && assert(p167766400000)(
          equalTo(167766400000L)
        )
      },
      testM("float") {
        for {
          f1 <- decode(Schema.Primitive(StandardType.FloatType), "560E4940")
        } yield assert(f1)(equalTo(3.1415f))
      },
      testM("double") {
        for {
          d1 <- decode(Schema.Primitive(StandardType.DoubleType), "6F1283C0CA210940")
        } yield assert(d1)(equalTo(3.1415d))
      },
      testM("bytes") {
        val c1Bytes = Chunk[Byte](-1, 0, 1, 2)
        for {
          empty <- decode(Schema.Primitive(StandardType.BinaryType), "00")
          c1    <- decode(Schema.Primitive(StandardType.BinaryType), "08FF000102")
        } yield assert(empty)(equalTo(Chunk.empty)) && assert(c1)(equalTo(c1Bytes))
      },
      testM("string") {
        for {
          empty <- decode(Schema.Primitive(StandardType.StringType), "00")
          s1    <- decode(Schema.Primitive(StandardType.StringType), "086176726F")
        } yield assert(empty)(equalTo("")) && assert(s1)(equalTo("avro"))
      },
      testM("bigDecimal") {
        for {
          t1 <- decode(Schema.Primitive(StandardType.BigDecimalType), "0C332E31343135")
          n1 <- decode(Schema.Primitive(StandardType.BigDecimalType), "0E2D332E31343135")
        } yield assert(t1)(equalTo(BigDecimal("3.1415").bigDecimal)) && assert(n1)(
          equalTo(BigDecimal("-3.1415").bigDecimal)
        )
      },
      testM("bitInteger") {
        for {
          t1 <- decode(Schema.Primitive(StandardType.BigIntegerType), "0A3331343135")
          n1 <- decode(Schema.Primitive(StandardType.BigIntegerType), "0C2D3331343135")
        } yield assert(t1)(equalTo(BigInt("31415").bigInteger)) && assert(n1)(equalTo(BigInt("-31415").bigInteger))
      },
      testM("char") {
        for {
          t1 <- decode(Schema.Primitive(StandardType.CharType), "0261")
        } yield assert(t1)(equalTo('a'.charValue()))
      },
      testM("dayOfWeek") {
        import java.time.DayOfWeek
        for {
          monday <- decode(Schema.Primitive(StandardType.DayOfWeekType), "00")
          friday <- decode(Schema.Primitive(StandardType.DayOfWeekType), "08")
        } yield assert(monday)(equalTo(DayOfWeek.MONDAY)) && assert(friday)(equalTo(DayOfWeek.FRIDAY))
      },
      testM("month") {
        import java.time.Month
        for {
          jan <- decode(Schema.Primitive(StandardType.Month), "00")
          may <- decode(Schema.Primitive(StandardType.Month), "08")
        } yield assert(jan)(equalTo(Month.JANUARY)) && assert(may)(equalTo(Month.MAY))
      },
      testM("monthDay") {
        import java.time.{ Month, MonthDay }
        for {
          jan4    <- decode(Schema.Primitive(StandardType.MonthDay), "0208")
          april12 <- decode(Schema.Primitive(StandardType.MonthDay), "0818")
        } yield assert(jan4)(equalTo(MonthDay.of(Month.JANUARY, 4))) && assert(april12)(
          equalTo(MonthDay.of(Month.APRIL, 12))
        )
      },
      testM("year") {
        import java.time.Year
        for {
          bc <- decode(Schema.Primitive(StandardType.Year), "07")
          ac <- decode(Schema.Primitive(StandardType.Year), "08")
        } yield assert(bc)(equalTo(Year.of(-4))) && assert(ac)(equalTo(Year.of(4)))
      },
      testM("yearMonth") {
        import java.time.{ Month, YearMonth }
        for {
          yearMonth <- decode(Schema.Primitive(StandardType.YearMonth), "0814")
        } yield assert(yearMonth)(equalTo(YearMonth.of(4, Month.OCTOBER)))
      },
      testM("period") {
        import java.time.Period
        for {
          period <- decode(Schema.Primitive(StandardType.Period), "020406")
        } yield assert(period)(equalTo(Period.of(1, 2, 3)))
      },
      testM("unit") {
        for {
          decoded <- decode(Schema.Primitive(StandardType.UnitType), "")
        } yield assert(decoded)(equalTo(()))
      },
      testM("uuid") {
        for {
          decoded <- decode(
                      Schema.Primitive(StandardType.UUIDType),
                      "4864333236666561352D393465342D346261352D393838652D336334663139343036376137"
                    )
        } yield assert(decoded)(equalTo(UUID.fromString("d326fea5-94e4-4ba5-988e-3c4f194067a7")))
      },
      testM("zoneId") {
        import java.time.ZoneId
        for {
          decoded <- decode(Schema.Primitive(StandardType.ZoneId), "06434554")
        } yield assert(decoded)(equalTo(ZoneId.of("CET")))
      },
      testM("zoneOffset") {
        import java.time.ZoneOffset
        for {
          decoded <- decode(Schema.Primitive(StandardType.ZoneOffset), "B054")
        } yield assert(decoded)(equalTo(ZoneOffset.ofHoursMinutes(1, 30)))
      },
      testM("duration") {
        import java.time.Duration
        import java.time.temporal.ChronoUnit
        for {
          _ <- encode(
                Schema.Primitive(StandardType.Duration(ChronoUnit.SECONDS)),
                Duration.of(2000004L, ChronoUnit.NANOS)
              )
          decoded <- decode(Schema.Primitive(StandardType.Duration(ChronoUnit.SECONDS)), "008892F401")
        } yield assert(decoded)(equalTo(Duration.ofNanos(2000004L)))
      },
      testM("instant") {
        import java.time.Instant
        import java.time.format.DateTimeFormatter
        val formatter = DateTimeFormatter.ISO_DATE_TIME
        for {
          decoded <- decode(Schema.Primitive(StandardType.Instant(formatter)), "18")
        } yield assert(decoded)(equalTo(Instant.ofEpochMilli(12L)))
      },
      testM("localDate") {
        import java.time.LocalDate
        import java.time.format.DateTimeFormatter
        val formatter = DateTimeFormatter.ISO_LOCAL_DATE
        for {
          decoded <- decode(Schema.Primitive(StandardType.LocalDate(formatter)), "14323032302D30332D3132")
        } yield assert(decoded)(equalTo(LocalDate.of(2020, 3, 12)))
      },
      testM("localDateTime") {
        import java.time.LocalDateTime
        import java.time.format.DateTimeFormatter
        val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
        for {
          decoded <- decode(
                      Schema.Primitive(StandardType.LocalDateTime(formatter)),
                      "26323032302D30332D31325430323A30333A3034"
                    )
        } yield assert(decoded)(equalTo(LocalDateTime.of(2020, 3, 12, 2, 3, 4)))
      },
      testM("localTime") {
        import java.time.LocalTime
        import java.time.format.DateTimeFormatter
        val formatter = DateTimeFormatter.ISO_LOCAL_TIME
        for {
          decoded <- decode(Schema.Primitive(StandardType.LocalTime(formatter)), "1030323A30333A3034")
        } yield assert(decoded)(equalTo(LocalTime.of(2, 3, 4)))
      },
      testM("offsetDateTime") {
        import java.time.{ LocalDateTime, OffsetDateTime, ZoneOffset }
        import java.time.format.DateTimeFormatter
        val formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
        for {
          decoded <- decode(
                      Schema.Primitive(StandardType.OffsetDateTime(formatter)),
                      "32323032302D30332D31325430323A30333A30342B30323A3030"
                    )
        } yield assert(decoded)(
          equalTo(OffsetDateTime.of(LocalDateTime.of(2020, 3, 12, 2, 3, 4), ZoneOffset.ofHours(2)))
        )
      },
      testM("offsetTime") {
        import java.time.{ OffsetTime, ZoneOffset }
        import java.time.format.DateTimeFormatter
        val formatter = DateTimeFormatter.ISO_OFFSET_TIME
        for {
          decoded <- decode(
                      Schema.Primitive(StandardType.OffsetTime(formatter)),
                      "1C30323A30333A30342B30323A3030"
                    )
        } yield assert(decoded)(equalTo(OffsetTime.of(2, 3, 4, 0, ZoneOffset.ofHours(2))))
      },
      testM("zonedDateTime") {
        import java.time.{ LocalDateTime, ZoneId, ZonedDateTime }
        import java.time.format.DateTimeFormatter
        val formatter = DateTimeFormatter.ISO_ZONED_DATE_TIME
        for {
          decoded <- decode(
                      Schema.Primitive(StandardType.ZonedDateTime(formatter)),
                      "32323032302D30332D31325430323A30333A30345A5B5554435D"
                    )
        } yield assert(decoded)(equalTo(ZonedDateTime.of(LocalDateTime.of(2020, 3, 12, 2, 3, 4), ZoneId.of("UTC"))))
      }
    ),
    suite("decode operation should reverse encode operation")(
      testM("int") {
        reverseCheck(Gen.anyInt)(Schema.Primitive(StandardType.IntType))
      },
      testM("short") {
        reverseCheck(Gen.anyShort)(Schema.Primitive(StandardType.ShortType))
      },
      testM("long") {
        reverseCheck(Gen.anyLong)(Schema.Primitive(StandardType.LongType))
      },
      testM("String") {
        reverseCheck(Gen.anyString)(Schema.Primitive(StandardType.StringType))
      }
    )
  )

  def reverseCheck[A, R](gen: Gen[R, A])(schema: Schema[A]) =
    checkM(gen) { x =>
      for {
        encoded <- encode(schema, x)
        decoded <- decode(schema, encoded)
      } yield assert(decoded)(equalTo(x))
    }

  def encode[A](schema: Schema[A], input: A): ZIO[Any, Nothing, Chunk[Byte]] =
    ZStream
      .succeed(input)
      .transduce(AvroCodec.encoder(schema))
      .run(ZSink.collectAll)

  def decode[A](schema: Schema[A], hexInput: String): ZIO[Any, String, A] =
    decode(schema, fromHex(hexInput))

  def decode[A](schema: Schema[A], input: Chunk[Byte]): ZIO[Any, String, A] =
    ZIO.fromEither(AvroCodec.decode(schema)(input))

  def toHex(chunk: Chunk[Byte]): String =
    chunk.toArray.map("%02X".format(_)).mkString

  def fromHex(hexString: String): Chunk[Byte] =
    if (hexString.isEmpty) Chunk.empty
    else {
      val byteArray = new BigInteger(hexString, 16).toByteArray
      if (byteArray.head == 0 && !hexString.startsWith("00"))
        Chunk.fromArray(byteArray.tail) // handle BigInteger leading zero bytes
      else Chunk.fromArray(byteArray)
    }
}
