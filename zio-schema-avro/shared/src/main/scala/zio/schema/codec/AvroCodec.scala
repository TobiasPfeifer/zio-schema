package zio.schema.codec

import zio.schema.Schema._
import zio.schema._
import zio.schema.ast.SchemaAst
import zio.stream.ZTransducer
import zio.{ Chunk, ZIO }

import java.nio.charset.StandardCharsets
import java.nio.{ ByteBuffer, ByteOrder }
import java.time.format.DateTimeFormatter
import java.{ lang, time }
import scala.annotation.tailrec
import scala.collection.immutable.ListMap

object AvroCodec extends Codec {

  override def encoder[A](schema: Schema[A]): ZTransducer[Any, Nothing, A, Byte] =
    ZTransducer.fromPush(
      (opt: Option[Chunk[A]]) =>
        ZIO.succeed(opt.map(values => values.flatMap(Encoder.encode(schema, _))).getOrElse(Chunk.empty))
    )

  override def encode[A](schema: Schema[A]): A => Chunk[Byte] = a => Encoder.encode(schema, a)

  override def decoder[A](schema: Schema[A]): ZTransducer[Any, String, Byte, A] =
    ZTransducer.fromPush(
      (opt: Option[Chunk[Byte]]) =>
        ZIO.fromEither(opt.map(chunk => Decoder.decode(schema, chunk).map(Chunk(_))).getOrElse(Right(Chunk.empty)))
    )

  override def decode[A](schema: Schema[A]): Chunk[Byte] => Either[String, A] =
    ch => Decoder.decode(schema, ch)

  object PrimitiveStructures {
    def optionEmptyStructure() = Primitive(StandardType.UnitType)

    def tupleStructure[A, B](first: Schema[A], second: Schema[B]): Schema[ListMap[String, _]] =
      record(Field("first", first), Field("second", second))

    def monthDayStructure(): Schema[ListMap[String, _]] =
      record(Field("month", Primitive(StandardType.IntType)), Field("day", Primitive(StandardType.IntType)))

    def periodStructure(): Schema[ListMap[String, _]] =
      record(
        Field("years", Primitive(StandardType.IntType)),
        Field("months", Primitive(StandardType.IntType)),
        Field("days", Primitive(StandardType.IntType))
      )

    def yearMonthStructure(): Schema[ListMap[String, _]] =
      record(Field("year", Primitive(StandardType.IntType)), Field("month", Primitive(StandardType.IntType)))

    def durationStructure(): Schema[ListMap[String, _]] =
      record(Field("seconds", Primitive(StandardType.LongType)), Field("nanos", Primitive(StandardType.IntType)))
  }

  object Encoder {
    private val boolFalseChunk: Chunk[Byte] = Chunk[Byte](0)
    private val boolTrueChunk: Chunk[Byte]  = Chunk[Byte](1)

    //scalafmt: { maxColumn = 400, optIn.configStyleArguments = false }
    def encode[A](schema: Schema[A], value: A): Chunk[Byte] = schema match {
      case Sequence(element, _, g, _) => encodeSequence(element, g(value))
      case Schema.MapSchema(ks, vs, _) =>
        value match { // TODO: tests
          case map: Map[k, v] => encodeSequence(ks <*> vs, Chunk.fromIterable(map))
          case _              => Chunk.empty
        }
      case Schema.SetSchema(s, _) =>
        value match { // TODO: tests
          case set: Set[a] => encode(s, Chunk.fromIterable(set))
          case _           => Chunk.empty
        }
      case Transform(codec, _, g, _)  => g(value).map(encode(codec, _)).getOrElse(Chunk.empty)
      case Primitive(standardType, _) => encodePrimitive(standardType, value)
      case Optional(codec, _) =>
        value match {
          case v: Option[_] => encodeOptional(codec, v)
          case _            => Chunk.empty
        }
      case Tuple(left, right, _) =>
        value match {
          case v @ (_, _) => encodeTuple(left, right, v)
          case _          => Chunk.empty
        }
      case EitherSchema(left, right, _) =>
        value match {
          case v: Either[_, _] => encodeEither(left, right, v)
          case _               => Chunk.empty
        }
      case lzy @ Lazy(_) => encode(lzy.schema, value)
      case Meta(ast, _)  => encode(Schema[SchemaAst], ast)
      case Enum1(c1, _) =>
        encodeCase(value, c1)
      case Enum2(c1, c2, _) =>
        encodeCase(value, c1, c2)
      case Enum3(c1, c2, c3, _) =>
        encodeCase(value, c1, c2, c3)
      case Enum4(c1, c2, c3, c4, _) =>
        encodeCase(value, c1, c2, c3, c4)
      case Enum5(c1, c2, c3, c4, c5, _) =>
        encodeCase(value, c1, c2, c3, c4, c5)
      case Enum6(c1, c2, c3, c4, c5, c6, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6)
      case Enum7(c1, c2, c3, c4, c5, c6, c7, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7)
      case Enum8(c1, c2, c3, c4, c5, c6, c7, c8, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8)
      case Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9)
      case Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)
      case Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)
      case Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)
      case Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)
      case Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)
      case Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)
      case Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)
      case Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
      case Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)
      case Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)
      case Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)
      case Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)
      case Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, _) =>
        encodeCase(value, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)
      case EnumN(caseSet, _) => encodeCase(value, caseSet.toSeq: _*)
      case CaseClass1(f1, _, ext1, _) =>
        encodeCaseClass(value, f1 -> ext1)
      case CaseClass2(f1, f2, _, ext1, ext2, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2)
      case CaseClass3(f1, f2, f3, _, ext1, ext2, ext3, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3)
      case CaseClass4(f1, f2, f3, f4, _, ext1, ext2, ext3, ext4, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4)
      case CaseClass5(f1, f2, f3, f4, f5, _, ext1, ext2, ext3, ext4, ext5, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5)
      case CaseClass6(f1, f2, f3, f4, f5, f6, _, ext1, ext2, ext3, ext4, ext5, ext6, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6)
      case CaseClass7(f1, f2, f3, f4, f5, f6, f7, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7)
      case CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8)
      case CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9)
      case CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10)
      case CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11)
      case CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12)
      case CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13)
      case CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14)
      case CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15)
      case CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16)
      case CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17)
      case CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18)
      case CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19)
      case CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20)
      case CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21)
      case CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, _, ext1, ext2, ext3, ext4, ext5, ext6, ext7, ext8, ext9, ext10, ext11, ext12, ext13, ext14, ext15, ext16, ext17, ext18, ext19, ext20, ext21, ext22, _) =>
        encodeCaseClass(value, f1 -> ext1, f2 -> ext2, f3 -> ext3, f4 -> ext4, f5 -> ext5, f6 -> ext6, f7 -> ext7, f8 -> ext8, f9 -> ext9, f10 -> ext10, f11 -> ext11, f12 -> ext12, f13 -> ext13, f14 -> ext14, f15 -> ext15, f16 -> ext16, f17 -> ext17, f18 -> ext18, f19 -> ext19, f20 -> ext20, f21 -> ext21, f22 -> ext22)
      case GenericRecord(fieldSet, _) => encodeGenericRecord(value, fieldSet.toChunk)
      case _: Fail[_]                 => Chunk.empty
    }

    private def encodePrimitive[A](standardType: StandardType[A], v: A): Chunk[Byte] = standardType match {
      case StandardType.UnitType                      => Chunk.empty
      case StandardType.StringType                    => encodeString(v)
      case StandardType.BoolType                      => encodeBoolean(v)
      case StandardType.ShortType                     => encodeInt(v.toInt)
      case StandardType.IntType                       => encodeInt(v)
      case StandardType.LongType                      => encodeLong(v)
      case StandardType.FloatType                     => encodeFloat(v)
      case StandardType.DoubleType                    => encodeDouble(v)
      case StandardType.BinaryType                    => encodeBinary(v)
      case StandardType.CharType                      => encodeString(v.toString)
      case StandardType.UUIDType                      => encodeUUID(v)
      case StandardType.BigDecimalType                => encodeDecimal(v)
      case StandardType.BigIntegerType                => encodeBigInteger(v)
      case StandardType.DayOfWeekType                 => encodeInt(v.ordinal())
      case StandardType.MonthType                     => encodeInt(v.ordinal())
      case StandardType.MonthDayType                  => encode(PrimitiveStructures.monthDayStructure(), ListMap("month" -> v.getMonthValue(), "day" -> v.getDayOfMonth()))
      case StandardType.PeriodType                    => encode(PrimitiveStructures.periodStructure(), ListMap("years" -> v.getYears(), "months" -> v.getMonths(), "days" -> v.getDays()))
      case StandardType.YearType                      => encodeInt(v.getValue())
      case StandardType.YearMonthType                 => encode(PrimitiveStructures.yearMonthStructure(), ListMap("year" -> v.getYear, "month" -> v.getMonthValue))
      case StandardType.ZoneIdType                    => encodeString(v.getId())
      case StandardType.ZoneOffsetType                => encodeInt(v.getTotalSeconds())
      case StandardType.Duration(_)                   => encode(PrimitiveStructures.durationStructure(), ListMap[String, Any]("seconds" -> v.getSeconds(), "nanos" -> v.getNano()))
      case StandardType.InstantType(_)                => encodeLong(v.toEpochMilli())
      case StandardType.LocalDateType(formatter)      => encodeString(v.format(formatter))
      case StandardType.LocalTimeType(formatter)      => encodeString(v.format(formatter))
      case StandardType.LocalDateTimeType(formatter)  => encodeString(v.format(formatter))
      case StandardType.OffsetTimeType(formatter)     => encodeString(v.format(formatter))
      case StandardType.OffsetDateTimeType(formatter) => encodeString(v.format(formatter))
      case StandardType.ZonedDateTimeType(formatter)  => encodeString(v.format(formatter))
    }

    def encodeInt(i: Int): Chunk[Byte] = {
      val n = (i << 1) ^ (i >> 31)
      if ((n & ~0x7F) != 0) {
        encodeVarInt(n >>> 7, Chunk(((n | 0x80) & 0xFF).toByte))
      } else {
        Chunk(n.toByte)
      }
    }

    @tailrec
    def encodeVarInt(n: Int, acc: Chunk[Byte]): Chunk[Byte] =
      if (n > 0x7F) {
        encodeVarInt(n >>> 7, acc :+ ((n | 0x80) & 0xFF).toByte)
      } else {
        acc ++ Chunk(n.toByte)
      }

    def encodeLong(l: Long): Chunk[Byte] = {
      val n = (l << 1) ^ (l >> 63)
      if ((n & ~0x7FL) != 0) {
        encodeVarLong(n >>> 7, Chunk(((n | 0x80) & 0xFF).toByte))
      } else {
        Chunk(n.toByte)
      }
    }

    @tailrec
    def encodeVarLong(n: Long, acc: Chunk[Byte]): Chunk[Byte] =
      if (n > 0x7F) {
        encodeVarLong(n >>> 7, acc :+ ((n | 0x80) & 0xFF).toByte)
      } else {
        acc ++ Chunk(n.toByte)
      }

    private def encodeString(value: String): Chunk[Byte] = {
      val encoded = Chunk.fromArray(value.getBytes(StandardCharsets.UTF_8))
      encodeInt(encoded.size) ++ encoded
    }

    def encodeBoolean(value: Boolean): Chunk[Byte] = if (value) boolTrueChunk else boolFalseChunk

    def encodeFloat(value: Float): Chunk[Byte] = {
      val intBits: Int = lang.Float.floatToRawIntBits(value)
      val bytes: Array[Byte] =
        ByteBuffer.allocate(lang.Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(intBits).array
      Chunk.fromArray(bytes)
    }

    def encodeDouble(value: Double): Chunk[Byte] = {
      val longBits: Long = lang.Double.doubleToRawLongBits(value)
      val bytes: Array[Byte] =
        ByteBuffer.allocate(lang.Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(longBits).array
      Chunk.fromArray(bytes)
    }

    def encodeBinary(value: Chunk[Byte]): Chunk[Byte] = encodeInt(value.size) ++ value

    def encodeUUID(value: java.util.UUID): Chunk[Byte] = encodeString(value.toString)

    def encodeDecimal(value: BigDecimal): Chunk[Byte] = encodeString(value.toString())
    // The scala is fixed in the corresponding Avro schema TODO: is it an issue that different values (different scale) yield different schemas?
    //Chunk.fromArray(value.bigDecimal.unscaledValue().toByteArray())

    def encodeBigInteger(value: BigInt): Chunk[Byte] = encodeString(value.toString())
    //encodeDecimal(BigDecimal(value))

    private def encodeSequence[A](element: Schema[A], sequence: Chunk[A]): Chunk[Byte] = {
      val encoded = sequence.flatMap(value => encode(element, value))
      encodeLong(sequence.size.toLong) ++ encoded ++ encodeLong(0L)
    }

    private def encodeOptional[A](schema: Schema[A], value: Option[A]): Chunk[Byte] =
      encodeEither(PrimitiveStructures.optionEmptyStructure(), schema, value.toRight(()))

    private def encodeTuple[A, B](left: Schema[A], right: Schema[B], tuple: (A, B)): Chunk[Byte] =
      encode(PrimitiveStructures.tupleStructure(left, right), ListMap[String, Any]("first" -> tuple._1, "second" -> tuple._2))

    private def encodeEither[A, B](left: Schema[A], right: Schema[B], either: Either[A, B]): Chunk[Byte] =
      either match {
        case Left(value)  => encodeBoolean(false) ++ encode(left, value)
        case Right(value) => encodeBoolean(true) ++ encode(right, value)
      }

    def encodeCase[Z](v: Z, cases: Case[_, Z]*): Chunk[Byte] = {
      val fieldIndex = cases.indexWhere(c => c.deconstruct(v).isDefined)
      val encoded = Chunk.fromIterable(if (fieldIndex == -1) {
        Chunk.empty
      } else {
        val subtypeCase = cases(fieldIndex)
        encode(subtypeCase.codec.asInstanceOf[Schema[Any]], subtypeCase.unsafeDeconstruct(v))
      })
      encodeInt(fieldIndex) ++ encoded
    }

    private def encodeGenericRecord(value: Map[String, _], structure: Seq[Field[_]]): Chunk[Byte] =
      Chunk
        .fromIterable(structure.map {
          case Field(label, schema, _) =>
            value
              .get(label)
              .map(value => encode(schema.asInstanceOf[Schema[Any]], value))
              .getOrElse(Chunk.empty)
        })
        .flatten

    private def encodeCaseClass[Z](value: Z, fields: (Field[_], Z => Any)*): Chunk[Byte] =
      Chunk.fromIterable(fields.map { case (Field(_, schema, _), ext) => encode(schema.asInstanceOf[Schema[Any]], ext(value)) }).flatten
  }

  object Decoder {
    import DecodeResult._

    sealed trait DecodeResult[+A] { self =>

      def map[B](f: A => B): DecodeResult[B] =
        self match {
          case e: Failure                             => e
          case DecodeResult.Success(result, leftover) => DecodeResult.Success(f(result), leftover)
        }

      def flatMap[B](f: (A, Chunk[Byte]) => DecodeResult[B]): DecodeResult[B] =
        self match {
          case e: Failure                             => e
          case DecodeResult.Success(result, leftover) => f(result, leftover)
        }

      def absolve[B](implicit ev: A <:< Either[String, B]): DecodeResult[B] =
        self match {
          case e: Failure => e
          case DecodeResult.Success(result, leftover) =>
            ev(result) match {
              case Left(message) => Failure(message)
              case Right(value)  => DecodeResult.Success(value, leftover)
            }
        }
    }

    object DecodeResult {
      final case class Failure(message: String)                     extends DecodeResult[Nothing]
      final case class Success[A](result: A, leftover: Chunk[Byte]) extends DecodeResult[A]
    }

    def decode[A](schema: Schema[A], chunk: Chunk[Byte]): Either[String, A] =
      decodePartical(schema, chunk) match {
        case Failure(message)                               => Left(message)
        case Success(result, leftover) if !leftover.isEmpty => Left(s"Decoding did not consume all bytes. Decoded value was $result with bytes leftover $leftover")
        case Success(result, _)                             => Right(result)
      }

    def decodePartical[A](schema: Schema[A], chunk: Chunk[Byte]): DecodeResult[A] =
      schema match {
        case Sequence(schemaA, fromChunk, _, _)                => decodeSequence(schemaA, chunk).map(fromChunk)
        case Schema.MapSchema(ks: Schema[k], vs: Schema[v], _) => decodePartical(Schema.Sequence(ks <*> vs, (c: Chunk[(k, v)]) => Map(c: _*), (m: Map[k, v]) => Chunk.fromIterable(m)), chunk) // TODO: tests
        case Schema.SetSchema(schema: Schema[s], _)            => decodePartical(Schema.Sequence(schema, (c: Chunk[s]) => Set(c: _*), (m: Set[s]) => Chunk.fromIterable(m)), chunk) // TODO: tests
        case Transform(codec, f, _, _)                         => decodePartical(codec, chunk).map(f).absolve
        case Primitive(standardType, _)                        => decodePrimitive(standardType, chunk)
        case Optional(codec, _)                                => decodeOptional(codec, chunk)
        case Fail(message, _)                                  => Failure(message)
        case Tuple(left, right, _)                             => decodeTuple(left, right, chunk)
        case EitherSchema(left, right, _)                      => decodeEither(left, right, chunk)
        case lzy @ Lazy(_)                                     => decodePartical(lzy.schema, chunk)
        case Meta(_, _)                                        => decodePartical(Schema[SchemaAst], chunk).map(_.toSchema)
        case Enum1(c1, _) =>
          enumDecoder(c1)(chunk)
        case Enum2(c1, c2, _) =>
          enumDecoder(c1, c2)(chunk)
        case Enum3(c1, c2, c3, _) =>
          enumDecoder(c1, c2, c3)(chunk)
        case Enum4(c1, c2, c3, c4, _) =>
          enumDecoder(c1, c2, c3, c4)(chunk)
        case Enum5(c1, c2, c3, c4, c5, _) =>
          enumDecoder(c1, c2, c3, c4, c5)(chunk)
        case Enum6(c1, c2, c3, c4, c5, c6, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6)(chunk)
        case Enum7(c1, c2, c3, c4, c5, c6, c7, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7)(chunk)
        case Enum8(c1, c2, c3, c4, c5, c6, c7, c8, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8)(chunk)
        case Enum9(c1, c2, c3, c4, c5, c6, c7, c8, c9, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9)(chunk)
        case Enum10(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)(chunk)
        case Enum11(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11)(chunk)
        case Enum12(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12)(chunk)
        case Enum13(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13)(chunk)
        case Enum14(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14)(chunk)
        case Enum15(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15)(chunk)
        case Enum16(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16)(chunk)
        case Enum17(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)(chunk)
        case Enum18(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18)(chunk)
        case Enum19(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19)(chunk)
        case Enum20(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20)(chunk)
        case Enum21(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21)(chunk)
        case Enum22(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, _) =>
          enumDecoder(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22)(chunk)
        case EnumN(caseSet, _) =>
          enumDecoder[A](caseSet.toSeq: _*)(chunk)
        case GenericRecord(fieldSet, _) =>
          recordDecoder(fieldSet.toChunk.toList, chunk, ListMap.empty)
        case CaseClass1(f, construct, _, _) =>
          caseClass1Decoder(f, construct, chunk)
        case CaseClass2(f1, f2, construct, _, _, _) =>
          caseClass2Decoder(f1, f2, construct, chunk)
        case CaseClass3(f1, f2, f3, construct, _, _, _, _) =>
          caseClass3Decoder(f1, f2, f3, construct, chunk)
        case CaseClass4(f1, f2, f3, f4, construct, _, _, _, _, _) =>
          caseClass4Decoder(f1, f2, f3, f4, construct, chunk)
        case CaseClass5(f1, f2, f3, f4, f5, construct, _, _, _, _, _, _) =>
          caseClass5Decoder(f1, f2, f3, f4, f5, construct, chunk)
        case CaseClass6(f1, f2, f3, f4, f5, f6, construct, _, _, _, _, _, _, _) =>
          caseClass6Decoder(f1, f2, f3, f4, f5, f6, construct, chunk)
        case CaseClass7(f1, f2, f3, f4, f5, f6, f7, construct, _, _, _, _, _, _, _, _) =>
          caseClass7Decoder(f1, f2, f3, f4, f5, f6, f7, construct, chunk)
        case CaseClass8(f1, f2, f3, f4, f5, f6, f7, f8, construct, _, _, _, _, _, _, _, _, _) =>
          caseClass8Decoder(f1, f2, f3, f4, f5, f6, f7, f8, construct, chunk)
        case CaseClass9(f1, f2, f3, f4, f5, f6, f7, f8, f9, construct, _, _, _, _, _, _, _, _, _, _) =>
          caseClass9Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, construct, chunk)
        case CaseClass10(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, construct, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass10Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, construct, chunk)
        case CaseClass11(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, construct, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass11Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, construct, chunk)
        case CaseClass12(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, construct, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass12Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, construct, chunk)
        case CaseClass13(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass13Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, construct, chunk)
        case CaseClass14(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass14Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, construct, chunk)
        case CaseClass15(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass15Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, construct, chunk)
        case CaseClass16(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass16Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, construct, chunk)
        case CaseClass17(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass17Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, construct, chunk)
        case CaseClass18(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass18Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, construct, chunk)
        case CaseClass19(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass19Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, construct, chunk)
        case CaseClass20(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass20Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, construct, chunk)
        case CaseClass21(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass21Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, construct, chunk)
        case CaseClass22(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, construct, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _) =>
          caseClass22Decoder(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22, construct, chunk)
      }

    def decodeSequence[A](schema: Schema[A], chunk: Chunk[Byte]): DecodeResult[Chunk[A]] = {
      @tailrec
      def loop(count: Long, chunk: Chunk[Byte], acc: Chunk[A]): DecodeResult[Chunk[A]] =
        if (count < 0) Failure(s"Negative sequence length: $count")
        else if (count == 0) Success(acc, chunk)
        else {
          decodePartical(schema, chunk) match {
            case Failure(message)          => Failure(message)
            case Success(result, leftover) => loop(count - 1, leftover, acc :+ result)
          }
        }

      @tailrec
      def continue(count: Long, chunk: Chunk[Byte], acc: Chunk[A]): DecodeResult[Chunk[A]] = {
        val step: DecodeResult[(Option[Long], Chunk[A])] = loop(count, chunk, Chunk.empty).flatMap {
          case (as, leftover) =>
            decodePrimitive(StandardType.LongType, leftover).flatMap {
              case (nextBlockCount, leftover) =>
                if (nextBlockCount < 0L) Failure(s"Sequence ended with a negativ number after decoding $as")
                else if (nextBlockCount == 0) Success(None -> (acc ++ as), leftover) //0L indicating end of sequence
                else Success(Some(nextBlockCount)          -> (acc ++ as), leftover)
            }
        }

        step match {
          case Failure(message)                                  => Failure(message)
          case Success((None, result), leftover)                 => Success(result, leftover)
          case Success((Some(nextBlockCount), result), leftover) => continue(nextBlockCount, leftover, result)
        }
      }

      decodePrimitive(StandardType.LongType, chunk).flatMap {
        case (count, leftover) => continue(count, leftover, Chunk.empty)
      }

    }

    def decodePrimitive[A](standardType: StandardType[A], chunk: Chunk[Byte]): DecodeResult[A] = standardType match {
      case StandardType.UnitType                      => Success((), chunk)
      case StandardType.StringType                    => decodeString(chunk)
      case StandardType.BoolType                      => decodeBoolean(chunk)
      case StandardType.ShortType                     => decodeInt(chunk).map(_.toShort)
      case StandardType.IntType                       => decodeInt(chunk)
      case StandardType.LongType                      => decodeLong(chunk)
      case StandardType.FloatType                     => decodeFloat(chunk)
      case StandardType.DoubleType                    => decodeDouble(chunk)
      case StandardType.BinaryType                    => decodeBinary(chunk)
      case StandardType.CharType                      => decodeChar(chunk)
      case StandardType.UUIDType                      => decodeUUID(chunk)
      case StandardType.BigDecimalType                => decodeBigDecimal(chunk)
      case StandardType.BigIntegerType                => decodeBigInteger(chunk)
      case StandardType.DayOfWeekType                 => decodeDayOfWeek(chunk)
      case StandardType.MonthType                     => decodeMonth(chunk)
      case StandardType.MonthDayType                  => decodeMonthDay(chunk)
      case StandardType.PeriodType                    => decodePeriod(chunk)
      case StandardType.YearType                      => decodeYear(chunk)
      case StandardType.YearMonthType                 => decodeYearMonth(chunk)
      case StandardType.ZoneIdType                    => decodeZoneId(chunk)
      case StandardType.ZoneOffsetType                => decodeZoneOffset(chunk)
      case StandardType.Duration(_)                   => decodeDuration(chunk)
      case StandardType.InstantType(_)                => decodeInstant(chunk)
      case StandardType.LocalDateType(formatter)      => decodeLocalDate(formatter, chunk)
      case StandardType.LocalTimeType(formatter)      => decodeLocalTime(formatter, chunk)
      case StandardType.LocalDateTimeType(formatter)  => decodeLocalDateTime(formatter, chunk)
      case StandardType.OffsetTimeType(formatter)     => decodeOffsetTime(formatter, chunk)
      case StandardType.OffsetDateTimeType(formatter) => decodeOffsetDateTime(formatter, chunk)
      case StandardType.ZonedDateTimeType(formatter)  => decodeZonedDateTime(formatter, chunk)
    }

    def decodeString(chunk: Chunk[Byte]): DecodeResult[String] =
      decodeInt(chunk).flatMap {
        case (length, leftover) if leftover.size >= length => Success(new String(leftover.take(length).toArray, StandardCharsets.UTF_8), leftover.drop(length))
        case (length, leftover)                            => Failure(s"Not enougth bytes to decode string of length $length from bytes $leftover")
      }

    def decodeBoolean(chunk: Chunk[Byte]): DecodeResult[Boolean] =
      if (chunk.isEmpty) Failure("Could not decode boolean from empty chunk")
      else if (chunk.head == 0) Success(false, chunk.tail)
      else if (chunk.head == 1) Success(true, chunk.tail)
      else Failure(s"Boolean decoder expected byte 0 (false) or 1 (true) but encountered ${chunk.head}")

    @tailrec
    def decodeVarLong(chunk: Chunk[Byte], shift: Int, max: Int, acc: Long): DecodeResult[Long] = {
      if (max <= 0) Success(acc, chunk)
      if (chunk.isEmpty) Failure("Not enougth bytes to decode variable length encoded number")
      else {
        val b   = chunk.head & 0xff
        val num = acc ^ ((b & 0x7FL) << shift)
        if (b > 0x7f) {
          decodeVarLong(chunk.tail, shift + 7, max - 1, num)
        } else {
          Success(num, chunk.tail)
        }
      }
    }

    def decodeInt(chunk: Chunk[Byte]): DecodeResult[Int] = {
      val unzigzag: Int => Int = n => (n >>> 1) ^ -(n & 1)
      decodeVarLong(chunk, 0, 5, 0L).map(_.toInt).map(unzigzag)
    }

    def decodeLong(chunk: Chunk[Byte]): DecodeResult[Long] = {
      val unzigzag: Long => Long = n => (n >>> 1) ^ -(n & 1)
      decodeVarLong(chunk, 0, 10, 0L).map(unzigzag)
    }

    def decodeFloat(chunk: Chunk[Byte]): DecodeResult[Float] = {
      val length = lang.Integer.BYTES
      if (chunk.size >= length) {
        Success(lang.Float.intBitsToFloat(ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN).put(chunk.take(length).toArray).flip().getInt()), chunk.drop(length))
      } else
        Failure(s"Not enogth bytes to decode float from bytes $chunk")
    }

    def decodeDouble(chunk: Chunk[Byte]): DecodeResult[Double] = {
      val length = lang.Long.BYTES
      if (chunk.size >= length)
        Success(lang.Double.longBitsToDouble(ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN).put(chunk.take(length).toArray).flip().getLong()), chunk.drop(length))
      else
        Failure(s"Not enogth bytes to decode float from bytes $chunk")
    }

    def decodeBinary(chunk: Chunk[Byte]): DecodeResult[Chunk[Byte]] =
      decodeInt(chunk).flatMap {
        case (length, leftover) if leftover.size >= length => Success(leftover.take(length), leftover.drop(length))
        case (length, leftover)                            => Failure(s"Not enogth bytes to decode binary of length $length from bytes $leftover")
      }

    def decodeChar(chunk: Chunk[Byte]): DecodeResult[Char] = decodeString(chunk).map(_.head)

    def decodeUUID(chunk: Chunk[Byte]): DecodeResult[java.util.UUID] = decodeString(chunk).map(java.util.UUID.fromString(_))

    def decodeBigDecimal(chunk: Chunk[Byte]): DecodeResult[java.math.BigDecimal] = decodeString(chunk).flatMap {
      case (s, leftover) =>
        try {
          Success(new java.math.BigDecimal(s), leftover)
        } catch { case t: Throwable => Failure(s"error decoding BigDecimal from String: $t") }
    }

    def decodeBigInteger(chunk: Chunk[Byte]): DecodeResult[java.math.BigInteger] = decodeString(chunk).flatMap {
      case (s, leftover) =>
        try {
          Success(new java.math.BigInteger(s), leftover)
        } catch { case t: Throwable => Failure(s"error decoding BigInteger from String: $t") }
    }

    def decodeDayOfWeek(chunk: Chunk[Byte]): DecodeResult[java.time.DayOfWeek] = decodeInt(chunk).map(i => java.time.DayOfWeek.of(i + 1))

    def decodeMonthDay(chunk: Chunk[Byte]): DecodeResult[java.time.MonthDay] =
      decodePartical(PrimitiveStructures.monthDayStructure(), chunk)
        .map(data => java.time.MonthDay.of(data.getOrElse("month", 0).asInstanceOf[Int], data.getOrElse("day", 0).asInstanceOf[Int]))

    def decodeMonth(chunk: Chunk[Byte]): DecodeResult[java.time.Month] = decodeInt(chunk).map(i => java.time.Month.of(i + 1))

    def decodePeriod(chunk: Chunk[Byte]): DecodeResult[java.time.Period] =
      decodePartical(PrimitiveStructures.periodStructure(), chunk)
        .map(data => java.time.Period.of(data.getOrElse("years", 0).asInstanceOf[Int], data.getOrElse("months", 0).asInstanceOf[Int], data.getOrElse("days", 0).asInstanceOf[Int]))

    def decodeYear(chunk: Chunk[Byte]): DecodeResult[java.time.Year] = decodeInt(chunk).map(java.time.Year.of)

    def decodeYearMonth(chunk: Chunk[Byte]): DecodeResult[java.time.YearMonth] =
      decodePartical(PrimitiveStructures.yearMonthStructure(), chunk)
        .map(data => java.time.YearMonth.of(data.getOrElse("year", 0).asInstanceOf[Int], data.getOrElse("month", 0).asInstanceOf[Int]))

    def decodeZoneId(chunk: Chunk[Byte]): DecodeResult[java.time.ZoneId] = decodeString(chunk).map(java.time.ZoneId.of)

    def decodeZoneOffset(chunk: Chunk[Byte]): DecodeResult[java.time.ZoneOffset] = decodeInt(chunk).map(java.time.ZoneOffset.ofTotalSeconds)

    def decodeDuration(chunk: Chunk[Byte]): DecodeResult[time.Duration] =
      decodePartical(PrimitiveStructures.durationStructure(), chunk)
        .map(data => time.Duration.ofSeconds(data.getOrElse("seconds", 0).asInstanceOf[Long], data.getOrElse("nanos", 0).asInstanceOf[Int].longValue()))

    def decodeInstant(chunk: Chunk[Byte]): DecodeResult[java.time.Instant] = decodeLong(chunk).map(java.time.Instant.ofEpochMilli)

    def decodeLocalDate(formatter: DateTimeFormatter, chunk: Chunk[Byte]): DecodeResult[java.time.LocalDate] = decodeString(chunk).map(formatter.parse(_, java.time.LocalDate.from(_)))

    def decodeLocalTime(formatter: DateTimeFormatter, chunk: Chunk[Byte]): DecodeResult[java.time.LocalTime] = decodeString(chunk).map(formatter.parse(_, java.time.LocalTime.from(_)))

    def decodeLocalDateTime(formatter: DateTimeFormatter, chunk: Chunk[Byte]): DecodeResult[java.time.LocalDateTime] = decodeString(chunk).map(formatter.parse(_, java.time.LocalDateTime.from(_)))

    def decodeOffsetTime(formatter: DateTimeFormatter, chunk: Chunk[Byte]): DecodeResult[java.time.OffsetTime] = decodeString(chunk).map(formatter.parse(_, java.time.OffsetTime.from(_)))

    def decodeOffsetDateTime(formatter: DateTimeFormatter, chunk: Chunk[Byte]): DecodeResult[java.time.OffsetDateTime] = decodeString(chunk).map(formatter.parse(_, java.time.OffsetDateTime.from(_)))

    def decodeZonedDateTime(formatter: DateTimeFormatter, chunk: Chunk[Byte]): DecodeResult[java.time.ZonedDateTime] = decodeString(chunk).map(formatter.parse(_, java.time.ZonedDateTime.from(_)))

    def decodeOptional[A](codec: Schema[A], chunk: Chunk[Byte]): DecodeResult[Option[A]] =
      decodeEither(PrimitiveStructures.optionEmptyStructure(), codec, chunk).map(_.toOption)

    def decodeTuple[A, B](first: Schema[A], second: Schema[B], chunk: Chunk[Byte]): DecodeResult[(A, B)] =
      decodePartical(PrimitiveStructures.tupleStructure(first, second), chunk).map(l => l.get("first").get.asInstanceOf[A] -> l.get("second").get.asInstanceOf[B])

    def decodeEither[A, B](left: Schema[A], right: Schema[B], chunk: Chunk[Byte]): DecodeResult[Either[A, B]] =
      decodePrimitive(StandardType.BoolType, chunk).flatMap {
        case (result, leftover) => if (result) decodePartical(right, leftover).map(Right(_)) else decodePartical(left, leftover).map(Left(_))
      }

    def enumDecoder[Z](cases: Schema.Case[_, Z]*)(chunk: Chunk[Byte]): DecodeResult[Z] =
      decodePrimitive(StandardType.IntType, chunk).flatMap {
        case (index, leftover) => decodePartical(cases.toList(index).codec, leftover).map(_.asInstanceOf[Z])
      }

    @tailrec
    def recordDecoder(fields: List[Field[_]], chunk: Chunk[Byte], acc: ListMap[String, _]): DecodeResult[ListMap[String, _]] =
      fields match {
        case head :: next => {
          decodePartical(head.schema, chunk) match {
            case Failure(message) => Failure(message)
            case Success(result, leftover) => {
              recordDecoder(next, leftover, acc + (head.label -> result))
            }
          }
        }
        case Nil => Success(acc, chunk)
      }

    def caseClass1Decoder[A, Z](f: Field[A], construct: A => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f), chunk, arr => construct(arr(0).asInstanceOf[A]))

    def caseClass2Decoder[A, B, Z](f1: Field[A], f2: Field[B], construct: (A, B) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B]))

    def caseClass3Decoder[A, B, C, Z](f1: Field[A], f2: Field[B], f3: Field[C], construct: (A, B, C) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C]))

    def caseClass4Decoder[A, B, C, D, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], construct: (A, B, C, D) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D]))

    def caseClass5Decoder[A, B, C, D, E, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], construct: (A, B, C, D, E) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E]))

    def caseClass6Decoder[A, B, C, D, E, F, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], construct: (A, B, C, D, E, F) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5, f6), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F]))

    def caseClass7Decoder[A, B, C, D, E, F, G, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], construct: (A, B, C, D, E, F, G) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5, f6, f7), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G]))

    def caseClass8Decoder[A, B, C, D, E, F, G, H, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], construct: (A, B, C, D, E, F, G, H) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5, f6, f7, f8), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H]))

    def caseClass9Decoder[A, B, C, D, E, F, G, H, I, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], construct: (A, B, C, D, E, F, G, H, I) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5, f6, f7, f8, f9), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H], arr(8).asInstanceOf[I]))

    def caseClass10Decoder[A, B, C, D, E, F, G, H, I, J, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], f10: Field[J], construct: (A, B, C, D, E, F, G, H, I, J) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H], arr(8).asInstanceOf[I], arr(9).asInstanceOf[J]))

    def caseClass11Decoder[A, B, C, D, E, F, G, H, I, J, K, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], f10: Field[J], f11: Field[K], construct: (A, B, C, D, E, F, G, H, I, J, K) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H], arr(8).asInstanceOf[I], arr(9).asInstanceOf[J], arr(10).asInstanceOf[K]))

    def caseClass12Decoder[A, B, C, D, E, F, G, H, I, J, K, L, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], f10: Field[J], f11: Field[K], f12: Field[L], construct: (A, B, C, D, E, F, G, H, I, J, K, L) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12), chunk, arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H], arr(8).asInstanceOf[I], arr(9).asInstanceOf[J], arr(10).asInstanceOf[K], arr(11).asInstanceOf[L]))

    def caseClass13Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], f10: Field[J], f11: Field[K], f12: Field[L], f13: Field[M], construct: (A, B, C, D, E, F, G, H, I, J, K, L, M) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13),
        chunk,
        arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H], arr(8).asInstanceOf[I], arr(9).asInstanceOf[J], arr(10).asInstanceOf[K], arr(11).asInstanceOf[L], arr(12).asInstanceOf[M])
      )

    def caseClass14Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], f10: Field[J], f11: Field[K], f12: Field[L], f13: Field[M], f14: Field[N], construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14),
        chunk,
        arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H], arr(8).asInstanceOf[I], arr(9).asInstanceOf[J], arr(10).asInstanceOf[K], arr(11).asInstanceOf[L], arr(12).asInstanceOf[M], arr(13).asInstanceOf[N])
      )

    def caseClass15Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], f10: Field[J], f11: Field[K], f12: Field[L], f13: Field[M], f14: Field[N], f15: Field[O], construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) => Z, chunk: Chunk[Byte]): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15),
        chunk,
        arr => construct(arr(0).asInstanceOf[A], arr(1).asInstanceOf[B], arr(2).asInstanceOf[C], arr(3).asInstanceOf[D], arr(4).asInstanceOf[E], arr(5).asInstanceOf[F], arr(6).asInstanceOf[G], arr(7).asInstanceOf[H], arr(8).asInstanceOf[I], arr(9).asInstanceOf[J], arr(10).asInstanceOf[K], arr(11).asInstanceOf[L], arr(12).asInstanceOf[M], arr(13).asInstanceOf[N], arr(14).asInstanceOf[O])
      )

    def caseClass16Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, Z](f1: Field[A], f2: Field[B], f3: Field[C], f4: Field[D], f5: Field[E], f6: Field[F], f7: Field[G], f8: Field[H], f9: Field[I], f10: Field[J], f11: Field[K], f12: Field[L], f13: Field[M], f14: Field[N], f15: Field[O], f16: Field[P], construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) => Z, chunk: Chunk[Byte])
      : DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16),
        chunk,
        arr =>
          construct(
            arr(0).asInstanceOf[A],
            arr(1).asInstanceOf[B],
            arr(2).asInstanceOf[C],
            arr(3).asInstanceOf[D],
            arr(4).asInstanceOf[E],
            arr(5).asInstanceOf[F],
            arr(6).asInstanceOf[G],
            arr(7).asInstanceOf[H],
            arr(8).asInstanceOf[I],
            arr(9).asInstanceOf[J],
            arr(10).asInstanceOf[K],
            arr(11).asInstanceOf[L],
            arr(12).asInstanceOf[M],
            arr(13).asInstanceOf[N],
            arr(14).asInstanceOf[O],
            arr(15).asInstanceOf[P]
          )
      )

    def caseClass17Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, Z](
      f1: Field[A],
      f2: Field[B],
      f3: Field[C],
      f4: Field[D],
      f5: Field[E],
      f6: Field[F],
      f7: Field[G],
      f8: Field[H],
      f9: Field[I],
      f10: Field[J],
      f11: Field[K],
      f12: Field[L],
      f13: Field[M],
      f14: Field[N],
      f15: Field[O],
      f16: Field[P],
      f17: Field[Q],
      construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) => Z,
      chunk: Chunk[Byte]
    ): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17),
        chunk,
        arr =>
          construct(
            arr(0).asInstanceOf[A],
            arr(1).asInstanceOf[B],
            arr(2).asInstanceOf[C],
            arr(3).asInstanceOf[D],
            arr(4).asInstanceOf[E],
            arr(5).asInstanceOf[F],
            arr(6).asInstanceOf[G],
            arr(7).asInstanceOf[H],
            arr(8).asInstanceOf[I],
            arr(9).asInstanceOf[J],
            arr(10).asInstanceOf[K],
            arr(11).asInstanceOf[L],
            arr(12).asInstanceOf[M],
            arr(13).asInstanceOf[N],
            arr(14).asInstanceOf[O],
            arr(15).asInstanceOf[P],
            arr(16).asInstanceOf[Q]
          )
      )

    def caseClass18Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, Z](
      f1: Field[A],
      f2: Field[B],
      f3: Field[C],
      f4: Field[D],
      f5: Field[E],
      f6: Field[F],
      f7: Field[G],
      f8: Field[H],
      f9: Field[I],
      f10: Field[J],
      f11: Field[K],
      f12: Field[L],
      f13: Field[M],
      f14: Field[N],
      f15: Field[O],
      f16: Field[P],
      f17: Field[Q],
      f18: Field[R],
      construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) => Z,
      chunk: Chunk[Byte]
    ): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18),
        chunk,
        arr =>
          construct(
            arr(0).asInstanceOf[A],
            arr(1).asInstanceOf[B],
            arr(2).asInstanceOf[C],
            arr(3).asInstanceOf[D],
            arr(4).asInstanceOf[E],
            arr(5).asInstanceOf[F],
            arr(6).asInstanceOf[G],
            arr(7).asInstanceOf[H],
            arr(8).asInstanceOf[I],
            arr(9).asInstanceOf[J],
            arr(10).asInstanceOf[K],
            arr(11).asInstanceOf[L],
            arr(12).asInstanceOf[M],
            arr(13).asInstanceOf[N],
            arr(14).asInstanceOf[O],
            arr(15).asInstanceOf[P],
            arr(16).asInstanceOf[Q],
            arr(17).asInstanceOf[R]
          )
      )

    def caseClass19Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, Z](
      f1: Field[A],
      f2: Field[B],
      f3: Field[C],
      f4: Field[D],
      f5: Field[E],
      f6: Field[F],
      f7: Field[G],
      f8: Field[H],
      f9: Field[I],
      f10: Field[J],
      f11: Field[K],
      f12: Field[L],
      f13: Field[M],
      f14: Field[N],
      f15: Field[O],
      f16: Field[P],
      f17: Field[Q],
      f18: Field[R],
      f19: Field[S],
      construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) => Z,
      chunk: Chunk[Byte]
    ): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19),
        chunk,
        arr =>
          construct(
            arr(0).asInstanceOf[A],
            arr(1).asInstanceOf[B],
            arr(2).asInstanceOf[C],
            arr(3).asInstanceOf[D],
            arr(4).asInstanceOf[E],
            arr(5).asInstanceOf[F],
            arr(6).asInstanceOf[G],
            arr(7).asInstanceOf[H],
            arr(8).asInstanceOf[I],
            arr(9).asInstanceOf[J],
            arr(10).asInstanceOf[K],
            arr(11).asInstanceOf[L],
            arr(12).asInstanceOf[M],
            arr(13).asInstanceOf[N],
            arr(14).asInstanceOf[O],
            arr(15).asInstanceOf[P],
            arr(16).asInstanceOf[Q],
            arr(17).asInstanceOf[R],
            arr(18).asInstanceOf[S]
          )
      )

    def caseClass20Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, Z](
      f1: Field[A],
      f2: Field[B],
      f3: Field[C],
      f4: Field[D],
      f5: Field[E],
      f6: Field[F],
      f7: Field[G],
      f8: Field[H],
      f9: Field[I],
      f10: Field[J],
      f11: Field[K],
      f12: Field[L],
      f13: Field[M],
      f14: Field[N],
      f15: Field[O],
      f16: Field[P],
      f17: Field[Q],
      f18: Field[R],
      f19: Field[S],
      f20: Field[T],
      construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) => Z,
      chunk: Chunk[Byte]
    ): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20),
        chunk,
        arr =>
          construct(
            arr(0).asInstanceOf[A],
            arr(1).asInstanceOf[B],
            arr(2).asInstanceOf[C],
            arr(3).asInstanceOf[D],
            arr(4).asInstanceOf[E],
            arr(5).asInstanceOf[F],
            arr(6).asInstanceOf[G],
            arr(7).asInstanceOf[H],
            arr(8).asInstanceOf[I],
            arr(9).asInstanceOf[J],
            arr(10).asInstanceOf[K],
            arr(11).asInstanceOf[L],
            arr(12).asInstanceOf[M],
            arr(13).asInstanceOf[N],
            arr(14).asInstanceOf[O],
            arr(15).asInstanceOf[P],
            arr(16).asInstanceOf[Q],
            arr(17).asInstanceOf[R],
            arr(18).asInstanceOf[S],
            arr(19).asInstanceOf[T]
          )
      )

    def caseClass21Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, Z](
      f1: Field[A],
      f2: Field[B],
      f3: Field[C],
      f4: Field[D],
      f5: Field[E],
      f6: Field[F],
      f7: Field[G],
      f8: Field[H],
      f9: Field[I],
      f10: Field[J],
      f11: Field[K],
      f12: Field[L],
      f13: Field[M],
      f14: Field[N],
      f15: Field[O],
      f16: Field[P],
      f17: Field[Q],
      f18: Field[R],
      f19: Field[S],
      f20: Field[T],
      f21: Field[U],
      construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) => Z,
      chunk: Chunk[Byte]
    ): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21),
        chunk,
        arr =>
          construct(
            arr(0).asInstanceOf[A],
            arr(1).asInstanceOf[B],
            arr(2).asInstanceOf[C],
            arr(3).asInstanceOf[D],
            arr(4).asInstanceOf[E],
            arr(5).asInstanceOf[F],
            arr(6).asInstanceOf[G],
            arr(7).asInstanceOf[H],
            arr(8).asInstanceOf[I],
            arr(9).asInstanceOf[J],
            arr(10).asInstanceOf[K],
            arr(11).asInstanceOf[L],
            arr(12).asInstanceOf[M],
            arr(13).asInstanceOf[N],
            arr(14).asInstanceOf[O],
            arr(15).asInstanceOf[P],
            arr(16).asInstanceOf[Q],
            arr(17).asInstanceOf[R],
            arr(18).asInstanceOf[S],
            arr(19).asInstanceOf[T],
            arr(20).asInstanceOf[U]
          )
      )

    def caseClass22Decoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, Z](
      f1: Field[A],
      f2: Field[B],
      f3: Field[C],
      f4: Field[D],
      f5: Field[E],
      f6: Field[F],
      f7: Field[G],
      f8: Field[H],
      f9: Field[I],
      f10: Field[J],
      f11: Field[K],
      f12: Field[L],
      f13: Field[M],
      f14: Field[N],
      f15: Field[O],
      f16: Field[P],
      f17: Field[Q],
      f18: Field[R],
      f19: Field[S],
      f20: Field[T],
      f21: Field[U],
      f22: Field[V],
      construct: (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) => Z,
      chunk: Chunk[Byte]
    ): DecodeResult[Z] =
      unsafeDecodeFields(
        List(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15, f16, f17, f18, f19, f20, f21, f22),
        chunk,
        arr =>
          construct(
            arr(0).asInstanceOf[A],
            arr(1).asInstanceOf[B],
            arr(2).asInstanceOf[C],
            arr(3).asInstanceOf[D],
            arr(4).asInstanceOf[E],
            arr(5).asInstanceOf[F],
            arr(6).asInstanceOf[G],
            arr(7).asInstanceOf[H],
            arr(8).asInstanceOf[I],
            arr(9).asInstanceOf[J],
            arr(10).asInstanceOf[K],
            arr(11).asInstanceOf[L],
            arr(12).asInstanceOf[M],
            arr(13).asInstanceOf[N],
            arr(14).asInstanceOf[O],
            arr(15).asInstanceOf[P],
            arr(16).asInstanceOf[Q],
            arr(17).asInstanceOf[R],
            arr(18).asInstanceOf[S],
            arr(19).asInstanceOf[T],
            arr(20).asInstanceOf[U],
            arr(21).asInstanceOf[V]
          )
      )

    def unsafeDecodeFields[Z](fields: List[Field[_]], chunk: Chunk[Byte], constructUnsafe: List[Any] => Z): DecodeResult[Z] = {
      val recordResult = recordDecoder(fields, chunk, ListMap.empty)
      recordResult match {
        case Failure(message) => Failure(message)
        case Success(result, leftover) => {
          val fieldResults = result.toList.map(_._2.asInstanceOf[Any])
          Success(constructUnsafe(fieldResults), leftover)
        }
      }
    }
  }
}
