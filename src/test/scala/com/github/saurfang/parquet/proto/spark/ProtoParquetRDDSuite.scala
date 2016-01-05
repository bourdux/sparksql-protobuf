package com.github.saurfang.parquet.proto.spark

import com.github.saurfang.parquet.proto.AddressBook._
import com.github.saurfang.parquet.proto.PrimitiveInGroup.PrimitiveInGroupMessage
import com.github.saurfang.parquet.proto.PrimitiveInGroup.PrimitiveInGroupMessage.Foo
import com.github.saurfang.parquet.proto.spark.sql.ProtoReflection
import com.google.protobuf.AbstractMessage
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.scalatest._

/**
  * We demonstrate that we have the ability to convert RDD[Protobuf] as dataframe.
  * We can also do the reverse: read parquet file back as RDD[Protobuf].
  */
class ProtoParquetRDDSuite extends FunSuite with Matchers with SharedSparkContext with Logging {

  // person specified as rows
  val personRows: Seq[Row] = Seq(
    Row("Alice", 0, "alice@outlook.com", Seq(), Seq("NYC", "Seattle")),
    Row("Bob", 1, "bob@gmail.com",
      Seq(Row("1234", Person.PhoneType.HOME.toString), Row("2345", Person.PhoneType.MOBILE.toString)),
      Seq()
    )
  )

  // person specified as protobuf message
  val personMessages: Seq[Person] = Seq(
    Person.newBuilder()
      .setEmail("alice@outlook.com")
      .setId(0)
      .setName("Alice")
      .addAddress("NYC")
      .addAddress("Seattle")
      .build(),
    Person.newBuilder()
      .setEmail("bob@gmail.com")
      .setId(1)
      .setName("Bob")
      .addPhone(
        Person.PhoneNumber.newBuilder()
          .setNumber("1234")
          .setType(Person.PhoneType.HOME)
      )
      .addPhone(
        Person.PhoneNumber.newBuilder()
          .setNumber("2345")
          .setType(Person.PhoneType.MOBILE)
      )
      .build
  )

  // primitive un group specified as rows
  val primitiveInGroupRows: Seq[Row] = Seq(
    Row("test1", Row(Seq(1, 2))),
    Row("test2", Row(Seq(3, 4)))
  )

  // primitive in group as protobuf message
  val primitiveInGroupMessages: Seq[PrimitiveInGroupMessage] = Seq(
    PrimitiveInGroupMessage.newBuilder()
      .setBar("test1")
      .setFoo(Foo.newBuilder().addRepeatedField(1).addRepeatedField(2))
      .build(),
    PrimitiveInGroupMessage.newBuilder()
      .setBar("test2")
      .setFoo(Foo.newBuilder().addRepeatedField(3).addRepeatedField(4))
      .build()
  )

  test("read parquet data as protobuf objects") {
    val sqlContext = new SQLContext(sc)

    // create RDD[Row] that contains person data
    val rawPersons = sc.parallelize(personRows)

    // derive person schema
    val personSchema = ProtoReflection.schemaFor[Person].dataType.asInstanceOf[StructType]

    // create person dataframe
    val personsDF = sqlContext.createDataFrame(rawPersons, personSchema)

    // quick checks
    personsDF.agg(Map("id" -> "max")).collect() shouldBe Array(Row(1))
    personsDF.rdd.map(_.getString(0)).collect().sorted shouldBe Array("Alice", "Bob")

    // save as parquet file
    personsDF.save("persons.parquet", SaveMode.Overwrite)

    // read parquet file back but as RDD[Person] instead
    val personsPB = new ProtoParquetRDD(sc, "persons.parquet", classOf[Person]).collect()
    personsPB.foreach(p => logInfo(p.toString))

    // Make sure all information about Bob and Alice are still intact
    // this includes both simple repeated field and message repeated field
    personsPB.sortBy(_.getName) shouldBe personMessages
  }

  test("write protobuf object as parquet") {
    val sqlContext = new SQLContext(sc)

    // create RDD[Person] that contains person data
    val protoPersons = sc.parallelize(personMessages)

    // convert person to DataFrame
    import com.github.saurfang.parquet.proto.spark.sql._
    val personsDF = sqlContext.createDataFrame(protoPersons)
    personsDF.printSchema()
    personsDF.show()

    // save as parquet file
    personsDF.save("persons.parquet", SaveMode.Overwrite)

    // read parquet file back and check the results
    sqlContext.parquetFile("persons.parquet").collect().sortBy(_.getString(0)) shouldBe personRows
  }

  test("convert to df using Class[_]") {
    val sqlContext = new SQLContext(sc)

    // create RDD[Person] that contains person data
    val protoPersons = sc.parallelize(personMessages)

    // convert person to DataFrame
    import com.github.saurfang.parquet.proto.spark.sql._
    val personsDF = sqlContext.createDataFrame(protoPersons)
    val personsDF2 = sqlContext.createDataFrameFromProto(
      protoPersons,
      Class.forName("com.github.saurfang.parquet.proto.AddressBook$Person").asInstanceOf[Class[AbstractMessage]]
    )

    personsDF.collect().sortBy(_.getString(0)) shouldBe personsDF2.collect().sortBy(_.getString(0))
  }

  test("convert primitive repeated field to df using Class [_]") {
    val sqlContext = new SQLContext(sc)

    // create RDD[PrimitiveInGroupMessage] that contains primitve in group data
    val protoPrimitiveInGroup = sc.parallelize(primitiveInGroupMessages)

    // convert primitive in group message to DataFrame
    import com.github.saurfang.parquet.proto.spark.sql._
    val protoPrimitiveInGroupDF = sqlContext.createDataFrame(protoPrimitiveInGroup)
    val protoPrimitiveInGroupDF2 = sqlContext.createDataFrameFromProto(
      protoPrimitiveInGroup,
      Class.forName("com.github.saurfang.parquet.proto.PrimitiveInGroup$PrimitiveInGroupMessage").asInstanceOf[Class[AbstractMessage]]
    )

    protoPrimitiveInGroupDF.collect().sortBy(_.getString(0)) shouldBe protoPrimitiveInGroupDF2.collect().sortBy(_.getString(0))


  }
}
