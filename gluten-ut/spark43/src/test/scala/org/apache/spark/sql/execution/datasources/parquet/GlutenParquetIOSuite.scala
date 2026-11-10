/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.parquet

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType, StringType, StructType}

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.hadoop.ParquetOutputFormat

import java.nio.file.Files

import scala.jdk.CollectionConverters._

/** A test suite that tests basic Parquet I/O. */
class GlutenParquetIOSuite extends ParquetIOSuite with GlutenSQLTestsBaseTrait {
  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath("sql", "core", "src", "test", "resources").toString + "/" + fileName
  }

  override protected def readResourceParquetFile(name: String): DataFrame = {
    spark.read.parquet(testFile(name))
  }

  // The Spark test also runs the scan node alone with VerifyNoAdditionalScanOutputExec, which
  // does not work for Gluten's scan node. Check only the result here.
  testGluten(
    "SPARK-53535: vectorized reader: missing all struct fields, struct with complex fields") {
    val data = Seq(
      Row(Row(Seq(11, 12, null, 14), Row("21", 22), Row(true)), 100),
      Row(Row(Seq(11, 12, null, 14), Row("21", 22), Row(false)), 100),
      Row(null, 100)
    )

    val tableSchema = new StructType()
      .add(
        "_1",
        new StructType()
          .add("_1", ArrayType(IntegerType, containsNull = true))
          .add("_2", new StructType().add("_1", StringType).add("_2", IntegerType))
          .add("_3", new StructType().add("_1", BooleanType))
      )
      .add("_2", IntegerType)

    val readSchema = new StructType()
      .add("_1", new StructType().add("_101", IntegerType).add("_102", LongType))

    withTempPath {
      path =>
        val file = path.getCanonicalPath
        spark.createDataFrame(data.asJava, tableSchema).write.partitionBy("_2").parquet(file)

        for {
          offheapEnabled <- Seq(true, false)
          returnNullStructIfAllFieldsMissing <- Seq(true, false)
        } {
          withSQLConf(
            SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.key ->
              returnNullStructIfAllFieldsMissing.toString,
            SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
            SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString
          ) {
            val expectedAnswer = if (!returnNullStructIfAllFieldsMissing) {
              Row(Row(null, null), 100) :: Row(Row(null, null), 100) :: Row(null, 100) :: Nil
            } else {
              Row(null, 100) :: Row(null, 100) :: Row(null, 100) :: Nil
            }

            withAllParquetReaders {
              checkAnswer(spark.read.schema(readSchema).parquet(file), expectedAnswer)
            }
          }
        }
    }
  }

  testGluten(
    "SPARK-54220: vectorized reader: missing all struct fields, struct with NullType only") {
    val data = Seq(
      Tuple1((null, null)),
      Tuple1((null, null)),
      Tuple1(null)
    )
    val readSchema = new StructType().add(
      "_1",
      new StructType()
        .add("_3", IntegerType, nullable = true)
        .add("_4", StringType, nullable = true),
      nullable = true)
    val expectedAnswer = Row(Row(null, null)) :: Row(Row(null, null)) :: Row(null) :: Nil

    withParquetFile(data) {
      file =>
        for (offheapEnabled <- Seq(true, false)) {
          withSQLConf(
            SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
            SQLConf.LEGACY_PARQUET_RETURN_NULL_STRUCT_IF_ALL_FIELDS_MISSING.key -> "false",
            SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString
          ) {
            withAllParquetReaders {
              val df = spark.read.schema(readSchema).parquet(file)
              checkAnswer(df, expectedAnswer)
            }
          }
        }
    }
  }

  /**
   * Like `ParquetIOSuite.readCorruptedRleSection`: writes 100 rows of `column` to a single
   * uncompressed data page, lets `corrupt` modify the RLE section that starts with
   * `03 00 00 00 C8 01 01` and returns the error of reading the file. The file is written by
   * parquet-mr, since the native writer ignores `PARQUET_2_0` and writes every column as optional.
   */
  private def readCorruptedRleSectionWithVelox(
      column: String,
      writerVersion: String = "PARQUET_1_0")(corrupt: (Array[Byte], Int) => Unit): Throwable = {
    var error: Throwable = null
    withTempPath {
      dir =>
        val path = dir.getCanonicalPath
        withSQLConf(GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false") {
          spark
            .range(100)
            .selectExpr(column)
            .coalesce(1)
            .write
            .option(ParquetOutputFormat.ENABLE_DICTIONARY, "false")
            .option(ParquetOutputFormat.WRITER_VERSION, writerVersion)
            .option("compression", "uncompressed")
            .parquet(path)
        }
        val file = dir.listFiles().filter(_.getName.endsWith(".parquet")).head
        val bytes = Files.readAllBytes(file.toPath)
        val section = Array[Byte](3, 0, 0, 0, 0xc8.toByte, 0x01, 1)
        val offset = bytes.indexOfSlice(section)
        assert(offset >= 0 && bytes.indexOfSlice(section, offset + 1) < 0)
        corrupt(bytes, offset)
        Files.write(file.toPath, bytes)
        dir.listFiles().filter(_.getName.endsWith(".crc")).foreach(_.delete())
        error = intercept[SparkException](spark.read.parquet(path).collect())
    }
    error
  }

  private def setMaxLength(bytes: Array[Byte], offset: Int): Unit =
    BytesUtils.intToBytes(Int.MaxValue).copyToArray(bytes, offset)

  /** Checks that the read failed with the Velox error about the corrupted page. */
  private def assertVeloxReadError(e: Throwable, message: String): Unit = {
    val messages = ExceptionUtils.getThrowableList(e).asScala.flatMap(c => Option(c.getMessage))
    assert(messages.exists(_.contains(message)), e)
  }

  // The Spark tests check the error of the vanilla vectorized reader. Velox reports the
  // corrupted page with its own error; what matters is that the read fails.
  testGluten("SPARK-59832: reading truncated definition levels fails") {
    val e = readCorruptedRleSectionWithVelox("if(id < 0, null, cast(id as int)) as a") {
      (bytes, offset) => bytes(offset + 4) = 0x80.toByte
    }
    assertVeloxReadError(e, "Invalid varint value: too few bytes")
  }

  testGluten("SPARK-59832: reading an invalid definition level length fails") {
    val e = readCorruptedRleSectionWithVelox("if(id < 0, null, cast(id as int)) as a")(setMaxLength)
    assertVeloxReadError(e, "Definition level length 2147483647 exceeds remaining page size")
  }

  testGluten("SPARK-59832: reading an invalid length of RLE booleans in a V2 page fails") {
    val e = readCorruptedRleSectionWithVelox("id >= 0 as b", writerVersion = "PARQUET_2_0")(
      setMaxLength)
    assertVeloxReadError(e, "Received invalid number of bytes: 2147483647 (corrupt data page?)")
  }
}
