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

import org.apache.spark.SparkConf
import org.apache.spark.sql.GlutenSQLTestsBaseTrait

// TODO: this needs a lot more testing but it's currently not easy to test with the parquet
// writer abstractions. Revisit.
class GlutenParquetEncodingSuite extends ParquetEncodingSuite with GlutenSQLTestsBaseTrait {
  // The tests read back the encodings parquet-mr writes, e.g. BYTE_STREAM_SPLIT, which the
  // native writer does not write. The reads still run in Velox.
  override def sparkConf: SparkConf =
    super.sparkConf.set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")
}
