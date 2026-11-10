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

class GlutenParquetColumnIndexSuite extends ParquetColumnIndexSuite with GlutenSQLTestsBaseTrait {
  // The tests check that the reader handles unaligned pages, so they need parquet-mr to write
  // the page layout they ask for. The native writer puts the filter column into a single page
  // (the tests check for more than one page since SPARK-59837). The reads still run in Velox.
  override def sparkConf: SparkConf =
    super.sparkConf.set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")
}
