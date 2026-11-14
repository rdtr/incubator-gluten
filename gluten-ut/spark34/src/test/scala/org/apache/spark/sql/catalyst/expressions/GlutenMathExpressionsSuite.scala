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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.types._

import java.nio.charset.StandardCharsets

class GlutenMathExpressionsSuite extends MathExpressionsSuite with GlutenTestsTrait {
  testGluten("unhex") {
    checkEvaluation(Unhex(Literal.create(null, StringType)), null)
    checkEvaluation(Unhex(Literal("737472696E67")), "string".getBytes(StandardCharsets.UTF_8))
    checkEvaluation(Unhex(Literal("")), new Array[Byte](0))
    checkEvaluation(Unhex(Literal("F")), Array[Byte](15))
    checkEvaluation(Unhex(Literal("ff")), Array[Byte](-1))

    checkEvaluation(Unhex(Literal("123")), Array[Byte](1, 35))
    checkEvaluation(Unhex(Literal("12345")), Array[Byte](1, 35, 69))
    // scalastyle:off
    // Turn off scala style for non-ascii chars
    checkEvaluation(Unhex(Literal("E4B889E9878DE79A84")), "三重的".getBytes(StandardCharsets.UTF_8))
    // scalastyle:on
    checkConsistencyBetweenInterpretedAndCodegen((e: Expression) => Unhex(e), StringType)
  }

  // Split out of "unhex" so that ClickHouse, which does not return null for invalid hex
  // input (see https://github.com/apache/gluten/issues/7232), can exclude only this case.
  testGluten("unhex returns null for invalid hex input") {
    checkEvaluation(Unhex(Literal("GG")), null)
    // scalastyle:off
    // Turn off scala style for non-ascii chars
    checkEvaluation(Unhex(Literal("三重的")), null)
    // scalastyle:on
  }
}
