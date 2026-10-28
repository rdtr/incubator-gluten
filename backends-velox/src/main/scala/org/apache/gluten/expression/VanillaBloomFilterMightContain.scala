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
package org.apache.gluten.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, BloomFilterMightContain, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

/**
 * Vanilla Spark's `might_contain` under a Gluten-owned class.
 *
 * Behaves exactly like [[BloomFilterMightContain]] and is never offloaded, so it reads bytes in
 * Spark's serialized format. [[org.apache.gluten.extension.RewriteBloomFilterPairs]] turns a
 * user-written `might_contain` into this class when its bloom filter is not produced by a
 * `bloom_filter_agg` that can be offloaded in the same plan, e.g. a Spark-format constant or a
 * subquery that reads a stored filter. See
 * [[org.apache.gluten.expression.aggregate.VanillaBloomFilterAggregate]] for why a separate class
 * is needed.
 */
case class VanillaBloomFilterMightContain(
    bloomFilterExpression: Expression,
    valueExpression: Expression)
  extends BinaryExpression {

  private val delegate = BloomFilterMightContain(bloomFilterExpression, valueExpression)

  override def prettyName: String = "might_contain"

  override def left: Expression = bloomFilterExpression

  override def right: Expression = valueExpression

  override def nullable: Boolean = delegate.nullable

  override def dataType: DataType = delegate.dataType

  override def checkInputDataTypes(): TypeCheckResult = delegate.checkInputDataTypes()

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): VanillaBloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression, valueExpression = newValueExpression)

  override def eval(input: InternalRow): Any = delegate.eval(input)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    delegate.genCode(ctx)
}
