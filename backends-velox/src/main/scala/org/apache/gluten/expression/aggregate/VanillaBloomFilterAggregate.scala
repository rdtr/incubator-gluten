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
package org.apache.gluten.expression.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{BloomFilterAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.sketch.BloomFilter

/**
 * Vanilla Spark's `bloom_filter_agg` under a Gluten-owned class.
 *
 * Behaves exactly like [[BloomFilterAggregate]] and is never offloaded, so its result is in Spark's
 * serialized format. [[org.apache.gluten.extension.RewriteBloomFilterPairs]] turns a user-written
 * `bloom_filter_agg` into this class when it cannot be offloaded together with a `might_contain`
 * consumer, e.g. a build-only aggregate whose bytes leave the query (`DataFrame.stat.bloomFilter`,
 * Spark Connect), or a string input. The separate class lets the later physical rewrite
 * ([[org.apache.gluten.extension.BloomFilterMightContainJointRewriteRule]]) tell such an aggregate
 * apart from the runtime filters Spark's `InjectRuntimeFilter` adds after the optimizer rule ran,
 * which must still be offloaded.
 */
case class VanillaBloomFilterAggregate(
    child: Expression,
    estimatedNumItemsExpression: Expression,
    numBitsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[BloomFilter]
  with TernaryLike[Expression] {

  private val delegate = BloomFilterAggregate(
    child,
    estimatedNumItemsExpression,
    numBitsExpression,
    mutableAggBufferOffset,
    inputAggBufferOffset)

  override def prettyName: String = "bloom_filter_agg"

  override def first: Expression = child

  override def second: Expression = estimatedNumItemsExpression

  override def third: Expression = numBitsExpression

  override def checkInputDataTypes(): TypeCheckResult = delegate.checkInputDataTypes()

  override def nullable: Boolean = delegate.nullable

  override def dataType: DataType = delegate.dataType

  override protected def withNewChildrenInternal(
      newChild: Expression,
      newEstimatedNumItemsExpression: Expression,
      newNumBitsExpression: Expression): VanillaBloomFilterAggregate = {
    copy(
      child = newChild,
      estimatedNumItemsExpression = newEstimatedNumItemsExpression,
      numBitsExpression = newNumBitsExpression)
  }

  override def createAggregationBuffer(): BloomFilter = delegate.createAggregationBuffer()

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter =
    delegate.update(buffer, input)

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter =
    delegate.merge(buffer, input)

  override def eval(buffer: BloomFilter): Any = delegate.eval(buffer)

  override def serialize(buffer: BloomFilter): Array[Byte] = delegate.serialize(buffer)

  override def deserialize(bytes: Array[Byte]): BloomFilter = delegate.deserialize(bytes)

  override def withNewMutableAggBufferOffset(newOffset: Int): VanillaBloomFilterAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): VanillaBloomFilterAggregate =
    copy(inputAggBufferOffset = newOffset)
}

object VanillaBloomFilterAggregate {
  def apply(agg: BloomFilterAggregate): VanillaBloomFilterAggregate = {
    VanillaBloomFilterAggregate(
      agg.child,
      agg.estimatedNumItemsExpression,
      agg.numBitsExpression,
      agg.mutableAggBufferOffset,
      agg.inputAggBufferOffset)
  }
}
