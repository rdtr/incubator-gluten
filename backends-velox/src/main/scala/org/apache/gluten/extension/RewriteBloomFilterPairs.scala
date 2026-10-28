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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.{VanillaBloomFilterMightContain, VeloxBloomFilterMightContain}
import org.apache.gluten.expression.aggregate.{VanillaBloomFilterAggregate, VeloxBloomFilterAggregate}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, BloomFilterMightContain, Cast, Expression, IsNotNull, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BinaryType, ByteType, IntegerType, LongType, ShortType}

import scala.util.Try

/**
 * Decides on the logical plan which `bloom_filter_agg` / `might_contain` pairs are offloaded.
 *
 * Velox's bloom filter has a different serialized format and hash function from Spark's, so the
 * producer (`bloom_filter_agg`) and the consumer (`might_contain`) of a filter must be offloaded
 * together or not at all. Whether they belong together is only visible in the logical plan: under
 * AQE each scalar subquery is compiled into its own query stage, and a physical rule sees the
 * aggregate without its consumer. This rule therefore runs in Spark's "Pre CBO Rules" batch, before
 * `InjectRuntimeFilter` and `MergeScalarSubqueries`, and its decision is baked into the plan that
 * AQE later compiles.
 *
 * Every Spark `BloomFilterAggregate` / `BloomFilterMightContain` in the plan is replaced:
 *   - a `might_contain` whose bloom filter is a scalar subquery that directly computes a
 *     `bloom_filter_agg` over a bigint-compatible input becomes the Velox pair
 *     ([[VeloxBloomFilterAggregate]] / [[VeloxBloomFilterMightContain]]). int/short/byte inputs are
 *     cast to bigint, like vanilla Spark widens them, and a nullable input gets
 *     `FILTER (WHERE input IS NOT NULL)` because the native aggregate rejects nulls while Spark
 *     ignores them. A Velox-format constant is accepted as well;
 *   - everything else becomes the vanilla marker ([[VanillaBloomFilterAggregate]] /
 *     [[VanillaBloomFilterMightContain]]), which behaves exactly like Spark's expression and is
 *     never offloaded. This keeps Spark's byte format for a build-only `bloom_filter_agg` whose
 *     bytes leave the query (`DataFrame.stat.bloomFilter`, Spark Connect, a cached filter), a
 *     string input, a Spark-format constant, and a subquery that reads a stored filter.
 *
 * Afterwards the only Spark `BloomFilterAggregate` / `BloomFilterMightContain` left in a plan are
 * the runtime filters that `InjectRuntimeFilter` adds later. Those always pair a
 * `bloom_filter_agg(xxhash64(...))` subquery with a `might_contain(..., xxhash64(...))` consumer,
 * and [[BloomFilterMightContainJointRewriteRule]] offloads them unconditionally.
 *
 * `OptimizeSubqueries` optimizes each subquery on its own before the outer plan, so the aggregate
 * inside a subquery first becomes the vanilla marker and is promoted to the Velox variant once the
 * outer pass finds its consumer. An already offloaded [[VeloxBloomFilterAggregate]] is left alone,
 * which keeps the rule idempotent when the optimizer runs again on a subquery.
 */
case class RewriteBloomFilterPairs(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!GlutenConfig.get.enableNativeBloomFilter) {
      return plan
    }
    plan.transformWithSubqueries {
      case p =>
        p.transformExpressions {
          case mc: BloomFilterMightContain => rewriteConsumer(mc)
          // Not consumed by a might_contain in this plan, or rejected by rewriteConsumer.
          case agg: BloomFilterAggregate => VanillaBloomFilterAggregate(agg)
        }
    }
  }

  private def rewriteConsumer(mc: BloomFilterMightContain): Expression = {
    val offloaded = mc.bloomFilterExpression match {
      case s: ScalarSubquery => offloadProducer(s.plan).map(s.withNewPlan)
      case e if isVeloxFormatConstant(e) => Some(e)
      case _ => None
    }
    offloaded match {
      case Some(bloomFilter) => VeloxBloomFilterMightContain(bloomFilter, mc.valueExpression)
      case None => VanillaBloomFilterMightContain(mc.bloomFilterExpression, mc.valueExpression)
    }
  }

  /**
   * The subquery plan with its `bloom_filter_agg` offloaded, if the subquery is a plain
   * `bloom_filter_agg` over an input Velox can hash.
   */
  private def offloadProducer(plan: LogicalPlan): Option[LogicalPlan] = plan match {
    case agg: Aggregate if agg.aggregateExpressions.size == 1 =>
      val producer = agg.aggregateExpressions.head match {
        case Alias(ae: AggregateExpression, _) => Some(ae)
        case ae: AggregateExpression => Some(ae)
        case _ => None
      }
      producer.flatMap {
        ae =>
          offloadAggregate(ae).map {
            newAe => agg.transformExpressions { case e if e.eq(ae) => newAe }
          }
      }
    case _ => None
  }

  private def offloadAggregate(ae: AggregateExpression): Option[AggregateExpression] = {
    ae.aggregateFunction match {
      case _: VeloxBloomFilterAggregate => Some(ae)
      case agg: BloomFilterAggregate =>
        offloadAggregate(
          ae,
          agg.child,
          agg.estimatedNumItemsExpression,
          agg.numBitsExpression,
          agg.mutableAggBufferOffset,
          agg.inputAggBufferOffset)
      case agg: VanillaBloomFilterAggregate =>
        offloadAggregate(
          ae,
          agg.child,
          agg.estimatedNumItemsExpression,
          agg.numBitsExpression,
          agg.mutableAggBufferOffset,
          agg.inputAggBufferOffset)
      case _ => None
    }
  }

  private def offloadAggregate(
      ae: AggregateExpression,
      child: Expression,
      estimatedNumItemsExpression: Expression,
      numBitsExpression: Expression,
      mutableAggBufferOffset: Int,
      inputAggBufferOffset: Int): Option[AggregateExpression] = {
    val longChild = child.dataType match {
      case LongType => Some(child)
      case IntegerType | ShortType | ByteType => Some(Cast(child, LongType))
      // Velox hashes bigint only; Spark's string input has no native counterpart.
      case _ => None
    }
    longChild.map {
      c =>
        val filter = if (child.nullable) {
          Some(ae.filter.map(And(_, IsNotNull(child))).getOrElse(IsNotNull(child)))
        } else {
          ae.filter
        }
        ae.copy(
          aggregateFunction = VeloxBloomFilterAggregate(
            c,
            estimatedNumItemsExpression,
            numBitsExpression,
            mutableAggBufferOffset,
            inputAggBufferOffset),
          filter = filter
        )
    }
  }

  // Velox's serialized bloom filter starts with its version byte (1); Spark's starts with a
  // big-endian int version, so its first byte is 0.
  private def isVeloxFormatConstant(e: Expression): Boolean = {
    e.dataType == BinaryType && e.foldable && {
      val bytes = Try(e.eval()).getOrElse(null).asInstanceOf[Array[Byte]]
      bytes != null && bytes.nonEmpty && bytes(0) == 1
    }
  }
}
