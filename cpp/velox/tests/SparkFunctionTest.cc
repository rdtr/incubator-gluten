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

#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "operators/functions/RegistrationAllFunctions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::functions::sparksql::test;
using namespace facebook::velox;

namespace {
constexpr const char* kSparkAnsiCast = "spark_ansi_cast";
constexpr const char* kSparkLegacyCast = "spark_legacy_cast";

std::string sparkAnsiEnabledConfigKey() {
  return functions::sparksql::SparkQueryConfig::qualify(functions::sparksql::SparkQueryConfig::kAnsiEnabled);
}
} // namespace

class SparkFunctionTest : public SparkFunctionBaseTest {
 public:
  SparkFunctionTest() {
    gluten::registerAllFunctions();
  }

 protected:
  template <typename T>
  void runRoundTest(const std::vector<std::tuple<T, T>>& data) {
    auto result = evaluate<SimpleVector<T>>("round(c0)", makeRowVector({makeFlatVector<T, 0>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<1>(data[i]));
    }
  }

  template <typename T>
  void runRoundWithDecimalTest(const std::vector<std::tuple<T, int32_t, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "round(c0, c1)", makeRowVector({makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<2>(data[i]));
    }
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundFloatData() {
    return {
        {1.0, 1.0},
        {1.9, 2.0},
        {1.3, 1.0},
        {0.0, 0.0},
        {0.9999, 1.0},
        {-0.9999, -1.0},
        {1.0 / 9999999, 0},
        {123123123.0 / 9999999, 12.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundIntegralData() {
    return {{1, 1}, {0, 0}, {-1, -1}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecFloatAndDoubleData() {
    return {{1.122112, 0, 1},       {1.129, 1, 1.1},        {1.129, 2, 1.13},         {1.0 / 3, 0, 0.0},
            {1.0 / 3, 1, 0.3},      {1.0 / 3, 2, 0.33},     {1.0 / 3, 6, 0.333333},   {-1.122112, 0, -1},
            {-1.129, 1, -1.1},      {-1.129, 2, -1.13},     {-1.129, 2, -1.13},       {-1.0 / 3, 0, 0.0},
            {-1.0 / 3, 1, -0.3},    {-1.0 / 3, 2, -0.33},   {-1.0 / 3, 6, -0.333333}, {1.0, -1, 0.0},
            {0.0, -2, 0.0},         {-1.0, -3, 0.0},        {11111.0, -1, 11110.0},   {11111.0, -2, 11100.0},
            {11111.0, -3, 11000.0}, {11111.0, -4, 10000.0}, {0.574, 2, 0.57},         {-0.574, 2, -0.57}};
  }

  // Checks round(c0, c1) against Spark's
  // BigDecimal.valueOf(c0).setScale(c1, HALF_UP), including the sign of zero.
  template <typename T>
  void runRoundFloatingPointTest(const std::vector<std::tuple<T, int32_t, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "round(c0, c1)", makeRowVector({makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      const auto [input, decimals, expected] = data[i];
      SCOPED_TRACE(fmt::format("round({}, {})", input, decimals));
      const T actual = result->valueAt(i);
      if (std::isnan(expected)) {
        ASSERT_TRUE(std::isnan(actual));
      } else {
        ASSERT_EQ(actual, expected);
        ASSERT_EQ(std::signbit(actual), std::signbit(expected));
      }
    }
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {0, 0, 0},
        {-1, 0, -1},
        {1, 1, 1},
        {0, 1, 0},
        {-1, 1, -1},
        {1, 10, 1},
        {0, 10, 0},
        {-1, 10, -1},
        {1, -1, 0},
        {0, -2, 0},
        {-1, -3, 0}};
  }
};

TEST_F(SparkFunctionTest, round) {
  runRoundTest<float>(testRoundFloatData<float>());
  runRoundTest<double>(testRoundFloatData<double>());
  runRoundTest<int64_t>(testRoundIntegralData<int64_t>());
  runRoundTest<int32_t>(testRoundIntegralData<int32_t>());
  runRoundTest<int16_t>(testRoundIntegralData<int16_t>());
  runRoundTest<int8_t>(testRoundIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatAndDoubleData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatAndDoubleData<double>());
  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}

// Spark rounds the digits of Double.toString(x), the shortest decimal that
// reads back as x; a FLOAT is widened to double first.
TEST_F(SparkFunctionTest, roundDouble) {
  constexpr double kInf = std::numeric_limits<double>::infinity();
  constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
  runRoundFloatingPointTest<double>({
      // Shortest form 0.285 is a tie, although the double is below it.
      {0.285, 2, 0.29},
      {0.575, 2, 0.58},
      {-0.575, 2, -0.58},
      {99.995, 2, 100.0},
      {123456789012.345, 2, 123456789012.35},
      // One ulp below a tie, from arithmetic (0.285 * 100, 1.005 * 100).
      {28.499999999999996, 0, 28.0},
      {100.49999999999999, 0, 100.0},
      {0.49999999999999994, 0, 0.0},
      {0.5549999999999999, 2, 0.55},
      // Exact ties round away from zero.
      {2.5, 0, 3.0},
      {-2.5, 0, -3.0},
      // Large values have no digits to round off.
      {4503599627370496.0, 0, 4503599627370496.0},
      {9007199254740992.0, 0, 9007199254740992.0},
      {1.7e15, 2, 1.7e15},
      {35184372088832.0, 2, 35184372088832.0},
      {1e300, 2, 1e300},
      {1.5, 400, 1.5},
      {1.0 / 3, 10, 0.3333333333},
      // Zero has no sign in BigDecimal.
      {-0.4, 0, 0.0},
      {-0.001, 2, 0.0},
      {-0.0, 0, 0.0},
      // Double.toString(4.9E-324) has two digits, 4.9E-324, not 5E-324.
      {4.9e-324, 323, 0.0},
      {kInf, 2, kInf},
      {-kInf, 2, -kInf},
      {kNaN, 2, kNaN},
  });
}

TEST_F(SparkFunctionTest, roundFloat) {
  constexpr float kInf = std::numeric_limits<float>::infinity();
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  runRoundFloatingPointTest<float>({
      // The widened float is below the tie: (double) 0.285f = 0.28499999642372131.
      {0.285f, 2, 0.28f},
      {0.575f, 2, 0.57f},
      {-0.575f, 2, -0.57f},
      {2.675f, 2, 2.67f},
      {1.005f, 2, 1.0f},
      {0.35f, 1, 0.3f},
      {67950.49f, 0, 67950.0f},
      {1.1f, 1, 1.1f},
      {99999.99f, 2, 99999.99f},
      // Exact ties round away from zero.
      {0.125f, 2, 0.13f},
      {-0.125f, 2, -0.13f},
      {2.5f, 0, 3.0f},
      // Large values have no digits to round off.
      {5000000.0f, 0, 5000000.0f},
      {16777216.0f, 0, 16777216.0f},
      {70000.0f, 2, 70000.0f},
      {std::numeric_limits<float>::max(), 2, std::numeric_limits<float>::max()},
      {std::numeric_limits<float>::denorm_min(), 45, std::numeric_limits<float>::denorm_min()},
      // Zero has no sign in BigDecimal.
      {-0.4f, 0, 0.0f},
      {-0.0f, 0, 0.0f},
      {kInf, 2, kInf},
      {-kInf, 2, -kInf},
      {kNaN, 2, kNaN},
  });
}

TEST_F(SparkFunctionTest, expressionLevelAnsiCastIgnoresSessionAnsiOff) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "false"}});
  auto input = makeRowVector({makeFlatVector<std::string>({"2147483648"})});
  core::TypedExprPtr field = std::make_shared<const core::FieldAccessTypedExpr>(VARCHAR(), "c0");
  auto ansiCast =
      std::make_shared<const core::CallTypedExpr>(INTEGER(), std::vector<core::TypedExprPtr>{field}, kSparkAnsiCast);

  VELOX_ASSERT_THROW(evaluate(ansiCast, input), "Cannot cast");
}

TEST_F(SparkFunctionTest, expressionLevelLegacyCastIgnoresSessionAnsiOn) {
  queryCtx_->testingOverrideConfigUnsafe({{sparkAnsiEnabledConfigKey(), "true"}});
  auto input = makeRowVector({makeFlatVector<int32_t>({1234567})});
  core::TypedExprPtr field = std::make_shared<const core::FieldAccessTypedExpr>(INTEGER(), "c0");
  auto legacyCast =
      std::make_shared<const core::CallTypedExpr>(TINYINT(), std::vector<core::TypedExprPtr>{field}, kSparkLegacyCast);

  facebook::velox::test::assertEqualVectors(makeFlatVector<int8_t>({-121}), evaluate(legacyCast, input));
}

// Spark GetMapValue is emitted as get_map_value and registered through the
// function overlay as a map-only, pushdown-capable subscript, so that
// Expr::extractSubfields() yields m["k"] instead of the bare column path. This
// keeps map-key pruning declared via HiveColumnHandle::requiredSubfields
// effective when the remaining filter references the map.
TEST_F(SparkFunctionTest, getMapValueSubfieldExtraction) {
  // Spark function names throughout: the test base registers only the Spark
  // functions, so the parser's `>` (gt) does not resolve.
  auto mapRow = ROW({"m"}, {MAP(VARCHAR(), BIGINT())});
  auto exprSet = compileExpression("greaterthan(get_map_value(m, 'k'), 0)", mapRow);
  auto subfields = exprSet->expr(0)->extractSubfields();
  ASSERT_EQ(subfields.size(), 1);
  EXPECT_EQ(subfields[0].toString(), "m[\"k\"]");

  // Velox's element_at stays non-pushdown: it also serves arrays, and a
  // Subfield path cannot express a negative array index.
  exprSet = compileExpression("greaterthan(element_at(m, 'k'), 0)", mapRow);
  subfields = exprSet->expr(0)->extractSubfields();
  ASSERT_EQ(subfields.size(), 1);
  EXPECT_EQ(subfields[0].toString(), "m");
}

TEST_F(SparkFunctionTest, getMapValueSemantics) {
  auto maps = makeMapVector<std::string, int64_t>({{{"k", 1}}, {{"x", 2}}, {}});
  auto result = evaluate("get_map_value(c0, 'k')", makeRowVector({maps}));
  facebook::velox::test::assertEqualVectors(makeNullableFlatVector<int64_t>({1, std::nullopt, std::nullopt}), result);

  // Integer literals parse as BIGINT, and the key must match the map's key type exactly.
  auto intKeyed = makeMapVector<int64_t, std::string>({{{1, "a"}, {2, "b"}}, {{3, "c"}}});
  result = evaluate("get_map_value(c0, 2)", makeRowVector({intKeyed}));
  facebook::velox::test::assertEqualVectors(makeNullableFlatVector<std::string>({"b", std::nullopt}), result);
}
