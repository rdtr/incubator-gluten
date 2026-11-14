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

#pragma once

#include <folly/CPortability.h>
#include <stdint.h>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <type_traits>

namespace gluten {

namespace detail {

/// Rounds a finite double the way Spark does:
/// java.math.BigDecimal.valueOf(x).setScale(decimals, HALF_UP), then
/// doubleValue() or floatValue() depending on TResult. BigDecimal.valueOf
/// takes the digits of Double.toString(x), the shortest decimal that reads
/// back as x, so rounding works on those digits rather than on the binary
/// value: round(0.285, 2) is 0.29 even though the double is below 0.285. A
/// FLOAT input is widened to double first, as in Spark, so its exact value is
/// rounded: round(cast(0.285 as float), 2) is 0.28.
/// Writes the decimal digits of |x| (x finite, non-zero) to digits, so that
/// |x| = 0.d1d2...dn * 10^point with d1 != 0. Without precision, the digits
/// are the shortest that read back as x; with precision p, they are x
/// correctly rounded to p + 1 significant digits.
template <typename... TPrecision>
inline void decimalDigits(double x, char* digits, int& length, int& point, TPrecision... precision) {
  // Scientific form: "d", "d.ddd", followed by "e+XX" or "e-XXX".
  char text[32];
  const auto end =
      std::to_chars(text, text + sizeof(text), std::fabs(x), std::chars_format::scientific, precision...).ptr;
  length = 0;
  const char* p = text;
  for (; p < end && *p != 'e'; ++p) {
    if (*p != '.') {
      digits[length++] = *p;
    }
  }
  // Exponent after 'e', always with a sign.
  ++p;
  const bool negativeExponent = *p++ == '-';
  int exponent = 0;
  for (; p < end; ++p) {
    exponent = exponent * 10 + (*p - '0');
  }
  point = (negativeExponent ? -exponent : exponent) + 1;
}

template <typename TResult>
TResult roundShortestDecimal(double x, int32_t decimals) {
  // At most 17 significant digits.
  char digits[24];
  int length;
  int point;
  decimalDigits(x, digits, length, point);
  if (length == 1) {
    // Double.toString uses at least two digits and picks the one closest to x
    // (4.9E-324, not 5E-324). It differs from the 1-digit form only for
    // subnormals.
    decimalDigits(x, digits, length, point, 1);
  }

  // Number of digits kept at the given scale.
  const int64_t keep = static_cast<int64_t>(point) + decimals;
  if (keep >= length) {
    // Nothing to round off. x is the shortest decimal read back, so it is
    // returned as is.
    return static_cast<TResult>(x);
  }
  if (keep < 0 || (keep == 0 && digits[0] < '5')) {
    // Rounds to zero, which BigDecimal has no sign for.
    return 0;
  }

  // Round half up (away from zero) on the kept digits.
  int resultLength;
  int resultPoint = point;
  if (keep == 0) {
    // 0.5 <= 0.d1... < 1 at the rounding position: rounds up to one unit.
    digits[0] = '1';
    resultLength = 1;
    resultPoint = point + 1;
  } else {
    resultLength = static_cast<int>(keep);
    if (digits[resultLength] >= '5') {
      int i = resultLength - 1;
      while (i >= 0 && digits[i] == '9') {
        --i;
      }
      if (i < 0) {
        // 99.5 -> 100: one digit '1', one position higher.
        digits[0] = '1';
        resultLength = 1;
        resultPoint = point + 1;
      } else {
        ++digits[i];
        resultLength = i + 1;
      }
    }
  }

  // Read "-d1...dk e(resultPoint - k)" back. strtod and strtof round
  // correctly, like BigDecimal.doubleValue() and floatValue(), and the text
  // has no decimal point, so the locale does not matter.
  char text[48];
  int textLength = 0;
  if (std::signbit(x)) {
    text[textLength++] = '-';
  }
  for (int i = 0; i < resultLength; ++i) {
    text[textLength++] = digits[i];
  }
  text[textLength++] = 'e';
  const auto end = std::to_chars(text + textLength, text + sizeof(text) - 1, resultPoint - resultLength).ptr;
  *end = '\0';
  if constexpr (std::is_same_v<TResult, float>) {
    return std::strtof(text, nullptr);
  } else {
    return std::strtod(text, nullptr);
  }
}

/// Fast path of roundShortestDecimal for the common case where x * 10^decimals
/// is not close to a tie. Returns false when it cannot decide.
template <typename TResult>
FOLLY_ALWAYS_INLINE bool tryRoundFast(double x, int32_t decimals, TResult& result) {
  // Powers of ten that are exact doubles.
  static constexpr double kPowersOf10[] = {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,  1e10, 1e11,
                                           1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};
  if (decimals < 0 || decimals > 22 || !std::isnormal(x)) {
    return false;
  }
  const double factor = kPowersOf10[decimals];
  const double scaled = std::fabs(x) * factor;
  if (!(scaled < 0x1p52)) {
    return false;
  }
  // The shortest decimal s of x is within ulp(x) / 2 <= 2^-53 * |x| of x, and
  // the product is within 2^-53 * scaled of |x| * factor, so s * factor is
  // within about 2^-52 * scaled of scaled. Outside a margin of 2^-50 * scaled
  // around the tie, s * factor rounds half up to the same integer as scaled.
  const double integral = std::floor(scaled);
  const double fraction = scaled - integral; // Exact below 2^52.
  if (std::fabs(fraction - 0.5) <= scaled * 0x1p-50) {
    return false;
  }
  const double rounded = fraction > 0.5 ? integral + 1 : integral;
  if (rounded == 0) {
    result = 0;
    return true;
  }
  // rounded and factor are exact, so the division rounds rounded * 10^-decimals
  // correctly to double, as BigDecimal.doubleValue() does.
  double quotient = rounded / factor;
  if (x < 0) {
    quotient = -quotient;
  }
  if constexpr (std::is_same_v<TResult, float>) {
    // BigDecimal.floatValue() rounds the decimal to float directly. Rounding
    // the double quotient again gives the same float unless the quotient is a
    // midpoint between two floats.
    const float narrowed = static_cast<float>(quotient);
    const double difference = quotient - static_cast<double>(narrowed);
    if (difference != 0) {
      if (!std::isnormal(narrowed)) {
        return false;
      }
      const float neighbor = std::nextafter(
          narrowed, difference > 0 ? std::numeric_limits<float>::infinity() : -std::numeric_limits<float>::infinity());
      if (2 * quotient == static_cast<double>(narrowed) + static_cast<double>(neighbor)) {
        return false;
      }
    }
    result = narrowed;
  } else {
    result = quotient;
  }
  return true;
}

} // namespace detail

template <typename T>
/// Spark's round. Integral numbers keep their value unless decimals is
/// negative. Floating-point numbers follow Spark's BigDecimal rounding, see
/// detail::roundShortestDecimal.
struct RoundFunction {
  template <typename TNum, typename TDecimals, bool alwaysRoundNegDec = true>
  FOLLY_ALWAYS_INLINE TNum round(const TNum& number, const TDecimals& decimals = 0) {
    static_assert(!std::is_same_v<TNum, bool> && "round not supported for bool");

    if constexpr (std::is_floating_point_v<TNum>) {
      if (!std::isfinite(number)) {
        return number;
      }
      if (decimals >= 0 && std::trunc(number) == number) {
        // An integral value never has digits to round off. Adding 0.0 turns
        // -0.0 into 0.0, as BigDecimal does.
        return number + TNum(0);
      }
      TNum result;
      if (detail::tryRoundFast(static_cast<double>(number), decimals, result)) {
        return result;
      }
      return detail::roundShortestDecimal<TNum>(static_cast<double>(number), decimals);
    } else {
      if constexpr (alwaysRoundNegDec) {
        if (decimals >= 0)
          return number;
      } else {
        return number;
      }

      // Using long double for high precision during intermediate calculations.
      long double factor = std::pow(10.0L, static_cast<long double>(decimals));
      static const TNum kInf = std::numeric_limits<TNum>::infinity();

      if (number < 0) {
        return static_cast<TNum>((std::round(std::nextafter(number, -kInf) * factor * -1) / factor) * -1);
      }
      return static_cast<TNum>(std::round(std::nextafter(number, kInf) * factor) / factor);
    }
  }
  template <typename TInput>
  FOLLY_ALWAYS_INLINE void call(TInput& result, const TInput& a, const int32_t b = 0) {
    result = round(a, b);
  }
};
} // namespace gluten
