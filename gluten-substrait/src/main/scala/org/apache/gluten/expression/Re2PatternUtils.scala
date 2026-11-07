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

import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import java.util.Locale

/**
 * Utilities for translating regex patterns from Java syntax to RE2 syntax. Used by any backend
 * whose native engine is RE2 (Velox, Bolt); see
 * [[org.apache.gluten.backendsapi.SparkPlanExecApi#rewriteRegexPattern]].
 */
object Re2PatternUtils {

  /**
   * Implements [[org.apache.gluten.backendsapi.SparkPlanExecApi#rewriteRegexPattern]] for RE2
   * backends. A string literal pattern is translated to RE2 syntax; patterns with constructs that
   * RE2 cannot express with the same meaning throw [[GlutenNotSupportException]] to trigger
   * fallback. Non-literal patterns are returned unchanged.
   */
  def rewriteRegexPattern(pattern: Expression): Expression = pattern match {
    case Literal(p: UTF8String, StringType) =>
      convertJavaPatternToRe2(p.toString) match {
        case Right(translated) => Literal(UTF8String.fromString(translated), StringType)
        case Left(reason) =>
          throw new GlutenNotSupportException(
            s"Regex pattern ${describePattern(p.toString)} cannot be translated to RE2: " +
              s"$reason. Falling back to Spark.")
      }
    case other => other
  }

  private def isHex(c: Char): Boolean =
    (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')

  /**
   * Renders a pattern for error messages: quoted, control characters escaped, and truncated so that
   * very long patterns do not bloat fallback reasons and logs.
   */
  def describePattern(pattern: String, maxLen: Int = 100): String = {
    val sb = new StringBuilder("'")
    var i = 0
    while (i < pattern.length && sb.length < maxLen) {
      pattern.charAt(i) match {
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case ch if ch < ' ' || ch == '\u007f' => sb.append("\\x%02X".format(ch.toInt))
        case ch => sb.append(ch)
      }
      i += 1
    }
    if (i < pattern.length) sb.append(s"...(truncated, ${pattern.length} chars total)")
    sb.append("'").toString
  }

  /** Returns the code unit of a backslash-u escape with 4 hex digits starting at `i`, or -1. */
  private def unicodeEscapeAt(p: String, i: Int): Int = {
    if (
      i + 6 <= p.length && p.charAt(i) == '\\' && p.charAt(i + 1) == 'u' &&
      (2 until 6).forall(k => isHex(p.charAt(i + k)))
    ) {
      Integer.parseInt(p.substring(i + 2, i + 6), 16)
    } else {
      -1
    }
  }

  /**
   * Attempts to translate a Java regex pattern to RE2-compatible syntax. Returns None when
   * [[convertJavaPatternToRe2]] rejects the pattern.
   */
  def translateJavaPatternToRe2(pattern: String): Option[String] =
    convertJavaPatternToRe2(pattern).toOption

  /**
   * Translates a Java regex pattern into an RE2 pattern with the same meaning, or returns
   * `Left(reason)` when the pattern has to be evaluated by java.util.regex.
   *
   * Many constructs compile in both engines but mean different things, so the scanner accepts only
   * a subset of the Java syntax that it understands, and rejects everything else.
   *
   * Translation rules:
   *   - backslash-u followed by 4 hex digits becomes `\x{XXXX}`; a high+low surrogate escape pair
   *     becomes a single `\x{code point}`. An unpaired surrogate is rejected.
   *   - `.` without DOTALL becomes `[^\n\r\x{85}\x{2028}\x{2029}]`: Java's `.` also excludes `\r`,
   *     U+0085, U+2028 and U+2029, RE2's only `\n`.
   *   - `\s` becomes `[\t\n\x0B\f\r ]` and `\S` its negation: Java's `\s` includes the vertical
   *     tab, RE2's does not.
   *   - Escaped characters, escapes that mean the same in both engines, and `\Q...\E` are copied
   *     verbatim. A `.` or `$` inside a character class is a literal and is copied.
   *
   * Rejected constructs:
   *   - `$`: Java also matches before a final line terminator.
   *   - Inline flags other than `s`: `(?i)` is ASCII-only in Java, `(?m)` knows more line
   *     terminators in Java, `(?U)` means Unicode classes in Java and ungreedy in RE2.
   *   - `\b`, `\B`: Java treats non-ASCII letters as word characters before JDK 19.
   *   - `\v`: a vertical whitespace class in Java, the VT character in RE2.
   *   - Backslash followed by a digit: a back reference or an octal escape of a different width in
   *     Java, including inside a character class where Java raises an error.
   *   - A `{` that does not start a repetition: an error in Java, a literal in RE2.
   *   - `\p{..}` names other than Unicode general categories, which Java and RE2 read differently.
   *   - Nested character classes and `&&`: unions and intersections in Java, literals in RE2.
   *   - Lookaround, atomic groups and any escape or construct not listed above.
   */
  def convertJavaPatternToRe2(pattern: String): Either[String, String] =
    new Re2Converter(pattern).run()

  /** Java's `.` without DOTALL: anything except the line terminators. */
  private val Re2Dot = "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]"

  /** Java's `\s` without UNICODE_CHARACTER_CLASS: `[ \t\n\x0B\f\r]` (RE2 lacks `\x0B`). */
  private val SpaceChars = "\\t\\n\\x0B\\f\\r "

  /** Unicode general categories that java.util.regex and RE2 both accept with the same meaning. */
  private val GeneralCategories =
    ("L Lu Ll Lt Lm Lo M Mn Mc Me N Nd Nl No P Pc Pd Ps Pe Pi Pf Po S Sm Sc Sk So Z Zs Zl Zp " +
      "C Cc Cf Co Cs").split(' ').toSet

  /** Escapes followed by one letter that mean the same in both engines. */
  private val SameLetterEscapes = Set('d', 'D', 'w', 'W', 't', 'n', 'r', 'f', 'a')

  final private class Unsupported(val reason: String) extends Exception(reason)

  final private class Re2Converter(p: String) {
    private val out = new java.lang.StringBuilder(p.length)
    private var i = 0
    // DOTALL state of the current group and of the enclosing groups. Java and RE2 both restore
    // inline flags at the end of a group.
    private var dotAll = false
    private var enclosingDotAll: List[Boolean] = Nil

    private def fail(reason: String): Nothing = throw new Unsupported(reason)

    def run(): Either[String, String] = {
      try {
        while (i < p.length) {
          p.charAt(i) match {
            case '\\' => escape(inClass = false)
            case '[' => charClass()
            case '.' =>
              out.append(if (dotAll) "." else Re2Dot)
              i += 1
            case '$' => fail("`$` also matches before a final line terminator in Java")
            case '(' => group()
            case ')' =>
              if (enclosingDotAll.isEmpty) fail("unbalanced `)`")
              dotAll = enclosingDotAll.head
              enclosingDotAll = enclosingDotAll.tail
              out.append(')')
              i += 1
            case '{' => repetition()
            case c =>
              out.append(c)
              i += 1
          }
        }
        if (enclosingDotAll.nonEmpty) fail("unbalanced `(`")
        Right(out.toString)
      } catch {
        case e: Unsupported => Left(e.reason)
      }
    }

    private def copy(n: Int): Unit = {
      out.append(p, i, i + n)
      i += n
    }

    /** Handles the escape at `i`, which is a backslash. */
    private def escape(inClass: Boolean): Unit = {
      if (i + 1 >= p.length) fail("trailing `\\`")
      val c = p.charAt(i + 1)
      c match {
        case 'u' => unicodeEscape()
        case 's' =>
          out.append(if (inClass) SpaceChars else s"[$SpaceChars]")
          i += 2
        case 'S' if !inClass =>
          out.append(s"[^$SpaceChars]")
          i += 2
        case _ if SameLetterEscapes.contains(c) => copy(2)
        case 'A' | 'z' if !inClass => copy(2)
        case 'x' => hexEscape()
        case 'p' | 'P' => property()
        case 'Q' if !inClass => quote()
        case 'b' | 'B' if !inClass =>
          fail("`\\b` treats non-ASCII letters as word characters in Java before JDK 19")
        case 'v' => fail("`\\v` is a vertical whitespace class in Java, the VT character in RE2")
        case _ if isAsciiDigit(c) =>
          fail("`\\<digit>` is a back reference or an octal escape of a different width in Java")
        case _ if c < 0x80 && !Character.isLetterOrDigit(c) =>
          // An escaped ASCII punctuation or space character is a literal in both engines.
          copy(2)
        case _ => fail(s"escape `\\$c` is not known to mean the same in RE2")
      }
    }

    /** A backslash-u escape, or a surrogate pair of them, becomes `\x{XXXX}`. */
    private def unicodeEscape(): Unit = {
      val cp = unicodeEscapeAt(p, i)
      if (cp < 0) fail("malformed `\\u` escape")
      if (Character.isHighSurrogate(cp.toChar)) {
        val lo = unicodeEscapeAt(p, i + 6)
        if (lo < 0 || !Character.isLowSurrogate(lo.toChar)) fail("unpaired surrogate escape")
        val full = Character.toCodePoint(cp.toChar, lo.toChar)
        out.append("\\x{").append(Integer.toHexString(full).toUpperCase(Locale.ROOT)).append("}")
        i += 12
      } else if (Character.isLowSurrogate(cp.toChar)) {
        fail("unpaired surrogate escape")
      } else {
        out.append("\\x{").append(p, i + 2, i + 6).append("}")
        i += 6
      }
    }

    /** `\xhh` or `\x{h...}`. */
    private def hexEscape(): Unit = {
      if (i + 2 < p.length && p.charAt(i + 2) == '{') {
        val end = p.indexOf('}', i + 3)
        if (end < 0 || end == i + 3 || !(i + 3 until end).forall(k => isHex(p.charAt(k)))) {
          fail("malformed `\\x{...}` escape")
        }
        copy(end + 1 - i)
      } else {
        if (i + 4 > p.length || !isHex(p.charAt(i + 2)) || !isHex(p.charAt(i + 3))) {
          fail("malformed `\\xhh` escape")
        }
        copy(4)
      }
    }

    /** `\pX`, `\p{Name}`, `\PX`, `\P{Name}`. */
    private def property(): Unit = {
      if (i + 2 >= p.length) fail("malformed `\\p` escape")
      val (name, length) = if (p.charAt(i + 2) == '{') {
        val end = p.indexOf('}', i + 3)
        if (end < 0) fail("malformed `\\p{...}` escape")
        (p.substring(i + 3, end), end + 1 - i)
      } else {
        (p.substring(i + 2, i + 3), 3)
      }
      if (!GeneralCategories.contains(name)) {
        fail(s"property `$name` is not known to mean the same in RE2")
      }
      copy(length)
    }

    /** `\Q...\E`: copied as is when the quoted text has no backslash. */
    private def quote(): Unit = {
      val end = p.indexOf("\\E", i + 2)
      if (end < 0) fail("`\\Q` without `\\E`")
      if (p.indexOf('\\', i + 2) < end) fail("backslash inside `\\Q...\\E`")
      copy(end + 2 - i)
    }

    /** A character class starting at `i`. */
    private def charClass(): Unit = {
      copy(1)
      if (i < p.length && p.charAt(i) == '^') copy(1)
      // A `]` right after `[` or `[^` is a literal in both engines.
      if (i < p.length && p.charAt(i) == ']') copy(1)
      while (i < p.length && p.charAt(i) != ']') {
        p.charAt(i) match {
          case '[' => fail("nested character classes are unions in Java")
          case '&' if i + 1 < p.length && p.charAt(i + 1) == '&' =>
            fail("`&&` is a class intersection in Java")
          case '\\' => escape(inClass = true)
          case _ => copy(1)
        }
      }
      if (i >= p.length) fail("unterminated character class")
      copy(1)
    }

    /** A group starting at `i`. */
    private def group(): Unit = {
      if (i + 1 >= p.length || p.charAt(i + 1) != '?') {
        openGroup()
        copy(1)
      } else if (p.startsWith("(?:", i)) {
        openGroup()
        copy(3)
      } else if (p.startsWith("(?<", i) && i + 3 < p.length && isAsciiLetter(p.charAt(i + 3))) {
        // Named group `(?<name>...)`: Java names are ASCII letters and digits.
        val end = p.indexOf('>', i + 3)
        if (end < 0 || !(i + 3 until end).forall(k => isAsciiLetterOrDigit(p.charAt(k)))) {
          fail("malformed group name")
        }
        openGroup()
        copy(end + 1 - i)
      } else {
        inlineFlags()
      }
    }

    private def isAsciiLetter(c: Char): Boolean = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')

    private def isAsciiDigit(c: Char): Boolean = c >= '0' && c <= '9'

    private def isAsciiLetterOrDigit(c: Char): Boolean = isAsciiLetter(c) || isAsciiDigit(c)

    private def openGroup(): Unit = {
      enclosingDotAll = dotAll :: enclosingDotAll
    }

    /** `(?flags)` or `(?flags:...)` at `i`. Only the `s` flag is supported. */
    private def inlineFlags(): Unit = {
      var j = i + 2
      var on = true
      var newDotAll = dotAll
      while (j < p.length && p.charAt(j) != ')' && p.charAt(j) != ':') {
        p.charAt(j) match {
          case '-' if on => on = false
          case 's' => newDotAll = on
          case 'i' => fail("`(?i)` is ASCII-only in Java, Unicode-aware in RE2")
          case 'm' => fail("`(?m)` treats `\\r`, U+0085, U+2028 and U+2029 as line ends in Java")
          case 'U' => fail("`(?U)` means Unicode character classes in Java, ungreedy in RE2")
          case '=' | '!' | '<' => fail("lookaround has no RE2 equivalent")
          case c => fail(s"construct `(?$c` is not supported")
        }
        j += 1
      }
      if (j >= p.length) fail("unterminated group")
      if (p.charAt(j) == ':') openGroup()
      dotAll = newDotAll
      copy(j + 1 - i)
    }

    /** `{n}`, `{n,}` or `{n,m}`. Any other `{` is an error in Java and a literal in RE2. */
    private def repetition(): Unit = {
      val end = p.indexOf('}', i)
      val body = if (end < 0) "" else p.substring(i + 1, end)
      val comma = body.indexOf(',')
      val valid = if (comma < 0) {
        body.nonEmpty && body.forall(isAsciiDigit)
      } else {
        comma > 0 && body.forall(c => isAsciiDigit(c) || c == ',') && body.count(_ == ',') == 1
      }
      if (!valid) fail("`{` that does not start a repetition is an error in Java")
      copy(end + 1 - i)
    }
  }
}
