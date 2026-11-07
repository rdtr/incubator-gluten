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
   * fallback. Non-literal patterns are returned unchanged. See [[convertJavaPatternToRe2]] for
   * `matchOnly`.
   */
  def rewriteRegexPattern(pattern: Expression, matchOnly: Boolean): Expression = pattern match {
    case Literal(p: UTF8String, StringType) =>
      convertJavaPatternToRe2(p.toString, matchOnly) match {
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
   *   - With `matchOnly`, `$` becomes `(?:\r\n|[\n\r\x{85}\x{2028}\x{2029}])?\z`. Java's `$` also
   *     matches before a final line terminator; consuming that terminator instead gives the same
   *     answer to whether there is a match, but not the same match position. The rewrite is used
   *     only when nothing after the `$` consumes input and no element of the pattern can consume
   *     `\r`, since Java's `$` does not match between `\r` and `\n`.
   *
   * Rejected constructs:
   *   - `$` without `matchOnly`, or where the rewrite above does not apply.
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
  def convertJavaPatternToRe2(
      pattern: String,
      matchOnly: Boolean = false): Either[String, String] =
    new Re2Converter(pattern, matchOnly).run()

  /** Java's `.` without DOTALL: anything except the line terminators. */
  private val Re2Dot = "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]"

  /**
   * Java's `$` without MULTILINE for a caller that only asks whether there is a match: an optional
   * final line terminator, then the end of the input.
   */
  private val Re2Dollar = "(?:\\r\\n|[\\n\\r\\x{85}\\x{2028}\\x{2029}])?\\z"

  /** Java's `\s` without UNICODE_CHARACTER_CLASS: `[ \t\n\x0B\f\r]` (RE2 lacks `\x0B`). */
  private val SpaceChars = "\\t\\n\\x0B\\f\\r "

  /** Unicode general categories that java.util.regex and RE2 both accept with the same meaning. */
  private val GeneralCategories =
    ("L Lu Ll Lt Lm Lo M Mn Mc Me N Nd Nl No P Pc Pd Ps Pe Pi Pf Po S Sm Sc Sk So Z Zs Zl Zp " +
      "C Cc Cf Co Cs").split(' ').toSet

  /** The general categories that contain `\r`. */
  private val CrCategories = Set("C", "Cc")

  /** Single-character escapes that mean the same in both engines, with their code points. */
  private val CharEscapes = Map('t' -> 0x09, 'n' -> 0x0a, 'r' -> 0x0d, 'f' -> 0x0c, 'a' -> 0x07)

  final private class Unsupported(val reason: String) extends Exception(reason)

  /**
   * What an escape or a class member matches: one code point, or a set of characters (`codePoint`
   * is -1), and whether that can be `\r`.
   */
  final private case class Atom(codePoint: Int, matchesCr: Boolean)

  private def charAtom(cp: Int): Atom = Atom(cp, cp == '\r')

  final private class Re2Converter(p: String, matchOnly: Boolean) {
    private val out = new java.lang.StringBuilder(p.length)
    private var i = 0
    // DOTALL state of the current group and of the enclosing groups. Java and RE2 both restore
    // inline flags at the end of a group.
    private var dotAll = false
    private var enclosingDotAll: List[Boolean] = Nil
    // For the `$` rewrite: one token per pattern element ('c' consumes input, 'a' is another
    // zero-width assertion, '$', '(', ')', '|', and 'q' for a quantifier), and whether any
    // element can consume `\r`.
    private val tokens = new StringBuilder
    private var canMatchCr = false

    private def fail(reason: String): Nothing = throw new Unsupported(reason)

    def run(): Either[String, String] = {
      try {
        while (i < p.length) {
          p.charAt(i) match {
            case '\\' =>
              val zeroWidth = i + 1 < p.length && (p.charAt(i + 1) == 'A' || p.charAt(i + 1) == 'z')
              val atom = escape(inClass = false)
              tokens.append(if (zeroWidth) 'a' else 'c')
              canMatchCr |= atom.matchesCr
            case '[' =>
              canMatchCr |= charClass()
              tokens.append('c')
            case '.' =>
              out.append(if (dotAll) "." else Re2Dot)
              canMatchCr |= dotAll
              tokens.append('c')
              i += 1
            case '$' =>
              if (!matchOnly) fail("`$` also matches before a final line terminator in Java")
              out.append(Re2Dollar)
              tokens.append('$')
              i += 1
            case '(' => group()
            case ')' =>
              if (enclosingDotAll.isEmpty) fail("unbalanced `)`")
              dotAll = enclosingDotAll.head
              enclosingDotAll = enclosingDotAll.tail
              tokens.append(')')
              copy(1)
            case '{' =>
              repetition()
              tokens.append('q')
            case '*' | '+' | '?' =>
              tokens.append('q')
              copy(1)
            case '|' =>
              tokens.append('|')
              copy(1)
            case '^' =>
              tokens.append('a')
              copy(1)
            case _ =>
              val cp = p.codePointAt(i)
              canMatchCr |= cp == '\r'
              tokens.append('c')
              copy(Character.charCount(cp))
          }
        }
        if (enclosingDotAll.nonEmpty) fail("unbalanced `(`")
        checkDollars()
        Right(out.toString)
      } catch {
        case e: Unsupported => Left(e.reason)
      }
    }

    /**
     * The rewritten `$` consumes the final line terminator, so it is only correct when nothing
     * after it consumes input. Java's `$` also does not match between `\r` and `\n`, which the
     * rewrite would, so no element of the pattern may consume `\r`.
     */
    private def checkDollars(): Unit = {
      val n = tokens.length
      for (k <- 0 until n if tokens.charAt(k) == '$') {
        var j = k + 1
        while (j < n) {
          tokens.charAt(j) match {
            case ')' | 'q' | '$' => j += 1
            case '|' =>
              // Skip the other alternatives of the current group.
              var depth = 0
              j += 1
              while (j < n && !(tokens.charAt(j) == ')' && depth == 0)) {
                if (tokens.charAt(j) == '(') depth += 1
                if (tokens.charAt(j) == ')') depth -= 1
                j += 1
              }
            case _ => fail("`$` that is followed by more pattern is not supported")
          }
        }
      }
      if (tokens.indexOf("$") >= 0 && canMatchCr) {
        fail("`$` in a pattern that can match `\\r` is not supported")
      }
    }

    private def copy(n: Int): Unit = {
      out.append(p, i, i + n)
      i += n
    }

    /** Handles the escape at `i`, which is a backslash. */
    private def escape(inClass: Boolean): Atom = {
      if (i + 1 >= p.length) fail("trailing `\\`")
      val c = p.charAt(i + 1)
      c match {
        case 'u' => charAtom(unicodeEscape())
        case 's' =>
          out.append(if (inClass) SpaceChars else s"[$SpaceChars]")
          i += 2
          Atom(-1, matchesCr = true)
        case 'S' if !inClass =>
          out.append(s"[^$SpaceChars]")
          i += 2
          Atom(-1, matchesCr = false)
        case 'd' | 'w' =>
          copy(2)
          Atom(-1, matchesCr = false)
        case 'D' | 'W' =>
          copy(2)
          Atom(-1, matchesCr = true)
        case _ if CharEscapes.contains(c) =>
          copy(2)
          charAtom(CharEscapes(c))
        case 'A' | 'z' if !inClass =>
          copy(2)
          Atom(-1, matchesCr = false)
        case 'x' => charAtom(hexEscape())
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
          charAtom(c)
        case _ => fail(s"escape `\\$c` is not known to mean the same in RE2")
      }
    }

    /** A backslash-u escape, or a surrogate pair of them, becomes `\x{XXXX}`. Returns its value. */
    private def unicodeEscape(): Int = {
      val cp = unicodeEscapeAt(p, i)
      if (cp < 0) fail("malformed `\\u` escape")
      if (Character.isHighSurrogate(cp.toChar)) {
        val lo = unicodeEscapeAt(p, i + 6)
        if (lo < 0 || !Character.isLowSurrogate(lo.toChar)) fail("unpaired surrogate escape")
        val full = Character.toCodePoint(cp.toChar, lo.toChar)
        out.append("\\x{").append(Integer.toHexString(full).toUpperCase(Locale.ROOT)).append("}")
        i += 12
        full
      } else if (Character.isLowSurrogate(cp.toChar)) {
        fail("unpaired surrogate escape")
      } else {
        out.append("\\x{").append(p, i + 2, i + 6).append("}")
        i += 6
        cp
      }
    }

    /** `\xhh` or `\x{h...}`. Returns the code point. */
    private def hexEscape(): Int = {
      val (digits, length) = if (i + 2 < p.length && p.charAt(i + 2) == '{') {
        val end = p.indexOf('}', i + 3)
        if (end < 0) fail("malformed `\\x{...}` escape")
        (p.substring(i + 3, end), end + 1 - i)
      } else {
        (p.substring(i + 2, math.min(i + 4, p.length)), 4)
      }
      if (
        digits.isEmpty || digits.length > 6 || (length == 4 && digits.length != 2) ||
        !digits.forall(isHex)
      ) {
        fail("malformed `\\x` escape")
      }
      val cp = Integer.parseInt(digits, 16)
      if (cp > Character.MAX_CODE_POINT) fail("`\\x` escape out of range")
      copy(length)
      cp
    }

    /** `\pX`, `\p{Name}`, `\PX`, `\P{Name}`. */
    private def property(): Atom = {
      if (i + 2 >= p.length) fail("malformed `\\p` escape")
      val negated = p.charAt(i + 1) == 'P'
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
      Atom(-1, matchesCr = CrCategories.contains(name) != negated)
    }

    /** `\Q...\E`: copied as is when the quoted text has no backslash. */
    private def quote(): Atom = {
      val end = p.indexOf("\\E", i + 2)
      if (end < 0) fail("`\\Q` without `\\E`")
      if (p.indexOf('\\', i + 2) < end) fail("backslash inside `\\Q...\\E`")
      val matchesCr = p.substring(i + 2, end).contains('\r')
      copy(end + 2 - i)
      Atom(-1, matchesCr)
    }

    /** One member of a character class at `i`. */
    private def classMember(): Atom = p.charAt(i) match {
      case '[' => fail("nested character classes are unions in Java")
      case '&' if i + 1 < p.length && p.charAt(i + 1) == '&' =>
        fail("`&&` is a class intersection in Java")
      case '\\' => escape(inClass = true)
      case _ =>
        val cp = p.codePointAt(i)
        copy(Character.charCount(cp))
        charAtom(cp)
    }

    /** A character class starting at `i`. Returns whether it can match `\r`. */
    private def charClass(): Boolean = {
      copy(1)
      val negated = i < p.length && p.charAt(i) == '^'
      if (negated) copy(1)
      var containsCr = false
      // A `]` right after `[` or `[^` is a literal in both engines.
      if (i < p.length && p.charAt(i) == ']') copy(1)
      while (i < p.length && p.charAt(i) != ']') {
        val lo = classMember()
        if (i + 1 < p.length && p.charAt(i) == '-' && p.charAt(i + 1) != ']') {
          copy(1)
          val hi = classMember()
          if (lo.codePoint < 0 || hi.codePoint < 0) fail("range with a class escape")
          containsCr |= lo.codePoint <= '\r' && '\r' <= hi.codePoint
        } else {
          containsCr |= lo.matchesCr
        }
      }
      if (i >= p.length) fail("unterminated character class")
      copy(1)
      containsCr != negated
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
      tokens.append('(')
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
