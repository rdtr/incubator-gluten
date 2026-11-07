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

import org.scalatest.funsuite.AnyFunSuite

import java.util.regex.Pattern

class Re2PatternUtilsSuite extends AnyFunSuite {

  private def tr(p: String): Option[String] = Re2PatternUtils.translateJavaPatternToRe2(p)

  test("plain pattern is unchanged") {
    assert(tr("[a-z]+") == Some("[a-z]+"))
  }

  test("unicode escape is translated and is idempotent") {
    assert(tr("\\u4e00") == Some("\\x{4e00}"))
    assert(tr("[\\u4e00-\\u9fa5]") == Some("[\\x{4e00}-\\x{9fa5}]"))
    assert(tr("\\x{4e00}") == Some("\\x{4e00}"))
    // Java rejects a backslash-u escape without 4 hex digits.
    assert(tr("\\u41").isEmpty)
  }

  test("escaped backslash before u is not a unicode escape") {
    assert(tr("\\\\u0041") == Some("\\\\u0041"))
    assert(tr("\\\\\\u0041") == Some("\\\\\\x{0041}"))
  }

  test("surrogate pairs are combined into one code point") {
    assert(tr("\\uD83D\\uDE00") == Some("\\x{1F600}"))
    assert(tr("[\\uD83D\\uDE00-\\uD83D\\uDE4F]") == Some("[\\x{1F600}-\\x{1F64F}]"))
  }

  test("unpaired surrogates are unsupported") {
    assert(tr("\\uD83Dx").isEmpty)
    assert(tr("\\uDE00").isEmpty)
    assert(tr("\\uD83D\\u0041").isEmpty)
  }

  test("lookaround and backreferences are unsupported") {
    assert(tr("(?=x)").isEmpty)
    assert(tr("(?!x)").isEmpty)
    assert(tr("(?<=x)y").isEmpty)
    assert(tr("(?<!x)y").isEmpty)
    assert(tr("(a)\\1").isEmpty)
  }

  test("escaped or in-class constructs are not mistaken for unsupported ones") {
    assert(tr("\\\\1").isDefined)
    assert(tr("\\(?=x").isDefined)
    assert(tr("[(?=]").isDefined)
    assert(tr("[^]](?=)").isEmpty) // ']' right after '[^' is literal; the lookahead is real
    assert(tr("[]](?:a)").isDefined)
  }

  test("named groups and non-capturing groups are supported") {
    assert(tr("(?<name>a)") == Some("(?<name>a)"))
    assert(tr("(?:a)b") == Some("(?:a)b"))
  }

  test("any backslash-digit escape is a backreference, even inside a character class") {
    assert(tr("(a)\\1").isEmpty)
    assert(tr("(a)\\12").isEmpty)
    assert(tr("[\\1]").isEmpty)
    // RE2 reads this as an octal escape and would run it, while Java rejects it: must fall back.
    assert(tr("[\\12]").isEmpty)
    assert(tr("\\\\1").isDefined)
  }

  test("describePattern quotes, escapes control characters and truncates") {
    assert(Re2PatternUtils.describePattern("a(?=b)") == "'a(?=b)'")
    assert(Re2PatternUtils.describePattern("a\nb\tc") == "'a\\nb\\tc'")
    val long = Re2PatternUtils.describePattern("x" * 500)
    assert(long.length < 200 && long.contains("truncated, 500 chars total"))
  }

  private val dot = "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]"
  private val space = "\\t\\n\\x0B\\f\\r "

  /** Inputs around every boundary the rewrites change. */
  // scalastyle:off nonascii
  private val inputs = Seq(
    "",
    "abc",
    "a\rb",
    "a\nb",
    "a\r\nb",
    "a\u0085b",
    "a\u2028b",
    "a\u2029b",
    "a\u000Bb",
    "a\tb c\fd",
    "a.b",
    "a\\sb",
    "]-^")
  // scalastyle:on nonascii

  private def assertTranslated(pattern: String, expected: String): Unit = {
    assert(tr(pattern) == Some(expected), s"pattern: $pattern")
    // The output is also a Java pattern with the same meaning as the input.
    inputs.foreach {
      s =>
        assert(
          Pattern.compile(pattern).matcher(s).find() == Pattern.compile(expected).matcher(s).find(),
          s"pattern: $pattern, input: $s")
    }
    assert(tr(expected) == Some(expected), s"translating again changed $expected")
  }

  private def assertUnchanged(pattern: String): Unit =
    assert(tr(pattern) == Some(pattern), s"pattern: $pattern")

  private def assertRejected(pattern: String, reason: String): Unit =
    Re2PatternUtils.convertJavaPatternToRe2(pattern) match {
      case Left(r) => assert(r.contains(reason), s"pattern: $pattern, reason: $r")
      case Right(p) => fail(s"pattern: $pattern was translated to $p")
    }

  test("patterns that mean the same in both engines are unchanged") {
    Seq(
      "",
      "abc",
      "^a+b*c?",
      "a|b",
      "(a)(?:b)(?<name1>c)",
      "a{2}b{2,}c{2,3}",
      "a*?b+?c??d{1,2}?",
      "\\d\\D\\w\\W\\t\\n\\r\\f\\a",
      "\\Aa\\z",
      "\\x41\\x{1F600}",
      "\\p{L}\\pL\\P{Lu}\\p{Nd}",
      "[abc][^a-z][]a][^]a][a-]",
      "[.$^{}()*+?|]",
      "[\\d\\w\\-\\]\\\\]",
      "\\.\\$\\{\\}\\(\\)\\[\\]\\*\\+\\?\\|\\^\\\\",
      "\\Qa.b$c\\E",
      "}]",
      "(?s).",
      "(?s:.)"
    ).foreach(assertUnchanged)
  }

  test("dot without DOTALL becomes a class without Java's line terminators") {
    assertTranslated(".", dot)
    assertTranslated("a.*b", s"a$dot*b")
    assertTranslated("(?-s).", s"(?-s)$dot")
    // DOTALL is scoped to the group that sets it, in both engines.
    assertTranslated("((?s).).", s"((?s).)$dot")
    assertTranslated("(?s:.).", s"(?s:.)$dot")
    assertTranslated("(?s)(?-s:.).", s"(?s)(?-s:$dot).")
    // A dot inside a class, escaped, or quoted is a literal.
    assertUnchanged("[.]")
    assertUnchanged("\\.")
    assertUnchanged("\\Q.\\E")
  }

  test("\\s and \\S become classes with the vertical tab") {
    assertTranslated("\\s", s"[$space]")
    assertTranslated("a\\s+b", s"a[$space]+b")
    assertTranslated("\\S", s"[^$space]")
    assertTranslated("[\\s,]", s"[$space,]")
    assertTranslated("[^\\s]", s"[^$space]")
    assertUnchanged("\\\\s")
    assertRejected("[\\S]", "not known")
  }

  test("constructs that RE2 reads differently are rejected") {
    // scalastyle:off nonascii
    assertRejected("a$", "`$`")
    assertRejected("(?m)^a", "`(?m)`")
    assertRejected("(?i)a", "`(?i)`")
    assertRejected("(?i:a)", "`(?i)`")
    assertRejected("(?si)a", "`(?i)`")
    assertRejected("(?U)\\w", "`(?U)`")
    assertRejected("\\v", "`\\v`")
    assertRejected("[\\v]", "`\\v`")
    assertRejected("\\101", "`\\<digit>`")
    assertRejected("\\0377", "`\\<digit>`")
    assertRejected("a\\b", "`\\b`")
    assertRejected("\\Ba", "`\\b`")
    assertRejected("a{", "`{`")
    assertRejected("{", "`{`")
    assertRejected("x{foo}", "`{`")
    assertRejected("a{,3}", "`{`")
    assertRejected("a{1,2,3}", "`{`")
    assertRejected("a{١}", "`{`")
    assertRejected("\\p{Latin}", "property `Latin`")
    assertRejected("\\p{IsLatin}", "property `IsLatin`")
    assertRejected("\\p{Alpha}", "property `Alpha`")
    assertRejected("\\pX", "property `X`")
    assertRejected("[\\p{Greek}]", "property `Greek`")
    assertRejected("[a&&b]", "`&&`")
    assertRejected("[^]&&x]", "`&&`")
    assertRejected("[a[b]]", "nested")
    // scalastyle:on nonascii
  }

  test("constructs the scanner does not know are rejected") {
    // scalastyle:off nonascii
    Seq(
      "\\h",
      "\\R",
      "\\X",
      "\\Z",
      "\\G",
      "\\e",
      "\\cM",
      "\\N{LATIN SMALL LETTER A}",
      "\\k<a>",
      "\\Qa\\\\E",
      "\\Qa",
      "[a",
      "(a",
      "a)",
      "(?>a)",
      "(?x)a",
      "(?d)a",
      "(?u)a",
      "\\x4",
      "\\x{}",
      "\\é",
      "a\\"
    ).foreach(p => assert(tr(p).isEmpty, s"pattern: $p"))
    // scalastyle:on nonascii
  }

  private val dollar = "(?:\\r\\n|[\\n\\r\\x{85}\\x{2028}\\x{2029}])?\\z"

  private def trMatch(p: String): Either[String, String] =
    Re2PatternUtils.convertJavaPatternToRe2(p, matchOnly = true)

  /** Inputs around the final line terminators that Java's `$` accepts. */
  // scalastyle:off nonascii
  private val dollarInputs = inputs ++ Seq(
    "b",
    "ba",
    "ab\n",
    "ab\r\n",
    "ab\r",
    "ab\u0085",
    "ab\u2028",
    "ab\u2029",
    "ab\n\n",
    "ab\r\n\n",
    "ab\n\r",
    "\r\n",
    "\n")
  // scalastyle:on nonascii

  test("$ is rewritten for match-only callers") {
    Seq(
      "b$" -> s"b$dollar",
      "$" -> dollar,
      "^\\w+$" -> s"^\\w+$dollar",
      "a.?b$" -> s"a$dot?b$dollar",
      "a$|b$" -> s"a$dollar|b$dollar",
      "x|(b$)" -> s"x|(b$dollar)",
      "(b$|a)" -> s"(b$dollar|a)",
      "(b$)+" -> s"(b$dollar)+",
      "(?:b$)?" -> s"(?:b$dollar)?",
      "[^\\r]$" -> s"[^\\r]$dollar",
      "\\P{Cc}$" -> s"\\P{Cc}$dollar",
      "[\\u4e00-\\u9fa5]{2,10}[0-9]+$" -> s"[\\x{4e00}-\\x{9fa5}]{2,10}[0-9]+$dollar"
    ).foreach {
      case (pattern, expected) =>
        assert(trMatch(pattern) == Right(expected), s"pattern: $pattern")
        dollarInputs.foreach {
          s =>
            assert(
              Pattern.compile(pattern).matcher(s).find() ==
                Pattern.compile(expected).matcher(s).find(),
              s"pattern: $pattern, input: $s")
        }
        assert(trMatch(expected) == Right(expected), s"translating again changed $expected")
        assert(tr(pattern).isEmpty, s"pattern: $pattern")
    }
  }

  test("$ is rejected where the rewrite would change the result") {
    // Something after `$` consumes input.
    Seq("b$\\n", "b$.", "(b$|a)c", "(b$)*c", "b$\\z", "b$^").foreach {
      p => assert(trMatch(p).left.exists(_.contains("followed by more pattern")), s"pattern: $p")
    }
    // An element can consume the `\r` of a final `\r\n`.
    Seq(
      "\\r$",
      "\\x0D$",
      "\\x{d}$",
      "\\u000D$",
      "\r$",
      "\\s$",
      "\\W$",
      "\\D$",
      "[^a]$",
      "[\\x00-\\x1F]$",
      "[\\s]$",
      "\\p{Cc}$",
      "\\P{L}$",
      "(?s).$",
      "\\Q\r\\E$"
    ).foreach {
      p => assert(trMatch(p).left.exists(_.contains("can match `\\r`")), s"pattern: $p")
      // Java's `$` does not match between `\r` and `\n`, so the rewrite would be wrong here.
    }
    assert(Pattern.compile("\\r$").matcher("ab\r\n").find() == false)
    assert(Pattern.compile(s"\\r$dollar").matcher("ab\r\n").find())
  }

  test("rewrites combine with unicode escapes") {
    assertTranslated(
      "^(\\d+)\\s*.(?s:.)[\\s.]\\u0041",
      s"^(\\d+)[$space]*$dot(?s:.)[$space.]\\x{0041}")
  }
}
