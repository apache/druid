/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Focused unit coverage for the shared {@link Globs} helpers. Integration semantics (e.g., how patterns interact with
 * excludes inside a matcher) live with the matcher tests; this file pins the translation rules so future refactors
 * don't drift them silently.
 */
class GlobsTest
{
  @Test
  void testLiteralGlobMatchesItself()
  {
    Assertions.assertEquals("user_daily", Globs.globToRegex("user_daily"));
  }

  @Test
  void testStarTranslatesToDotStar()
  {
    Assertions.assertEquals("user_.*", Globs.globToRegex("user_*"));
  }

  @Test
  void testQuestionMarkTranslatesToDot()
  {
    Assertions.assertEquals("user_.", Globs.globToRegex("user_?"));
  }

  @Test
  void testRegexMetacharactersInLiteralPositionsAreEscaped()
  {
    // Each of these has special regex meaning; the translator must escape them so they match literally.
    Assertions.assertEquals("user\\.daily", Globs.globToRegex("user.daily"));
    Assertions.assertEquals("a\\+b", Globs.globToRegex("a+b"));
    Assertions.assertEquals("\\(foo\\)", Globs.globToRegex("(foo)"));
    Assertions.assertEquals("a\\|b", Globs.globToRegex("a|b"));
    Assertions.assertEquals("\\^start", Globs.globToRegex("^start"));
    Assertions.assertEquals("end\\$", Globs.globToRegex("end$"));
  }

  @Test
  void testBackslashEscapesGlobMetacharacters()
  {
    // Escaped glob metacharacters become literal regex characters (themselves escaped).
    Assertions.assertEquals("a\\*b", Globs.globToRegex("a\\*b"));
    Assertions.assertEquals("a\\?b", Globs.globToRegex("a\\?b"));
    Assertions.assertEquals("a\\\\b", Globs.globToRegex("a\\\\b"));
  }

  @Test
  void testTrailingUnescapedBackslashIsRejected()
  {
    Assertions.assertThrows(DruidException.class, () -> Globs.globToRegex("foo\\"));
  }

  @Test
  void testCompileAllReturnsEmptyListForEmptyInput()
  {
    Assertions.assertTrue(Globs.compileAll(List.of()).isEmpty());
  }

  @Test
  void testCompileAllCompilesEachPattern()
  {
    final List<Pattern> compiled = Globs.compileAll(List.of("user_*", "report_?"));
    Assertions.assertEquals(2, compiled.size());
    Assertions.assertTrue(compiled.get(0).matcher("user_daily").matches());
    Assertions.assertTrue(compiled.get(1).matcher("report_x").matches());
    Assertions.assertFalse(compiled.get(1).matcher("report_xy").matches());
  }

  @Test
  void testMatchesAnyReturnsTrueOnFirstHit()
  {
    final List<Pattern> compiled = Globs.compileAll(List.of("user_*", "report_*"));
    Assertions.assertTrue(Globs.matchesAny("user_daily", compiled));
    Assertions.assertTrue(Globs.matchesAny("report_hourly", compiled));
    Assertions.assertFalse(Globs.matchesAny("other", compiled));
  }

  @Test
  void testMatchesAnyEmptyPatternListNeverMatches()
  {
    Assertions.assertFalse(Globs.matchesAny("anything", List.of()));
  }

  @Test
  void testCompileLiteralStarReturnsMatchAny()
  {
    // The literal "*" is special-cased to MATCH_ANY so callers can short-circuit a "matches any value, including
    // null" branch without paying a regex match. Any other glob takes the regex path.
    final Globs.CompiledGlob compiled = Globs.compile("*");
    Assertions.assertSame(Globs.CompiledGlob.MATCH_ANY, compiled);
    Assertions.assertTrue(compiled.matchAny);
    Assertions.assertNull(compiled.pattern);
  }

  @Test
  void testCompileGlobReturnsCompiledRegex()
  {
    final Globs.CompiledGlob compiled = Globs.compile("us-*");
    Assertions.assertFalse(compiled.matchAny);
    Assertions.assertNotNull(compiled.pattern);
    Assertions.assertTrue(compiled.matches("us-east-1"));
    Assertions.assertFalse(compiled.matches("eu-west"));
  }

  @Test
  void testCompileLiteralNullStringHasNoSpecialTreatment()
  {
    // The string "null" goes through the regex path like any other literal glob, the helper does not give the
    // literal-string "null" any special meaning
    final Globs.CompiledGlob compiled = Globs.compile("null");
    Assertions.assertFalse(compiled.matchAny);
    Assertions.assertNotNull(compiled.pattern);
    Assertions.assertTrue(compiled.matches("null"));
    Assertions.assertFalse(compiled.matches("nullx"));
  }
}
