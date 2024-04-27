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

package org.apache.druid.common.utils;

import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

public class IdUtilsTest
{
  private static final String THINGO = "thingToValidate";
  public static final String VALID_ID_CHARS = "alpha123..*~!@#&%^&*()-+ Россия\\ 한국 中国!";

  @Test
  public void testValidIdName()
  {
    IdUtils.validateId(THINGO, VALID_ID_CHARS);
  }

  @Test
  public void testInvalidNull()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: must not be null"
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, null));
  }

  @Test
  public void testInvalidEmpty()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: must not be null"
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, ""));
  }

  @Test
  public void testInvalidSlashes()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [/paths/are/bad/since/we/make/files/from/stuff] cannot contain '/'."
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "/paths/are/bad/since/we/make/files/from/stuff"));
  }

  @Test
  public void testInvalidLeadingDot()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [./nice/try] cannot start with '.'."
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "./nice/try"));
  }

  @Test
  public void testInvalidSpacesRegexTabs()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [spaces\tare\tbetter\tthan\ttabs\twhich\tare\tillegal] contains illegal whitespace characters.  Only space is allowed."
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "spaces\tare\tbetter\tthan\ttabs\twhich\tare\tillegal"));
  }

  @Test
  public void testInvalidSpacesRegexNewline()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [new\nline] contains illegal whitespace characters.  Only space is allowed."
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "new\nline"));
  }

  @Test
  public void testInvalidSpacesRegexCarriageReturn()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [does\rexist\rby\ritself] contains illegal whitespace characters.  Only space is allowed."
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "does\rexist\rby\ritself"));
  }

  @Test
  public void testInvalidSpacesRegexLineTabulation()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [what\u000Bis line tabulation] contains illegal whitespace characters.  Only space is allowed."
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "what\u000Bis line tabulation"));
  }

  @Test
  public void testInvalidSpacesRegexFormFeed()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [form\ffeed?] contains illegal whitespace characters.  Only space is allowed."
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "form\u000cfeed?"));
  }

  @Test
  public void testInvalidUnprintableChars()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [form\u0081feed?] contains illegal UTF8 character [#129] at position [4]"
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "form\u0081feed?"));
  }

  @Test
  public void testInvalidEmojis()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [thingToValidate]: Value [form\uD83D\uDCAFfeed?] contains illegal UTF8 character [#55357] at position [4]"
    ).assertThrowsAndMatches(() -> IdUtils.validateId(THINGO, "form💯feed?"));
  }

  @Test
  public void testNewTaskIdWithoutInterval()
  {
    final String id = IdUtils.newTaskId(
        "prefix",
        "suffix",
        DateTimes.of("2020-01-01"),
        "type",
        "datasource",
        null
    );
    final String expected = String.join(
        "_",
        "prefix",
        "type",
        "datasource",
        "suffix",
        DateTimes.of("2020-01-01").toString()
    );
    Assert.assertEquals(expected, id);
  }

  @Test
  public void testNewTaskIdWithInterval()
  {
    final String id = IdUtils.newTaskId(
        "prefix",
        "suffix",
        DateTimes.of("2020-01-01"),
        "type",
        "datasource",
        Intervals.of("2020-01-01/2020-06-01")
    );
    final String expected = String.join(
        "_",
        "prefix",
        "type",
        "datasource",
        "suffix",
        DateTimes.of("2020-01-01").toString(),
        DateTimes.of("2020-06-01").toString(),
        DateTimes.of("2020-01-01").toString()
    );
    Assert.assertEquals(expected, id);
  }
}
