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

package org.apache.druid.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;


class DruidTimestampParserTest
{
  /**
   * A format without a zone offset is UTC; a bare date is midnight UTC, and a bare time is on the epoch day.
   */
  @ParameterizedTest(name = "{0} -> {1}")
  @CsvSource({
      // Explicit offsets.
      "2025-01-15T12:30:45.000Z,  2025-01-15T12:30:45.000Z",
      "2025-01-15T12:30:45.123Z,  2025-01-15T12:30:45.123Z",
      "2025-01-15T12:30:45Z,      2025-01-15T12:30:45Z",
      "2025-01-15T12:30:45+00:00, 2025-01-15T12:30:45Z",
      "2025-01-15T12:30:45+05:30, 2025-01-15T07:00:45Z",
      "2025-01-15T12:30:45-04:00, 2025-01-15T16:30:45Z",
      // Zoneless, with a T separator.
      "2025-01-15T12:30:45,       2025-01-15T12:30:45Z",
      "2025-06-01T10:00:00,       2025-06-01T10:00:00Z",
      // Zoneless, with a space separator.
      "2025-06-01 10:30:45.123,   2025-06-01T10:30:45.123Z",
      "2025-06-01 10:30:45,       2025-06-01T10:30:45Z",
      // Bare date and bare time.
      "2025-01-15,                2025-01-15T00:00:00Z",
      "14:30:15,                  1970-01-01T14:30:15Z"
  })
  void testParse(final String input, final String expectedInstant) throws ParseException
  {
    Assertions.assertEquals(Timestamp.from(Instant.parse(expectedInstant)), DruidTimestampParser.parse(input));
  }

  @Test
  void testEpochMillis() throws ParseException
  {
    Assertions.assertEquals(new Timestamp(1715000000000L), DruidTimestampParser.parse("1715000000000"));
    Assertions.assertEquals(new Timestamp(0), DruidTimestampParser.parse("0"));
  }

  @Test
  void testNullOrBlankInputThrowsIllegalArgumentException()
  {
    Assertions.assertThrows(IllegalArgumentException.class, () -> DruidTimestampParser.parse(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> DruidTimestampParser.parse(""));
    Assertions.assertThrows(IllegalArgumentException.class, () -> DruidTimestampParser.parse("   "));
  }

  @Test
  void testUnparseableInputThrowsParseException()
  {
    Assertions.assertThrows(ParseException.class, () -> DruidTimestampParser.parse("not-a-timestamp"));
    Assertions.assertThrows(ParseException.class, () -> DruidTimestampParser.parse("abc123xyz"));
  }
}
