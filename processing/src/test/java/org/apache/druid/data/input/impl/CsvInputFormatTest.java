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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;

public class CsvInputFormatTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final CsvInputFormat format = new CsvInputFormat(Collections.singletonList("a"), "|", null, true, 10);
    final byte[] bytes = mapper.writeValueAsBytes(format);
    final CsvInputFormat fromJson = (CsvInputFormat) mapper.readValue(bytes, InputFormat.class);
    Assert.assertEquals(format, fromJson);
  }

  @Test
  public void testDeserializeWithoutColumnsWithHasHeaderRow() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final CsvInputFormat inputFormat = (CsvInputFormat) mapper.readValue(
        "{\"type\":\"csv\",\"hasHeaderRow\":true}",
        InputFormat.class
    );
    Assert.assertTrue(inputFormat.isFindColumnsFromHeader());
  }

  @Test
  public void testDeserializeWithoutColumnsWithFindColumnsFromHeaderTrue() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final CsvInputFormat inputFormat = (CsvInputFormat) mapper.readValue(
        "{\"type\":\"csv\",\"findColumnsFromHeader\":true}",
        InputFormat.class
    );
    Assert.assertTrue(inputFormat.isFindColumnsFromHeader());
  }

  @Test
  public void testDeserializeWithoutColumnsWithFindColumnsFromHeaderFalse()
  {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonProcessingException e = Assert.assertThrows(
        JsonProcessingException.class,
        () -> mapper.readValue(
            "{\"type\":\"csv\",\"findColumnsFromHeader\":false}",
            InputFormat.class
        )
    );
    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
            "Cannot construct instance of `org.apache.druid.data.input.impl.CsvInputFormat`, problem: "
            + "If [columns] is not set, the first row of your data must have your header and "
            + "[findColumnsFromHeader] must be set to true."))
    );
  }

  @Test
  public void testDeserializeWithoutColumnsWithBothHeaderProperties()
  {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonProcessingException e = Assert.assertThrows(
        JsonProcessingException.class,
        () -> mapper.readValue(
            "{\"type\":\"csv\",\"findColumnsFromHeader\":true,\"hasHeaderRow\":true}",
            InputFormat.class
        )
    );
    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
            "Cannot construct instance of `org.apache.druid.data.input.impl.CsvInputFormat`, problem: "
            + "Cannot accept both [findColumnsFromHeader] and [hasHeaderRow]"))
    );
  }

  @Test
  public void testDeserializeWithoutAnyProperties()
  {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonProcessingException e = Assert.assertThrows(
        JsonProcessingException.class,
        () -> mapper.readValue("{\"type\":\"csv\"}", InputFormat.class)
    );
    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
            "Cannot construct instance of `org.apache.druid.data.input.impl.CsvInputFormat`, problem: "
            + "Either [columns] or [findColumnsFromHeader] must be set"))
    );
  }

  @Test
  public void testComma()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Column[a,] cannot have the delimiter[,] in its name");
    new CsvInputFormat(Collections.singletonList("a,"), "|", null, false, 0);
  }

  @Test
  public void testDelimiter()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot have same delimiter and list delimiter of [,]");
    new CsvInputFormat(Collections.singletonList("a\t"), ",", null, false, 0);
  }

  @Test
  public void testFindColumnsFromHeaderWithColumnsReturningItsValue()
  {
    final CsvInputFormat format = new CsvInputFormat(Collections.singletonList("a"), null, null, true, 0);
    Assert.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testFindColumnsFromHeaderWithMissingColumnsReturningItsValue()
  {
    final CsvInputFormat format = new CsvInputFormat(null, null, null, true, 0);
    Assert.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithMissingColumnsThrowingError()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Either [columns] or [findColumnsFromHeader] must be set");
    new CsvInputFormat(null, null, null, null, 0);
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithColumnsReturningFalse()
  {
    final CsvInputFormat format = new CsvInputFormat(Collections.singletonList("a"), null, null, null, 0);
    Assert.assertFalse(format.isFindColumnsFromHeader());
  }

  @Test
  public void testHasHeaderRowWithMissingFindColumnsThrowingError()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot accept both [findColumnsFromHeader] and [hasHeaderRow]");
    new CsvInputFormat(null, null, true, false, 0);
  }

  @Test
  public void testHasHeaderRowWithMissingColumnsReturningItsValue()
  {
    final CsvInputFormat format = new CsvInputFormat(null, null, true, null, 0);
    Assert.assertTrue(format.isFindColumnsFromHeader());
  }
}
