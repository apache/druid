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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.utils.CompressionUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;

public class DelimitedInputFormatTest
{
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final DelimitedInputFormat format = new DelimitedInputFormat(
        Collections.singletonList("a"),
        "|",
        "delim",
        null,
        true,
        10,
        null
    );
    final byte[] bytes = mapper.writeValueAsBytes(format);
    final DelimitedInputFormat fromJson = (DelimitedInputFormat) mapper.readValue(bytes, InputFormat.class);
    Assert.assertEquals(format, fromJson);
  }

  @Test
  public void testTab()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Column[a\t] cannot have the delimiter[\t] in its name");
    new DelimitedInputFormat(Collections.singletonList("a\t"), ",", null, null, false, 0, null);
  }

  @Test
  public void testDelimiterAndListDelimiter()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot have same delimiter and list delimiter of [,]");
    new DelimitedInputFormat(Collections.singletonList("a\t"), ",", ",", null, false, 0, null);
  }

  @Test
  public void testCustomizeSeparator()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Column[a|] cannot have the delimiter[|] in its name");
    new DelimitedInputFormat(Collections.singletonList("a|"), ",", "|", null, false, 0, null);
  }

  @Test
  public void testFindColumnsFromHeaderWithColumnsReturningItsValue()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(
        Collections.singletonList("a"),
        null,
        "delim",
        null,
        true,
        0,
        null
    );
    Assert.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testFindColumnsFromHeaderWithMissingColumnsReturningItsValue()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(
        null,
        null,
        "delim",
        null,
        true,
        0,
        null
    );
    Assert.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testTryParseNumbers()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(
        null,
        null,
        "delim",
        null,
        true,
        0,
        true
    );
    Assert.assertTrue(format.shouldTryParseNumbers());
  }

  @Test
  public void testDeserializeWithTryParseNumbers() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final DelimitedInputFormat inputFormat = (DelimitedInputFormat) mapper.readValue(
        "{\"type\":\"tsv\",\"hasHeaderRow\":true,\"tryParseNumbers\":true}",
        InputFormat.class
    );
    Assert.assertTrue(inputFormat.shouldTryParseNumbers());
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithMissingColumnsThrowingError()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Either [columns] or [findColumnsFromHeader] must be set");
    new DelimitedInputFormat(null, null, "delim", null, null, 0, null);
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithColumnsReturningFalse()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(
        Collections.singletonList("a"),
        null,
        "delim",
        null,
        null,
        0,
        null
    );
    Assert.assertFalse(format.isFindColumnsFromHeader());
  }

  @Test
  public void testHasHeaderRowWithMissingFindColumnsThrowingError()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot accept both [findColumnsFromHeader] and [hasHeaderRow]");
    new DelimitedInputFormat(null, null, "delim", true, false, 0, null);
  }

  @Test
  public void testHasHeaderRowWithMissingColumnsReturningItsValue()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(null, null, "delim", true, null, 0, null);
    Assert.assertTrue(format.isFindColumnsFromHeader());
  }
  @Test
  public void test_getWeightedSize_withoutCompression()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(null, null, "delim", true, null, 0, null);
    final long unweightedSize = 100L;
    Assert.assertEquals(unweightedSize, format.getWeightedSize("file.tsv", unweightedSize));
  }

  @Test
  public void test_getWeightedSize_withGzCompression()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(null, null, "delim", true, null, 0, null);
    final long unweightedSize = 100L;
    Assert.assertEquals(
        unweightedSize * CompressionUtils.COMPRESSED_TEXT_WEIGHT_FACTOR,
        format.getWeightedSize("file.tsv.gz", unweightedSize)
    );
  }
}
