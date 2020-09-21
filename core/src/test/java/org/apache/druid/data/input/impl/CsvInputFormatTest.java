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
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
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
    expectedException.expectMessage("At least one of [Property{name='hasHeaderRow', value=null}");
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
    expectedException.expectMessage("At most one of [Property{name='hasHeaderRow', value=true}");
    new CsvInputFormat(null, null, true, false, 0);
  }

  @Test
  public void testHasHeaderRowWithMissingColumnsReturningItsValue()
  {
    final CsvInputFormat format = new CsvInputFormat(null, null, true, null, 0);
    Assert.assertTrue(format.isFindColumnsFromHeader());
  }
}
