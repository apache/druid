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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;

public class TsvInputFormatTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final TsvInputFormat format = new TsvInputFormat(Collections.singletonList("a"), "|", null, null, true, 10);
    final byte[] bytes = mapper.writeValueAsBytes(format);
    final TsvInputFormat fromJson = (TsvInputFormat) mapper.readValue(bytes, InputFormat.class);
    Assert.assertEquals(format, fromJson);
  }

  @Test
  public void testTab()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Column[a\t] has a tab, it cannot");
    new TsvInputFormat(Collections.singletonList("a\t"), ",", null, null, false, 0);
  }

  @Test
  public void testCustomizeSeparator()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Column[a|] has a customize separator: |, it cannot");
    new TsvInputFormat(Collections.singletonList("a|"), ",", "|", null, false, 0);
  }
}
