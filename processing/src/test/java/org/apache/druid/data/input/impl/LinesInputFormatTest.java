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
import org.apache.druid.segment.TestHelper;
import org.apache.druid.utils.CompressionUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class LinesInputFormatTest
{
  private ObjectMapper mapper;

  @Before
  public void setUp() throws Exception
  {
    mapper = TestHelper.makeJsonMapper();
  }

  @Test
  public void testSerde() throws IOException
  {
    final LinesInputFormat expected = new LinesInputFormat();

    final byte[] json = mapper.writeValueAsBytes(expected);

    // Read as map
    final Map<String, Object> map = mapper.readValue(json, Map.class);
    Assert.assertEquals("lines", map.get("type"));

    // Read as InputFormat
    final InputFormat fromJson = mapper.readValue(json, InputFormat.class);
    MatcherAssert.assertThat(fromJson, Matchers.instanceOf(LinesInputFormat.class));
  }

  @Test
  public void test_getWeightedSize_withoutCompression()
  {
    final LinesInputFormat format = new LinesInputFormat();
    final long unweightedSize = 100L;
    Assert.assertEquals(unweightedSize, format.getWeightedSize("file.txt", unweightedSize));
  }

  @Test
  public void test_getWeightedSize_withGzCompression()
  {
    final LinesInputFormat format = new LinesInputFormat();
    final long unweightedSize = 100L;
    Assert.assertEquals(
        unweightedSize * CompressionUtils.COMPRESSED_TEXT_WEIGHT_FACTOR,
        format.getWeightedSize("file.txt.gz", unweightedSize)
    );
  }
}
