/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class DimsConcatExtractionFnTest
{
  @Test
  public void testFormat()
  {
    DimsConcatExtractionFn fn = new DimsConcatExtractionFn("%s->%s", null);
    List<String> testDimValues = ImmutableList.of("abc", "def");
    Assert.assertEquals("abc->def", fn.apply(testDimValues));
  }

  @Test
  public void testDelimiter()
  {
    DimsConcatExtractionFn fn = new DimsConcatExtractionFn(null, ":");
    List<String> testDimValues = ImmutableList.of("abc", "def");
    Assert.assertEquals("abc:def", fn.apply(testDimValues));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    String fnStr = "{\"type\":\"dimsConcat\", \"format\":\"%s->%s\"}";
    ExtractionFn fn = mapper.readValue(fnStr, ExtractionFn.class);

    Assert.assertTrue(fn instanceof DimsConcatExtractionFn);

    DimsConcatExtractionFn dimsConcatExtractionFn = (DimsConcatExtractionFn)fn;
    Assert.assertEquals("%s->%s", dimsConcatExtractionFn.getFormat());

    List<String> testDimValues = ImmutableList.of("abc", "def");
    Assert.assertEquals("abc->def", fn.apply(testDimValues));
  }
}
