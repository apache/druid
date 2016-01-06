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

package io.druid.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class JavaScriptDimFilterSerDeTest
{
  private static ObjectMapper mapper;

  private static final String JSFilter =
      "{\"type\":\"javascript\",\"extractionFn\":null,\"function\":\"function(d1, d2) { return d1 === d2 }\",\"dimensions\":[\"dimTest1\",\"dimTest2\"]}";
  private static final String JSFilterBackwardCompatible =
      "{\"type\":\"javascript\",\"dimension\":\"dimTest\",\"function\":\"function(d1) { return true }\",\"byRow\":false}";
  private static final String JSFilterRewritten =
      "{\"type\":\"javascript\",\"extractionFn\":null,\"function\":\"function(d1) { return true }\",\"dimensions\":[\"dimTest\"]}";

  private static final String JSFilterbyRow =
      "{\"type\":\"javascript\",\"extractionFn\":null,\"function\":\"function(d1, d2) { return d1 === d2 }\",\"dimensions\":[\"dimTest1\",\"dimTest2\"]}";

  @Before
  public void setUp()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testDeserialization1() throws IOException
  {
    final JavaScriptDimFilter actualJSDimFilter = mapper.reader(DimFilter.class).readValue(JSFilterBackwardCompatible);
    final JavaScriptDimFilter expectedJSDimFilter =
        JavaScriptDimFilter.of("dimTest", "function(d1) { return true }", null);
    Assert.assertEquals(expectedJSDimFilter, actualJSDimFilter);
  }

  @Test
  public void testDeserialization2() throws IOException
  {
    final JavaScriptDimFilter actualJSDimFilter = mapper.reader(DimFilter.class).readValue(JSFilter);
    final JavaScriptDimFilter expectedJSDimFilter =
        JavaScriptDimFilter.of(new String[]{"dimTest1", "dimTest2"}, "function(d1, d2) { return d1 === d2 }", null);
    Assert.assertEquals(expectedJSDimFilter, actualJSDimFilter);
  }

  @Test
  public void testSerialization1() throws IOException
  {
    final JavaScriptDimFilter JSDimFilter =
        JavaScriptDimFilter.of(new String[]{"dimTest1", "dimTest2"}, "function(d1, d2) { return d1 === d2 }", null);
    Assert.assertEquals(JSFilter, mapper.writeValueAsString(JSDimFilter));
  }

  @Test
  public void testSerialization2() throws IOException
  {
    final JavaScriptDimFilter JSDimFilter = JavaScriptDimFilter.of("dimTest", "function(d1) { return true }", null);
    Assert.assertEquals(JSFilterRewritten, mapper.writeValueAsString(JSDimFilter));
  }

  @Test
  public void testSerialization3() throws IOException
  {
    final JavaScriptDimFilter JSDimFilter = JavaScriptDimFilter.byRow(
        new String[]{"dimTest1", "dimTest2"},
        "function(d1, d2) { return d1 === d2 }",
        null
    );
    Assert.assertEquals(JSFilterbyRow, mapper.writeValueAsString(JSDimFilter));
  }
}
