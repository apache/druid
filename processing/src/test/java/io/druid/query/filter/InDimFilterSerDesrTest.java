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
import io.druid.query.extraction.RegexDimExtractionFn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class InDimFilterSerDesrTest
{
  private static ObjectMapper mapper;

  private final String actualInFilter = "{\"type\":\"in\",\"dimension\":\"dimTest\",\"values\":[\"bad\",\"good\"],\"extractionFn\":null}";
  @Before
  public void setUp()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testDeserialization() throws IOException
  {
    final InDimFilter actualInDimFilter = mapper.reader(DimFilter.class).readValue(actualInFilter);
    final InDimFilter expectedInDimFilter = new InDimFilter("dimTest", Arrays.asList("good", "bad"), null);
    Assert.assertEquals(expectedInDimFilter, actualInDimFilter);
  }

  @Test
  public void testSerialization() throws IOException
  {
    final InDimFilter dimInFilter = new InDimFilter("dimTest", Arrays.asList("good", "bad"), null);
    final String expectedInFilter = mapper.writeValueAsString(dimInFilter);
    Assert.assertEquals(expectedInFilter, actualInFilter);
  }

  @Test
  public void testGetCacheKey()
  {
    final InDimFilter inDimFilter_1 = new InDimFilter("dimTest", Arrays.asList("good", "bad"), null);
    final InDimFilter inDimFilter_2 = new InDimFilter("dimTest", Arrays.asList("good,bad"), null);
    Assert.assertNotEquals(inDimFilter_1.getCacheKey(), inDimFilter_2.getCacheKey());

    RegexDimExtractionFn regexFn = new RegexDimExtractionFn(".*", false, null);
    final InDimFilter inDimFilter_3 = new InDimFilter("dimTest", Arrays.asList("good", "bad"), regexFn);
    final InDimFilter inDimFilter_4 = new InDimFilter("dimTest", Arrays.asList("good,bad"), regexFn);
    Assert.assertNotEquals(inDimFilter_3.getCacheKey(), inDimFilter_4.getCacheKey());
  }

  @Test
  public void testGetCacheKeyNullValue() throws IOException
  {
    InDimFilter inDimFilter = mapper.readValue("{\"type\":\"in\",\"dimension\":\"dimTest\",\"values\":[null]}", InDimFilter.class);
    Assert.assertNotNull(inDimFilter.getCacheKey());
  }
}
