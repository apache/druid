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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by zhxiaog on 15/11/16.
 */
public class BetweenDimFilterTest
{
  private static ObjectMapper mapper;

  @Before
  public void setUp()
  {
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  @Test
  public void testDeserialization_ImplicitNumericallyWithFloatValues() throws IOException
  {
    String expectedInFilter = "{\"type\":\"between\",\"dimension\":\"dimTest\",\"lower\":3.0,\"upper\":10.0}";
    BetweenDimFilter filter = mapper.readValue(expectedInFilter, BetweenDimFilter.class);
    Assert.assertEquals(new BetweenDimFilter("dimTest", 3.0f, 10.0f, true), filter);
  }

  @Test
  public void testDeSerialization_ExplicitNumericallyOnStringValues() throws IOException
  {
    String expectedInFilter = "{\"type\":\"between\",\"dimension\":\"dimTest\",\"lower\":\"3.0\",\"upper\":\"10.0\","
                              + "\"numerically\":true}";
    BetweenDimFilter filter = mapper.readValue(expectedInFilter, BetweenDimFilter.class);
    Assert.assertEquals(new BetweenDimFilter("dimTest", 3.0f, 10.0f, true), filter);
  }

  @Test
  public void testDeSerialization_ExplicitAlphabeticallyOnFloatValues() throws IOException
  {
    String expectedInFilter = "{\"type\":\"between\",\"dimension\":\"dimTest\",\"lower\":100,\"upper\":101,"
                              + "\"numerically\":false}";
    BetweenDimFilter filter = mapper.readValue(expectedInFilter, BetweenDimFilter.class);
    Assert.assertEquals(new BetweenDimFilter("dimTest", "100", "101", false), filter);
  }


  @Test
  public void testSerialization() throws JsonProcessingException
  {
    String expectedStrFilter = "{\"type\":\"between\",\"dimension\":\"dimTest\",\"lower\":3.0,\"upper\":10.0,\"numerically\":true}";
    String actual = mapper.writeValueAsString(new BetweenDimFilter("dimTest", 3.0f, 10.0f, true));
    Assert.assertEquals(expectedStrFilter, actual);
  }
}
