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

package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class JsonConfigTesterBaseTest
    extends JsonConfigTesterBase<JsonConfigTesterBaseTest.Sample>
{
  @Test
  public void defaultTestPropertiesForSample()
  {
    Assertions.assertEquals("0", testProperties.getProperty("druid.test.prefix.primitiveInt"));
    Assertions.assertEquals("false", testProperties.getProperty("druid.test.prefix.primitiveBoolean"));
    Assertions.assertTrue(!testProperties.getProperty("druid.test.prefix.text").isEmpty());
    Assertions.assertEquals("[]", testProperties.getProperty("druid.test.prefix.list"));
    Assertions.assertEquals("[]", testProperties.getProperty("druid.test.prefix.set"));
    Assertions.assertEquals("{}", testProperties.getProperty("druid.test.prefix.map"));
    for (Map.Entry entry : System.getProperties().entrySet()) {
      Assertions.assertEquals(entry.getValue(), testProperties.getProperty(String.valueOf(entry.getKey())));
    }
  }

  @Test
  public void injectFieldValues()
  {
    propertyValues.put(getPropertyKey("primitiveInt"), "1");
    propertyValues.put(getPropertyKey("primitiveBoolean"), "true");
    propertyValues.put(getPropertyKey("text"), "foo");
    propertyValues.put(getPropertyKey("list"), "[\"one\",\"two\"]");
    propertyValues.put(getPropertyKey("set"), "[\"three\",\"four\"]");
    propertyValues.put(getPropertyKey("map"), "{\"k1\": \"v1\", \"k2\": \"v2\"}");
    testProperties.putAll(propertyValues);
    configProvider.inject(testProperties, configurator);
    Sample results = configProvider.get();

    Assertions.assertEquals(1, results.getPrimitiveInt());
    Assertions.assertTrue(results.getPrimitiveBoolean());
    Assertions.assertEquals("foo", results.getText());

    List<String> list = results.getList();
    Assertions.assertEquals(2, list.size());
    Assertions.assertEquals("one", list.get(0));
    Assertions.assertEquals("two", list.get(1));

    Set<String> set = results.getSet();
    Assertions.assertEquals(2, set.size());
    Assertions.assertTrue(set.contains("three"));
    Assertions.assertTrue(set.contains("four"));

    Map<String, String> map = results.getMap();
    Assertions.assertEquals(2, map.size());
    Assertions.assertEquals("v1", map.get("k1"));
    Assertions.assertEquals("v2", map.get("k2"));
  }

  public static class Sample
  {
    @JsonProperty
    private int primitiveInt;

    @JsonProperty
    private boolean primitiveBoolean;

    @JsonProperty
    private String text;

    @JsonProperty
    private List<String> list;

    @JsonProperty
    private Set<String> set;

    @JsonProperty
    private Map<String, String> map;

    public int getPrimitiveInt()
    {
      return primitiveInt;
    }

    public boolean getPrimitiveBoolean()
    {
      return primitiveBoolean;
    }

    public String getText()
    {
      return text;
    }

    public List<String> getList()
    {
      return list;
    }

    public Set<String> getSet()
    {
      return set;
    }

    public Map<String, String> getMap()
    {
      return map;
    }
  }
}
