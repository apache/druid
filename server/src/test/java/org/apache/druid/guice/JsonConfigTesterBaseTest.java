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
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals("0", testProperties.getProperty("druid.test.prefix.primitiveInt"));
    Assert.assertEquals("false", testProperties.getProperty("druid.test.prefix.primitiveBoolean"));
    Assert.assertTrue(!testProperties.getProperty("druid.test.prefix.text").isEmpty());
    Assert.assertEquals("[]", testProperties.getProperty("druid.test.prefix.list"));
    Assert.assertEquals("[]", testProperties.getProperty("druid.test.prefix.set"));
    Assert.assertEquals("{}", testProperties.getProperty("druid.test.prefix.map"));
    for (Map.Entry entry : System.getProperties().entrySet()) {
      Assert.assertEquals(entry.getValue(), testProperties.get(entry.getKey()));
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
    Sample results = configProvider.get().get();

    Assert.assertEquals(1, results.getPrimitiveInt());
    Assert.assertTrue(results.getPrimitiveBoolean());
    Assert.assertEquals("foo", results.getText());

    List<String> list = results.getList();
    Assert.assertEquals(2, list.size());
    Assert.assertEquals("one", list.get(0));
    Assert.assertEquals("two", list.get(1));

    Set<String> set = results.getSet();
    Assert.assertEquals(2, set.size());
    Assert.assertTrue(set.contains("three"));
    Assert.assertTrue(set.contains("four"));

    Map<String, String> map = results.getMap();
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("v1", map.get("k1"));
    Assert.assertEquals("v2", map.get("k2"));
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
