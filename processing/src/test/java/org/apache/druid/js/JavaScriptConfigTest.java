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

package org.apache.druid.js;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class JavaScriptConfigTest
{
  private static ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    String json = "{\"enabled\":true}";

    JavaScriptConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                json,
                JavaScriptConfig.class
            )
        ), JavaScriptConfig.class
    );

    Assert.assertTrue(config.isEnabled());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String json = "{}";

    JavaScriptConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                json,
                JavaScriptConfig.class
            )
        ), JavaScriptConfig.class
    );

    Assert.assertFalse(config.isEnabled());
  }

  @Test
  public void testEquals()
  {
    JavaScriptConfig config1 = new JavaScriptConfig(true);
    JavaScriptConfig config2 = new JavaScriptConfig(true);
    JavaScriptConfig config3 = new JavaScriptConfig(false);

    Assert.assertEquals(config1, config2);
    Assert.assertNotEquals(config1, config3);
    Assert.assertNotEquals(config2, config3);
    Assert.assertNotEquals(config1, null);
    Assert.assertNotEquals(config1, new Object());

    Assert.assertTrue(config1.equals(config1));
  }

  @Test
  public void testHashCode()
  {
    JavaScriptConfig config1 = new JavaScriptConfig(true);
    JavaScriptConfig config2 = new JavaScriptConfig(true);
    JavaScriptConfig config3 = new JavaScriptConfig(false);

    Assert.assertEquals(config1.hashCode(), config2.hashCode());
    Assert.assertNotEquals(config1.hashCode(), config3.hashCode());
  }

  @Test
  public void testToString() {
    JavaScriptConfig javaScriptConfig = new JavaScriptConfig(true);
    Assert.assertEquals("JavaScriptConfig{enabled=true}", javaScriptConfig.toString());
  }

  @Test
  public void testEnabledInstance()
  {
    JavaScriptConfig javaScriptConfig = JavaScriptConfig.getEnabledInstance();
    Assert.assertTrue(javaScriptConfig.isEnabled());
    Assert.assertSame(javaScriptConfig, JavaScriptConfig.getEnabledInstance());
  }
}
