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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    Assertions.assertTrue(config.isEnabled());
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

    Assertions.assertFalse(config.isEnabled());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.simple()
            .forClass(JavaScriptConfig.class)
            .usingGetClass()
            .withNonnullFields("enabled")
            .verify();
  }

  @Test
  public void testToString()
  {
    JavaScriptConfig javaScriptConfig = new JavaScriptConfig(true);
    Assertions.assertEquals("JavaScriptConfig{enabled=true}", javaScriptConfig.toString());
  }

  @Test
  public void testEnabledInstance()
  {
    JavaScriptConfig javaScriptConfig = JavaScriptConfig.getEnabledInstance();
    Assertions.assertTrue(javaScriptConfig.isEnabled());
    Assertions.assertSame(javaScriptConfig, JavaScriptConfig.getEnabledInstance());
  }
}
