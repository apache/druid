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

package org.apache.druid.segment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class DefaultColumnFormatsConfigTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testDefaultsSerde() throws JsonProcessingException
  {
    DefaultColumnFormatConfig defaultColumnFormatConfig = new DefaultColumnFormatConfig(null);
    String there = MAPPER.writeValueAsString(defaultColumnFormatConfig);
    DefaultColumnFormatConfig andBack = MAPPER.readValue(there, DefaultColumnFormatConfig.class);
    Assert.assertEquals(defaultColumnFormatConfig, andBack);
    Assert.assertNull(andBack.getNestedColumnFormatVersion());
  }

  @Test
  public void testDefaultsSerdeOverride() throws JsonProcessingException
  {
    DefaultColumnFormatConfig defaultColumnFormatConfig = new DefaultColumnFormatConfig(4);
    String there = MAPPER.writeValueAsString(defaultColumnFormatConfig);
    DefaultColumnFormatConfig andBack = MAPPER.readValue(there, DefaultColumnFormatConfig.class);
    Assert.assertEquals(defaultColumnFormatConfig, andBack);
    Assert.assertEquals(4, (int) andBack.getNestedColumnFormatVersion());
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(DefaultColumnFormatConfig.class).usingGetClass().verify();
  }
}
