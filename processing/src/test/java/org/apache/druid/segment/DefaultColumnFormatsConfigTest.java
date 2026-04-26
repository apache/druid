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
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DefaultColumnFormatsConfigTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testDefaultsSerde() throws JsonProcessingException
  {
    DefaultColumnFormatConfig defaultColumnFormatConfig = new DefaultColumnFormatConfig(null, null, null, null);
    String there = MAPPER.writeValueAsString(defaultColumnFormatConfig);
    DefaultColumnFormatConfig andBack = MAPPER.readValue(there, DefaultColumnFormatConfig.class);
    Assertions.assertEquals(defaultColumnFormatConfig, andBack);
    Assertions.assertNull(andBack.getNestedColumnFormatVersion());
    Assertions.assertNull(andBack.getStringMultiValueHandlingMode());
  }

  @Test
  public void testDefaultsSerdeOverride() throws JsonProcessingException
  {
    DefaultColumnFormatConfig defaultColumnFormatConfig = new DefaultColumnFormatConfig("ARRAY", 5, null, null);
    String there = MAPPER.writeValueAsString(defaultColumnFormatConfig);
    DefaultColumnFormatConfig andBack = MAPPER.readValue(there, DefaultColumnFormatConfig.class);
    Assertions.assertEquals(defaultColumnFormatConfig, andBack);
    Assertions.assertEquals(5, (int) andBack.getNestedColumnFormatVersion());
    Assertions.assertEquals(DimensionSchema.MultiValueHandling.ARRAY.toString(), andBack.getStringMultiValueHandlingMode());
    Assertions.assertNull(andBack.getMaxStringLength());
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(DefaultColumnFormatConfig.class)
                  .usingGetClass()
                  .verify();
  }
}
