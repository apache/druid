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
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class NestedDataColumnSchemaTest
{
  private static final DefaultColumnFormatConfig DEFAULT_CONFIG = new DefaultColumnFormatConfig(null, null);
  private static final ObjectMapper MAPPER;

  static {
    MAPPER = new DefaultObjectMapper();
    MAPPER.setInjectableValues(
        new InjectableValues.Std().addValue(
            DefaultColumnFormatConfig.class,
            DEFAULT_CONFIG
        )
    );
  }

  @Test
  public void testSerdeRoundTrip() throws JsonProcessingException
  {
    final NestedDataColumnSchema v5 = new NestedDataColumnSchema("test", 5);
    Assert.assertEquals(v5, MAPPER.readValue(MAPPER.writeValueAsString(v5), NestedDataColumnSchema.class));
  }

  @Test
  public void testSerdeDefault() throws JsonProcessingException
  {
    final String there = "{\"type\":\"json\", \"name\":\"test\"}";
    NestedDataColumnSchema andBack = MAPPER.readValue(there, NestedDataColumnSchema.class);
    Assert.assertEquals(new NestedDataColumnSchema("test", 5), andBack);
  }
}
