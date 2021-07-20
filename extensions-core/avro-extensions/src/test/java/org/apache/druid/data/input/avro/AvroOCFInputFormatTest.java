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

package org.apache.druid.data.input.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class AvroOCFInputFormatTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private JSONPathSpec flattenSpec;

  @Before
  public void before()
  {
    flattenSpec = new JSONPathSpec(
        true,
        ImmutableList.of(
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "nested", "someRecord.subLong")
        )
    );
    for (Module jacksonModule : new AvroExtensionsModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, jsonMapper));
  }

  @Test
  public void testSerde() throws Exception
  {
    AvroOCFInputFormat inputFormat = new AvroOCFInputFormat(
        jsonMapper,
        flattenSpec,
        null,
        false,
        false
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );

    Assert.assertEquals(inputFormat, inputFormat2);
  }

  @Test
  public void testSerdeNonDefaults() throws Exception
  {
    String schemaStr = "{\n"
                       + "  \"namespace\": \"org.apache.druid.data.input\",\n"
                       + "  \"name\": \"SomeAvroDatum\",\n"
                       + "  \"type\": \"record\",\n"
                       + "  \"fields\" : [\n"
                       + "    {\"name\":\"timestamp\",\"type\":\"long\"},\n"
                       + "    {\"name\":\"someLong\",\"type\":\"long\"}\n,"
                       + "    {\"name\":\"eventClass\",\"type\":\"string\", \"aliases\": [\"eventType\"]}\n"
                       + "  ]\n"
                       + "}";

    TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>()
    {
    };
    final Map<String, Object> readerSchema = jsonMapper.readValue(schemaStr, typeRef);
    AvroOCFInputFormat inputFormat = new AvroOCFInputFormat(
        jsonMapper,
        flattenSpec,
        readerSchema,
        true,
        true
    );
    NestedInputFormat inputFormat2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        NestedInputFormat.class
    );

    Assert.assertEquals(inputFormat, inputFormat2);
  }
}
