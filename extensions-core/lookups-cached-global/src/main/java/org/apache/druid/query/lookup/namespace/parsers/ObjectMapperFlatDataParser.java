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

package org.apache.druid.query.lookup.namespace.parsers;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.Parser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@JsonTypeName("simpleJson")
public class ObjectMapperFlatDataParser implements FlatDataParser
{
  private final Parser<String, String> parser;

  @JsonCreator
  public ObjectMapperFlatDataParser(
      final @JacksonInject @Json ObjectMapper jsonMapper
  )
  {
    // There's no point canonicalizing field names, we expect them to all be unique.
    final JsonFactory jsonFactory = jsonMapper.getFactory().copy();
    jsonFactory.configure(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES, false);

    parser = new Parser<String, String>()
    {
      @Override
      public Map<String, String> parseToMap(String input)
      {
        try {
          return jsonFactory.createParser(input).readValueAs(JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void setFieldNames(Iterable<String> fieldNames)
      {
        throw new UOE("No field names available");
      }

      @Override
      public List<String> getFieldNames()
      {
        throw new UOE("No field names available");
      }
    };
  }

  @Override
  public Parser<String, String> getParser()
  {
    return parser;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "ObjectMapperFlatDataParser{}";
  }
}
