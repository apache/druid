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

package org.apache.druid;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;

import java.io.IOException;

/**
 */
public class TestObjectMapper extends ObjectMapper
{
  public TestObjectMapper()
  {
    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    configure(SerializationFeature.INDENT_OUTPUT, false);
    registerModule(new TestModule());
  }

  public static class TestModule extends SimpleModule
  {
    TestModule()
    {
      addSerializer(Interval.class, ToStringSerializer.instance);
      addDeserializer(
          Interval.class,
          new StdDeserializer<Interval>(Interval.class)
          {
            @Override
            public Interval deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
            {
              return Intervals.of(jsonParser.getText());
            }
          }
      );
    }
  }
}
