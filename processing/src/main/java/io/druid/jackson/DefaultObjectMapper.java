/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

/**
 */
public class DefaultObjectMapper extends ObjectMapper
{
  public DefaultObjectMapper()
  {
    this((JsonFactory)null);
  }

  public DefaultObjectMapper(DefaultObjectMapper mapper)
  {
    super(mapper);
  }

  public DefaultObjectMapper(JsonFactory factory)
  {
    super(factory);
    registerModule(new DruidDefaultSerializersModule());
    registerModule(new GuavaModule());
    registerModule(new QueryGranularityModule());
    registerModule(new AggregatorsModule());
    registerModule(new SegmentsModule());

    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    configure(MapperFeature.AUTO_DETECT_GETTERS, false);
//    configure(MapperFeature.AUTO_DETECT_CREATORS, false); https://github.com/FasterXML/jackson-databind/issues/170
    configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    configure(SerializationFeature.INDENT_OUTPUT, false);
  }

  @Override
  public ObjectMapper copy()
  {
    return new DefaultObjectMapper(this);
  }
}
