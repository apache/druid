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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ShardSpecTestUtils
{
  public static ObjectMapper initObjectMapper()
  {
    // Copied configurations from org.apache.druid.jackson.DefaultObjectMapper
    final ObjectMapper mapper = new ObjectMapper();
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    // See https://github.com/FasterXML/jackson-databind/issues/170
    // configure(MapperFeature.AUTO_DETECT_CREATORS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    mapper.configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
    mapper.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, false);
    return mapper;
  }

  private ShardSpecTestUtils()
  {
  }
}
