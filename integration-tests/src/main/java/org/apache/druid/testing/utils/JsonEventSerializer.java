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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Pair;

import java.util.List;
import java.util.Map;

public class JsonEventSerializer implements EventSerializer
{
  public static final String TYPE = "json";

  private final ObjectMapper jsonMapper;

  @JsonCreator
  public JsonEventSerializer(@JacksonInject @Json ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public byte[] serialize(List<Pair<String, Object>> event) throws JsonProcessingException
  {
    Map<String, Object> map = Maps.newHashMapWithExpectedSize(event.size());
    event.forEach(pair -> map.put(pair.lhs, pair.rhs));
    return jsonMapper.writeValueAsBytes(map);
  }

  @Override
  public void close()
  {
  }
}
