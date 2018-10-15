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


package org.apache.druid.java.util.emitter.service;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.core.Event;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class UnitEvent implements Event
{
  private final String feed;
  private final Number value;
  private final Map<String, String> dimensions;

  public UnitEvent(String feed, Number value)
  {
    this(feed, value, Collections.emptyMap());
  }

  public UnitEvent(String feed, Number value, Map<String, String> dimensions)
  {
    this.feed = feed;
    this.value = value;
    this.dimensions = dimensions;
  }

  @Override
  @JsonValue
  public Map<String, Object> toMap()
  {
    Map<String, Object> result = new HashMap<>(dimensions);
    result.put("feed", feed);
    result.put("metrics", ImmutableMap.of("value", value));
    return ImmutableMap.copyOf(result);
  }

  @Override
  public String getFeed()
  {
    return feed;
  }

}
