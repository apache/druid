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

package org.apache.druid.query.lookup.namespace;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * This class is intended to be used in general cluster testing, and not as a serious lookup.
 * Any desire to use a static map in a lookup in *general* should use `org.apache.druid.query.extraction.MapLookupExtractor`
 * Any desire to test the *caching mechanisms in this extension* can use this class.
 */
@JsonTypeName(StaticMapExtractionNamespace.TYPE_NAME)
public class StaticMapExtractionNamespace implements ExtractionNamespace
{
  static final String TYPE_NAME = "staticMap";
  private final Map<String, String> map;

  @JsonCreator
  public StaticMapExtractionNamespace(
      @JsonProperty("map") Map<String, String> map
  )
  {
    this.map = Preconditions.checkNotNull(map, "`map` required");
  }

  @JsonProperty
  public Map<String, String> getMap()
  {
    return map;
  }

  @Override
  public long getPollMs()
  {
    // Load once and forget it
    return 0;
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

    StaticMapExtractionNamespace that = (StaticMapExtractionNamespace) o;

    return getMap().equals(that.getMap());

  }

  @Override
  public int hashCode()
  {
    return getMap().hashCode();
  }
}
