/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.StringUtils;
import io.druid.query.lookup.LookupExtractor;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonTypeName("map")
public class MapLookupExtractor extends LookupExtractor
{
  private final Map<String, String> map;
  private final Map<String, String> reverseMap;

  private final boolean isOneToOne;

  @JsonCreator
  public MapLookupExtractor(
      @JsonProperty("map") Map<String, String> map,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {

    Preconditions.checkNotNull(map, "map");

    this.isOneToOne = isOneToOne;

    if (this.isOneToOne) {
      this.map = HashBiMap.create(map);
      this.reverseMap = ((HashBiMap<String, String>) this.map).inverse();
    } else {
      this.map = map;
      this.reverseMap = null;
    }
  }

  @JsonProperty
  public Map<String, String> getMap()
  {
    return ImmutableMap.copyOf(map);
  }

  @Nullable
  @Override
  public String apply(@NotNull String val)
  {
    return map.get(val);
  }

  @Override
  public List<String> unapply(final String value)
  {
    String valueToLookup = Strings.nullToEmpty(value);

    if (this.reverseMap != null) {
      String val = this.reverseMap.get(valueToLookup);
      return (val != null) ? Collections.singletonList(val) : Collections.emptyList();
    } else {
      return Lists.newArrayList(Maps.filterKeys(map, key -> map.get(key).equals(valueToLookup)).keySet());
    }
  }

  @Override
  @JsonProperty("isOneToOne")
  public boolean isOneToOne()
  {
    return isOneToOne;
  }

  @Override
  public byte[] getCacheKey()
  {
    try {
      final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      for (Map.Entry<String, String> entry : map.entrySet()) {
        final String key = entry.getKey();
        final String val = entry.getValue();
        if (!Strings.isNullOrEmpty(key)) {
          outputStream.write(StringUtils.toUtf8(key));
        }
        outputStream.write((byte) 0xFF);
        if (!Strings.isNullOrEmpty(val)) {
          outputStream.write(StringUtils.toUtf8(val));
        }
        outputStream.write((byte) 0xFF);
      }
      return outputStream.toByteArray();
    }
    catch (IOException ex) {
      // If ByteArrayOutputStream.write has problems, that is a very bad thing
      throw Throwables.propagate(ex);
    }
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

    MapLookupExtractor that = (MapLookupExtractor) o;

    return map.equals(that.map);
  }

  @Override
  public int hashCode()
  {
    return map.hashCode();
  }

}
