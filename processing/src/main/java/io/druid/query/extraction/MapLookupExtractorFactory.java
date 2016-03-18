/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactory;

import javax.annotation.Nullable;
import java.util.Map;

public class MapLookupExtractorFactory implements LookupExtractorFactory
{
  private final Map<String, String> map;
  private final boolean isOneToOne;
  private final MapLookupExtractor lookupExtractor;

  @JsonCreator
  public MapLookupExtractorFactory(
      @JsonProperty("map") Map<String, String> map,
      @JsonProperty("isOneToOne") boolean isOneToOne
  )
  {
    this.map = Preconditions.checkNotNull(map, "map cannot be null");
    this.isOneToOne = isOneToOne;
    this.lookupExtractor = new MapLookupExtractor(map, isOneToOne);
  }

  @Override
  public boolean start()
  {
    return true;
  }

  @Override
  public boolean close()
  {
    return true;
  }

  /**
   * For MapLookups, the replaces consideration is very easy, it simply considers if the other is the same as this one
   *
   * @param other Some other LookupExtractorFactory which might need replaced
   * @return true - should replace,   false - should not replace
   */
  @Override
  public boolean replaces(@Nullable LookupExtractorFactory other)
  {
    return !equals(other);
  }

  @Override
  public LookupExtractor get()
  {
    return lookupExtractor;
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

    MapLookupExtractorFactory that = (MapLookupExtractorFactory) o;

    if (isOneToOne != that.isOneToOne) {
      return false;
    }
    return map.equals(that.map);

  }

  @Override
  public int hashCode()
  {
    int result = map.hashCode();
    result = 31 * result + (isOneToOne ? 1 : 0);
    return result;
  }
}
