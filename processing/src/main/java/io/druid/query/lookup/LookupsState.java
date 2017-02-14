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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 */
public class LookupsState
{
  private Map<String, LookupExtractorFactoryContainer> current;
  private Map<String, LookupExtractorFactoryContainer> toLoad;
  private Set<String> toDrop;

  @JsonCreator
  public LookupsState(
      @JsonProperty("current") Map<String, LookupExtractorFactoryContainer> current,
      @JsonProperty("toLoad") Map<String, LookupExtractorFactoryContainer> toLoad,
      @JsonProperty("toDrop") Set<String> toDrop
  )
  {
    this.current = current == null ? Collections.EMPTY_MAP : current;
    this.toLoad = toLoad == null ? Collections.EMPTY_MAP : toLoad;
    this.toDrop = toDrop == null ? Collections.EMPTY_SET : toDrop;
  }

  @JsonProperty
  public Map<String, LookupExtractorFactoryContainer> getCurrent()
  {
    return current;
  }

  @JsonProperty
  public Map<String, LookupExtractorFactoryContainer> getToLoad()
  {
    return toLoad;
  }

  @JsonProperty
  public Set<String> getToDrop()
  {
    return toDrop;
  }

  @Override
  public String toString()
  {
    return "LookupsState{" +
           "current=" + current +
           ", toLoad=" + toLoad +
           ", toDrop=" + toDrop +
           '}';
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

    LookupsState that = (LookupsState) o;

    if (!current.equals(that.current)) {
      return false;
    }
    if (!toLoad.equals(that.toLoad)) {
      return false;
    }
    return toDrop.equals(that.toDrop);

  }

  @Override
  public int hashCode()
  {
    int result = current.hashCode();
    result = 31 * result + toLoad.hashCode();
    result = 31 * result + toDrop.hashCode();
    return result;
  }
}
