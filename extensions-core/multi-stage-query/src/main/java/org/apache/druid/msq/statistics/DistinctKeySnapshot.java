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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.frame.key.RowKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DistinctKeySnapshot implements KeyCollectorSnapshot
{
  private final List<SerializablePair<RowKey, Long>> keys;
  private final int spaceReductionFactor;

  @JsonCreator
  DistinctKeySnapshot(
      @JsonProperty("keys") final List<SerializablePair<RowKey, Long>> keys,
      @JsonProperty("spaceReductionFactor") final int spaceReductionFactor
  )
  {
    this.keys = Preconditions.checkNotNull(keys, "keys");
    this.spaceReductionFactor = spaceReductionFactor;
  }

  @JsonProperty
  public List<SerializablePair<RowKey, Long>> getKeys()
  {
    return keys;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getSpaceReductionFactor()
  {
    return spaceReductionFactor;
  }

  public Map<RowKey, Long> getKeysAsMap()
  {
    final Map<RowKey, Long> keysMap = new HashMap<>();

    for (final SerializablePair<RowKey, Long> key : keys) {
      keysMap.put(key.lhs, key.rhs);
    }

    return keysMap;
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
    DistinctKeySnapshot that = (DistinctKeySnapshot) o;

    // Not expected to be called in production, so it's OK that this calls getKeysAsMap() each time.
    return spaceReductionFactor == that.spaceReductionFactor && Objects.equals(getKeysAsMap(), that.getKeysAsMap());
  }

  @Override
  public int hashCode()
  {
    // Not expected to be called in production, so it's OK that this calls getKeysAsMap() each time.
    return Objects.hash(getKeysAsMap(), spaceReductionFactor);
  }
}
