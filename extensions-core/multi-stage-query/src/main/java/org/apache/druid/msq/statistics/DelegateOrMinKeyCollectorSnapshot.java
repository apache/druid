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
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Objects;

public class DelegateOrMinKeyCollectorSnapshot<T extends KeyCollectorSnapshot> implements KeyCollectorSnapshot
{
  static final String FIELD_SNAPSHOT = "snapshot";
  static final String FIELD_MIN_KEY = "minKey";

  private final T snapshot;
  private final RowKey minKey;

  @JsonCreator
  public DelegateOrMinKeyCollectorSnapshot(
      @JsonProperty(FIELD_SNAPSHOT) final T snapshot,
      @JsonProperty(FIELD_MIN_KEY) final RowKey minKey
  )
  {
    this.snapshot = snapshot;
    this.minKey = minKey;

    if (snapshot != null && minKey != null) {
      throw new ISE("Cannot have both '%s' and '%s'", FIELD_SNAPSHOT, FIELD_MIN_KEY);
    }
  }

  @Nullable
  @JsonProperty(FIELD_SNAPSHOT)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public T getSnapshot()
  {
    return snapshot;
  }

  @Nullable
  @JsonProperty(FIELD_MIN_KEY)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public RowKey getMinKey()
  {
    return minKey;
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
    DelegateOrMinKeyCollectorSnapshot<?> that = (DelegateOrMinKeyCollectorSnapshot<?>) o;
    return Objects.equals(snapshot, that.snapshot) && Objects.equals(minKey, that.minKey);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(snapshot, minKey);
  }
}
