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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.IAE;

import java.util.Map;
import java.util.Objects;

public abstract class SeekableStreamDataSourceMetadata<T1, T2> implements DataSourceMetadata
{
  private final SeekableStreamPartitions<T1, T2> seekableStreamPartitions;

  @JsonCreator
  public SeekableStreamDataSourceMetadata(
      @JsonProperty("partitions") SeekableStreamPartitions<T1, T2> seekableStreamPartitions
  )
  {
    this.seekableStreamPartitions = seekableStreamPartitions;
  }

  @JsonProperty("partitions")
  public SeekableStreamPartitions<T1, T2> getSeekableStreamPartitions()
  {
    return seekableStreamPartitions;
  }

  @Override
  public boolean isValidStart()
  {
    return true;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    return plus(other).equals(other.plus(this));
  }


  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    if (!(this.getClass().isInstance(other))) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    @SuppressWarnings("unchecked")
    final SeekableStreamDataSourceMetadata<T1, T2> that = (SeekableStreamDataSourceMetadata<T1, T2>) other;

    if (that.getSeekableStreamPartitions().getId().equals(seekableStreamPartitions.getId())) {
      // Same topic, merge offsets.
      final Map<T1, T2> newMap = Maps.newHashMap();

      for (Map.Entry<T1, T2> entry : seekableStreamPartitions.getPartitionSequenceMap().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<T1, T2> entry : that.getSeekableStreamPartitions().getPartitionSequenceMap().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      return createConcretDataSourceMetaData(seekableStreamPartitions.getId(), newMap);
    } else {
      // Different topic, prefer "other".
      return other;
    }
  }


  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    if (!(this.getClass().isInstance(other))) {
      throw new IAE(
          "Expected instance of %s, got %s",
          this.getClass().getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    @SuppressWarnings("unchecked")
    final SeekableStreamDataSourceMetadata<T1, T2> that = (SeekableStreamDataSourceMetadata<T1, T2>) other;

    if (that.getSeekableStreamPartitions().getId().equals(seekableStreamPartitions.getId())) {
      // Same stream, remove partitions present in "that" from "this"
      final Map<T1, T2> newMap = Maps.newHashMap();

      for (Map.Entry<T1, T2> entry : seekableStreamPartitions.getPartitionSequenceMap().entrySet()) {
        if (!that.getSeekableStreamPartitions().getPartitionSequenceMap().containsKey(entry.getKey())) {
          newMap.put(entry.getKey(), entry.getValue());
        }
      }

      return createConcretDataSourceMetaData(seekableStreamPartitions.getId(), newMap);
    } else {
      // Different stream, prefer "this".
      return this;
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    SeekableStreamDataSourceMetadata that = (SeekableStreamDataSourceMetadata) o;
    return Objects.equals(getSeekableStreamPartitions(), that.getSeekableStreamPartitions());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getSeekableStreamPartitions());
  }

  @Override
  public String toString()
  {
    return "SeekableStreamDataSourceMetadata{" +
           "SeekableStreamPartitions=" + getSeekableStreamPartitions() +
           '}';
  }

  protected abstract SeekableStreamDataSourceMetadata<T1, T2> createConcretDataSourceMetaData(
      String streamId,
      Map<T1, T2> newMap
  );
}
