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
import org.apache.druid.frame.key.RowKey;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ClusterByStatisticsSnapshot
{
  private final List<Bucket> buckets;
  private final Set<Integer> hasMultipleValues;

  @JsonCreator
  ClusterByStatisticsSnapshot(
      @JsonProperty("buckets") final List<Bucket> buckets,
      @JsonProperty("hasMultipleValues") @Nullable final Set<Integer> hasMultipleValues
  )
  {
    this.buckets = Preconditions.checkNotNull(buckets, "buckets");
    this.hasMultipleValues = hasMultipleValues != null ? hasMultipleValues : Collections.emptySet();
  }

  public static ClusterByStatisticsSnapshot empty()
  {
    return new ClusterByStatisticsSnapshot(Collections.emptyList(), null);
  }

  @JsonProperty("buckets")
  List<Bucket> getBuckets()
  {
    return buckets;
  }

  @JsonProperty("hasMultipleValues")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Set<Integer> getHasMultipleValues()
  {
    return hasMultipleValues;
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
    ClusterByStatisticsSnapshot that = (ClusterByStatisticsSnapshot) o;
    return Objects.equals(buckets, that.buckets) && Objects.equals(hasMultipleValues, that.hasMultipleValues);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(buckets, hasMultipleValues);
  }

  static class Bucket
  {
    private final RowKey bucketKey;
    private final KeyCollectorSnapshot keyCollectorSnapshot;

    @JsonCreator
    Bucket(
        @JsonProperty("bucketKey") RowKey bucketKey,
        @JsonProperty("data") KeyCollectorSnapshot keyCollectorSnapshot
    )
    {
      this.bucketKey = Preconditions.checkNotNull(bucketKey, "bucketKey");
      this.keyCollectorSnapshot = Preconditions.checkNotNull(keyCollectorSnapshot, "data");
    }

    @JsonProperty
    public RowKey getBucketKey()
    {
      return bucketKey;
    }

    @JsonProperty("data")
    public KeyCollectorSnapshot getKeyCollectorSnapshot()
    {
      return keyCollectorSnapshot;
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
      Bucket bucket = (Bucket) o;
      return Objects.equals(bucketKey, bucket.bucketKey)
             && Objects.equals(keyCollectorSnapshot, bucket.keyCollectorSnapshot);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(bucketKey, keyCollectorSnapshot);
    }
  }
}
