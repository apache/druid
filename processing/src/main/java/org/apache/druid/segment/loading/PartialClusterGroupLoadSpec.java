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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link PartialLoadSpec} that requests partial loading of a clustered segment's cluster groups. The base class
 * carries the common {@code fingerprint} and {@code delegate} wire fields; this subtype adds the resolved
 * {@code clusterGroupIndices} (positions into {@link org.apache.druid.timeline.ClusterGroupTuples#getTuples()}) that
 * the historical should range-read into the local segment.
 */
@JsonTypeName(PartialClusterGroupLoadSpec.TYPE)
public class PartialClusterGroupLoadSpec extends PartialLoadSpec
{
  public static final String TYPE = "partialClusterGroup";

  /**
   * Builds the raw wire-form {@link Map} representation of a {@link PartialClusterGroupLoadSpec} request. Used by the
   * coordinator-side matcher (which doesn't instantiate the typed class because doing so would require plumbing an
   * {@link ObjectMapper} through every matcher just to satisfy the constructor's lazy-delegate supplier).
   */
  public static Map<String, Object> wireForm(
      Map<String, Object> delegate,
      List<Integer> clusterGroupIndices,
      String fingerprint
  )
  {
    return Map.of(
        "type", TYPE,
        "delegate", delegate,
        "clusterGroupIndices", clusterGroupIndices,
        "fingerprint", fingerprint
    );
  }

  private final List<Integer> clusterGroupIndices;

  @JsonCreator
  public PartialClusterGroupLoadSpec(
      @JsonProperty("delegate") Map<String, Object> delegate,
      @JsonProperty("clusterGroupIndices") List<Integer> clusterGroupIndices,
      @JsonProperty("fingerprint") String fingerprint,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(delegate, fingerprint, jsonMapper);
    // An empty index list is the wire form of the matcher's "empty match", used when a cluster-group matcher matches
    // some siblings of a shard group but not this one. The historical-side partial loader honors this by performing
    // no cluster-group-specific data download, keeping the shard group uniformly populated for broker-side
    // completeness.
    Preconditions.checkNotNull(clusterGroupIndices, "clusterGroupIndices");
    this.clusterGroupIndices = List.copyOf(clusterGroupIndices);
  }

  @JsonProperty
  public List<Integer> getClusterGroupIndices()
  {
    return clusterGroupIndices;
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
    PartialClusterGroupLoadSpec that = (PartialClusterGroupLoadSpec) o;
    return Objects.equals(getDelegate(), that.getDelegate())
        && Objects.equals(clusterGroupIndices, that.clusterGroupIndices)
        && Objects.equals(getFingerprint(), that.getFingerprint());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getDelegate(), clusterGroupIndices, getFingerprint());
  }

  @Override
  public String toString()
  {
    return "PartialClusterGroupLoadSpec{" +
           "delegate=" + getDelegate() +
           ", clusterGroupIndices=" + clusterGroupIndices +
           ", fingerprint=" + getFingerprint() +
           '}';
  }
}
