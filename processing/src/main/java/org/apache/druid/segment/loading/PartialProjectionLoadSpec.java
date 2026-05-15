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
import org.apache.druid.utils.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link PartialLoadSpec} that requests partial loading of a segment's projections. The base class carries the
 * common {@code fingerprint} and {@code delegate} wire fields; this subtype adds the resolved projection names that
 * the historical should range-read into the local segment.
 * <p>
 * The historical-side partial-load path inspects this wrapper at mount time. Until that path exists, the base
 * class's default {@link #loadSegment} performs a full download via the inner delegate, and the announcement layer
 * stamps the fingerprint + full size on the response so the coordinator's reconciler counts the replica as a
 * satisfying full-fallback rather than re-queuing the load.
 */
@JsonTypeName(PartialProjectionLoadSpec.TYPE)
public class PartialProjectionLoadSpec extends PartialLoadSpec
{
  public static final String TYPE = "partialProjection";

  /**
   * Builds the raw wire-form {@link Map} representation of a {@link PartialProjectionLoadSpec} request. Used by the
   * coordinator-side matcher (which doesn't instantiate the typed class because doing so would require plumbing an
   * {@link ObjectMapper} through every matcher just to satisfy the constructor's lazy-delegate supplier).
   */
  public static Map<String, Object> wireForm(
      Map<String, Object> delegate,
      List<String> projections,
      String fingerprint
  )
  {
    return Map.of(
        "type", TYPE,
        "delegate", delegate,
        "projections", projections,
        "fingerprint", fingerprint
    );
  }

  private final List<String> projections;

  @JsonCreator
  public PartialProjectionLoadSpec(
      @JsonProperty("delegate") Map<String, Object> delegate,
      @JsonProperty("projections") List<String> projections,
      @JsonProperty("fingerprint") String fingerprint,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(delegate, fingerprint, jsonMapper);
    Preconditions.checkArgument(
        !CollectionUtils.isNullOrEmpty(projections),
        "projections must not be null or empty"
    );
    this.projections = List.copyOf(projections);
  }

  @JsonProperty
  public List<String> getProjections()
  {
    return projections;
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
    PartialProjectionLoadSpec that = (PartialProjectionLoadSpec) o;
    return Objects.equals(getDelegate(), that.getDelegate())
        && Objects.equals(projections, that.projections)
        && Objects.equals(getFingerprint(), that.getFingerprint());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getDelegate(), projections, getFingerprint());
  }

  @Override
  public String toString()
  {
    return "PartialProjectionLoadSpec{" +
           "delegate=" + getDelegate() +
           ", projections=" + projections +
           ", fingerprint=" + getFingerprint() +
           '}';
  }
}
