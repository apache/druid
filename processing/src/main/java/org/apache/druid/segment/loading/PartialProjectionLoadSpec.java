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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link LoadSpec} wrapper that carries partial-projection metadata from the coordinator to a historical alongside
 * the original backend-specific load spec. The wrapped {@code delegate} is held as a raw {@link Map} so that the
 * concrete backend type (e.g. {@code s3}, {@code local}, {@code hdfs}) is materialized only when needed; this avoids
 * pulling backend-specific dependencies onto every node that touches the wire form.
 * <p>
 * Both {@link #loadSegment(File)} and {@link #openRangeReader()} delegate verbatim to the inner load spec. The
 * historical-side partial-load path inspects this wrapper at mount time to learn which projections to range-read and
 * the fingerprint identifying the request the coordinator made.
 */
@JsonTypeName(PartialProjectionLoadSpec.TYPE)
public class PartialProjectionLoadSpec implements LoadSpec
{
  public static final String TYPE = "partialProjection";

  private final Map<String, Object> delegate;
  private final List<String> projections;
  private final String fingerprint;
  private final Supplier<LoadSpec> materializedDelegateSupplier;

  @JsonCreator
  public PartialProjectionLoadSpec(
      @JsonProperty("delegate") Map<String, Object> delegate,
      @JsonProperty("projections") List<String> projections,
      @JsonProperty("fingerprint") String fingerprint,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    Preconditions.checkArgument(
        !CollectionUtils.isNullOrEmpty(projections),
        "projections must not be null or empty"
    );
    this.projections = List.copyOf(projections);
    this.fingerprint = Preconditions.checkNotNull(fingerprint, "fingerprint");
    this.materializedDelegateSupplier = Suppliers.memoize(() -> jsonMapper.convertValue(delegate, LoadSpec.class));
  }

  @JsonProperty
  public Map<String, Object> getDelegate()
  {
    return delegate;
  }

  @JsonProperty
  public List<String> getProjections()
  {
    return projections;
  }

  @JsonProperty
  public String getFingerprint()
  {
    return fingerprint;
  }

  @Override
  public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
  {
    return materializedDelegateSupplier.get().loadSegment(destDir);
  }

  @Override
  @Nullable
  public SegmentRangeReader openRangeReader() throws IOException
  {
    return materializedDelegateSupplier.get().openRangeReader();
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
    return Objects.equals(delegate, that.delegate)
        && Objects.equals(projections, that.projections)
        && Objects.equals(fingerprint, that.fingerprint);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(delegate, projections, fingerprint);
  }

  @Override
  public String toString()
  {
    return "PartialProjectionLoadSpec{" +
           "delegate=" + delegate +
           ", projections=" + projections +
           ", fingerprint=" + fingerprint +
           '}';
  }
}
