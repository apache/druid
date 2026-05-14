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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.PartialLoadSpec;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Wire form for a coordinator load request and a historical's load announcement.
 * <p>
 * The {@code fingerprint} and {@code loadedBytes} fields are optional and only populated by historicals when
 * announcing a partial load. Coordinator-issued load requests leave them null; partial-load metadata rides inside the
 * wrapped {@code LoadSpec} on outbound requests instead.
 */
public class SegmentChangeRequestLoad implements DataSegmentChangeRequest
{
  private static final Logger log = new Logger(SegmentChangeRequestLoad.class);

  /**
   * Builds a load announcement for a segment loaded on a historical. Any {@link PartialLoadSpec} subtype (identified
   * by the wire-form conventions documented on that class: a {@code type} starting with
   * {@link PartialLoadSpec#TYPE_PREFIX}, plus top-level {@code fingerprint} and {@code delegate} fields) produces a
   * "full-fallback" announcement: the wrapper's fingerprint and {@link DataSegment#getSize()} ride along as
   * {@code loadedBytes}, satisfying the coordinator's partial-load rule even when the historical did a regular full
   * load via the inner delegate. Without this, the coordinator's reconciler would treat the replica as stale and
   * re-queue the load indefinitely.
   * <p>
   * Detection is convention-based (no subtype allowlist) so future {@link PartialLoadSpec} subtypes work
   * automatically without touching this code.
   * <p>
   * For segments loaded without a partial-load wrapper (the common case), this returns a bare load request with no
   * fingerprint or loadedBytes, equivalent to {@link #SegmentChangeRequestLoad(DataSegment)}.
   */
  public static SegmentChangeRequestLoad forAnnouncement(DataSegment segment)
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    if (loadSpec == null) {
      return new SegmentChangeRequestLoad(segment);
    }
    final Object type = loadSpec.get("type");
    if (!(type instanceof String stringType) || !stringType.startsWith(PartialLoadSpec.TYPE_PREFIX)) {
      return new SegmentChangeRequestLoad(segment);
    }
    final Object fingerprint = loadSpec.get("fingerprint");
    final Object delegate = loadSpec.get("delegate");
    if (fingerprint instanceof String stringFingerprint && delegate instanceof Map) {
      return new SegmentChangeRequestLoad(segment, stringFingerprint, segment.getSize());
    }
    // Type name says partial-load but the wire form is malformed, the PartialLoadSpec subtype's @JsonProperty
    // contract guarantees both fields, so this is a bug. Log and fall through to a plain announcement to keep the
    // queue moving.
    log.warn(
        "Partial-load wrapper for segment[%s] type[%s] is malformed (fingerprint[%s], delegate[%s]); "
        + "announcing as a regular load.",
        segment.getId(),
        type,
        fingerprint,
        delegate
    );
    return new SegmentChangeRequestLoad(segment);
  }

  private final DataSegment segment;
  @Nullable private final String fingerprint;
  @Nullable private final Long loadedBytes;

  /**
   * To avoid pruning of the loadSpec on the broker, needed when the broker is loading broadcast segments,
   * we deserialize into a {@link LoadableDataSegment}, which never removes the loadSpec.
   */
  @JsonCreator
  public SegmentChangeRequestLoad(
      @JsonUnwrapped LoadableDataSegment segment,
      @JsonProperty("fingerprint") @Nullable String fingerprint,
      @JsonProperty("loadedBytes") @Nullable Long loadedBytes
  )
  {
    this((DataSegment) segment, fingerprint, loadedBytes);
  }

  public SegmentChangeRequestLoad(DataSegment segment)
  {
    this(segment, null, null);
  }

  public SegmentChangeRequestLoad(
      DataSegment segment,
      @Nullable String fingerprint,
      @Nullable Long loadedBytes
  )
  {
    this.segment = segment;
    this.fingerprint = fingerprint;
    this.loadedBytes = loadedBytes;
  }


  @Override
  public void go(DataSegmentChangeHandler handler, @Nullable DataSegmentChangeCallback callback)
  {
    handler.addSegment(segment, callback);
  }

  @JsonProperty
  @JsonUnwrapped
  public DataSegment getSegment()
  {
    return segment;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public String getFingerprint()
  {
    return fingerprint;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Long getLoadedBytes()
  {
    return loadedBytes;
  }

  @Override
  public String asString()
  {
    return StringUtils.format("LOAD: %s", segment.getId());
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
    SegmentChangeRequestLoad that = (SegmentChangeRequestLoad) o;
    return Objects.equals(segment, that.segment)
        && Objects.equals(fingerprint, that.fingerprint)
        && Objects.equals(loadedBytes, that.loadedBytes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segment, fingerprint, loadedBytes);
  }

  @Override
  public String toString()
  {
    return "SegmentChangeRequestLoad{" +
           "segment=" + segment +
           (fingerprint != null ? ", fingerprint=" + fingerprint : "") +
           (loadedBytes != null ? ", loadedBytes=" + loadedBytes : "") +
           '}';
  }
}
