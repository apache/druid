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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Base for {@link LoadSpec} wrappers that carry partial-load metadata (a fingerprint identifying the request the
 * coordinator made plus a raw {@code delegate} inner load spec describing where the segment data lives) alongside
 * scheme-specific data in concrete subtypes (projection names, row ranges, etc.). The {@code delegate} is held as a
 * raw {@link Map} so the concrete backend type (e.g. {@code s3}, {@code local}, {@code hdfs}) is only materialized
 * when needed via {@link #materializedDelegate()}; this avoids pulling backend-specific dependencies onto every node
 * that touches the wire form.
 * <p>
 * The default {@link #loadSegment(File)} and {@link #openRangeReader()} delegate verbatim to the materialized inner
 * load spec — i.e., a full load. Subtypes override these only when they actually implement scheme-specific partial
 * loading; until then, historicals fall back to a full download and the announcement-side path stamps the
 * wrapper's fingerprint plus the full segment size on the load announcement so the coordinator counts the replica
 * as a satisfying full-fallback rather than re-queueing the load.
 * <p>
 * Wire-format conventions required of all subtypes (enforced by code that inspects raw {@code Map}-form load specs):
 * <ul>
 *   <li>The Jackson {@code @JsonTypeName} starts with {@link #TYPE_PREFIX} ({@code "partial"}).</li>
 *   <li>The wire form includes {@code fingerprint} ({@link String}) and {@code delegate} ({@link Map}) at the top
 *       level. These are provided by {@link #getFingerprint()} and {@link #getDelegate()} on this base class, so
 *       subtypes inherit the contract automatically.</li>
 * </ul>
 * Together these let callers identify any partial-load wrapper from its raw map form without enumerating concrete
 * subtypes.
 */
public abstract class PartialLoadSpec implements LoadSpec
{
  /**
   * All partial-load LoadSpec wire types use this Jackson type-name prefix. See class doc for the convention.
   */
  public static final String TYPE_PREFIX = "partial";

  /**
   * Returns {@code true} if {@code loadSpec} matches the shape of the {@link PartialLoadSpec} subtype.
   * Convention-based detection (no subtype allowlist): the {@code type} field must be a {@link String} starting with
   * {@link #TYPE_PREFIX}, the {@code fingerprint} field must be a {@link String}, and the {@code delegate} field
   * must be a {@link Map}. These properties are enforced by this base class's {@code @JsonProperty} getters, so any
   * subtype satisfies them automatically.
   */
  public static boolean detectPartialLoadSpec(@Nullable Map<String, Object> loadSpec)
  {
    return loadSpec != null
           && loadSpec.get("type") instanceof String typeString
           && typeString.startsWith(TYPE_PREFIX)
           && loadSpec.get("fingerprint") instanceof String
           && loadSpec.get("delegate") instanceof Map;
  }

  /**
   * Returns {@code true} if {@code loadSpec}'s {@code type} field claims partial-load semantics (starts with
   * {@link #TYPE_PREFIX}), regardless of whether the remaining wire form is well-formed. Useful when callers want
   * to distinguish "not a partial-load wrapper" from "claims to be partial but the {@code fingerprint} or
   * {@code delegate} fields are missing or malformed" — the latter typically indicates a bug worth logging.
   */
  public static boolean hasPartialTypePrefix(@Nullable Map<String, Object> loadSpec)
  {
    return loadSpec != null
           && loadSpec.get("type") instanceof String typeString
           && typeString.startsWith(TYPE_PREFIX);
  }

  private final Map<String, Object> delegate;
  private final String fingerprint;
  private final Supplier<LoadSpec> materializedDelegateSupplier;

  protected PartialLoadSpec(Map<String, Object> delegate, String fingerprint, ObjectMapper jsonMapper)
  {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    this.fingerprint = Preconditions.checkNotNull(fingerprint, "fingerprint");
    Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.materializedDelegateSupplier = Suppliers.memoize(() -> jsonMapper.convertValue(delegate, LoadSpec.class));
  }

  @JsonProperty
  public final Map<String, Object> getDelegate()
  {
    return delegate;
  }

  @JsonProperty
  public final String getFingerprint()
  {
    return fingerprint;
  }

  /**
   * Returns the materialized inner {@link LoadSpec}. Lazy + memoized so the backend-specific deserialization runs
   * once per wrapper instance. Used by the default {@link #loadSegment(File)} / {@link #openRangeReader()}
   * implementations and available to subtypes that want to compose full-fallback into their scheme-specific paths.
   */
  protected LoadSpec materializedDelegate()
  {
    return materializedDelegateSupplier.get();
  }

  @Override
  public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
  {
    return materializedDelegate().loadSegment(destDir);
  }

  @Override
  @Nullable
  public SegmentRangeReader openRangeReader() throws IOException
  {
    return materializedDelegate().openRangeReader();
  }

  /**
   * Returns the cache-layer bundle names that this partial-load spec selects for {@code segment}, resolving the
   * scheme-specific identifiers carried in the load spec against the segment {@code metadata}. The historical's cache
   * layer uses the returned names to eagerly mount and pin the matching bundles as static entries; other bundles
   * remain weak and on-demand.
   * <p>
   * Returns an empty list when the load spec carries an empty selection, where only the metadata header is sticky and
   * no bundle is pinned.
   */
  public abstract List<String> getSelectedBundleNames(DataSegment segment, SegmentFileMetadata metadata);
}
