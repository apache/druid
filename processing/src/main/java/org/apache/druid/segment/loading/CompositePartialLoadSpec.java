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
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link PartialLoadSpec} that combines several sibling partial-load specs, selecting the union of every member's
 * bundles. This lets one partial-load rule mix schemes and lets a single scheme express selections a single
 * matcher cannot, such as a disjoint union of include/exclude pattern pairs in wildcard based matchers.
 * <p>
 * Members are held as raw {@link Map} load specs and materialized lazily, the same way the base class handles
 * {@link #getDelegate()}. Member load specs deliberately omit the {@link #DELEGATE_FIELD} field: every member of a
 * composite describes a selection within the <em>same</em> segment, so repeating the backend load spec once per
 * member would bloat every load request and announcement for no benefit. This spec injects its own
 * {@link #getDelegate()} into each member as it materializes it, which is why a member carrying its own
 * {@code delegate} is rejected at construction (it means the producing matcher failed to strip it, and silently
 * overwriting it could mask a real mismatch).
 * <p>
 * Composites nest: a member that is itself a {@code partialComposite} receives the injected delegate and injects it
 * into its own members in turn.
 */
@JsonTypeName(CompositePartialLoadSpec.TYPE)
public class CompositePartialLoadSpec extends PartialLoadSpec
{
  public static final String TYPE = "partialComposite";

  /**
   * Builds the raw {@link Map} form of a {@link CompositePartialLoadSpec} request. Used by the coordinator-side
   * matcher, which doesn't instantiate the typed class because doing so would require plumbing an
   * {@link ObjectMapper} through every matcher just to satisfy the constructor's lazy-materialization suppliers.
   * <p>
   * Each entry of {@code members} must be the raw {@link Map} form of some other {@link PartialLoadSpec}
   * <em>without</em> its {@link #DELEGATE_FIELD} field; see the class doc.
   */
  public static Map<String, Object> wireForm(
      Map<String, Object> delegate,
      List<Map<String, Object>> members,
      String fingerprint
  )
  {
    return Map.of(
        TYPE_FIELD, TYPE,
        DELEGATE_FIELD, delegate,
        "members", members,
        FINGERPRINT_FIELD, fingerprint
    );
  }

  private final List<Map<String, Object>> members;
  private final Supplier<List<PartialLoadSpec>> materializedMembersSupplier;

  @JsonCreator
  public CompositePartialLoadSpec(
      @JsonProperty("delegate") Map<String, Object> delegate,
      @JsonProperty("members") List<Map<String, Object>> members,
      @JsonProperty("fingerprint") String fingerprint,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(delegate, fingerprint, jsonMapper);
    Preconditions.checkArgument(
        !CollectionUtils.isNullOrEmpty(members),
        "members must not be null or empty"
    );
    final List<Map<String, Object>> copied = new ArrayList<>(members.size());
    for (int i = 0; i < members.size(); i++) {
      copied.add(validateMember(members.get(i), i));
    }
    this.members = List.copyOf(copied);
    this.materializedMembersSupplier = Suppliers.memoize(() -> materializeMembers(jsonMapper));
  }

  @JsonProperty
  public List<Map<String, Object>> getMembers()
  {
    return members;
  }

  /**
   * The union of every member's selected bundles, in member order and then member-internal order. Duplicates are
   * dropped: two members of the same scheme can legitimately select overlapping bundles (e.g. two cluster-group
   * selections that share a group), and the caller treats the result as a set.
   * <p>
   * The base bundle needs no special handling here —
   * {@code PartialSegmentMetadataCacheEntry#bundlesInMountOrder} expands each selected bundle's inferred
   * dependencies, which pins {@code __base} exactly once regardless of how many members asked for something that
   * depends on it.
   * <p>
   * Returns an empty list only when every member selects nothing (the "sibling-empty" case propagated through
   * composition).
   */
  @Override
  public List<String> getSelectedBundleNames(DataSegment segment, SegmentFileMetadata metadata)
  {
    final LinkedHashSet<String> union = new LinkedHashSet<>();
    for (PartialLoadSpec member : materializedMembersSupplier.get()) {
      union.addAll(member.getSelectedBundleNames(segment, metadata));
    }
    return List.copyOf(union);
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
    CompositePartialLoadSpec that = (CompositePartialLoadSpec) o;
    return Objects.equals(getDelegate(), that.getDelegate())
        && Objects.equals(members, that.members)
        && Objects.equals(getFingerprint(), that.getFingerprint());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getDelegate(), members, getFingerprint());
  }

  @Override
  public String toString()
  {
    return "CompositePartialLoadSpec{" +
           "delegate=" + getDelegate() +
           ", members=" + members +
           ", fingerprint=" + getFingerprint() +
           '}';
  }

  private List<PartialLoadSpec> materializeMembers(ObjectMapper jsonMapper)
  {
    final List<PartialLoadSpec> materialized = new ArrayList<>(members.size());
    for (Map<String, Object> member : members) {
      // Splice this composite's delegate into the member before materializing it: members omit it on the wire, but
      // the PartialLoadSpec constructor requires one.
      final Map<String, Object> withDelegate = new LinkedHashMap<>(member);
      withDelegate.put(DELEGATE_FIELD, getDelegate());
      final LoadSpec memberSpec = jsonMapper.convertValue(withDelegate, LoadSpec.class);
      if (!(memberSpec instanceof PartialLoadSpec partialMember)) {
        throw DruidException.defensive(
            "Composite partial load spec member of type[%s] materialized to non-partial type[%s]",
            member.get(TYPE_FIELD),
            memberSpec.getClass().getSimpleName()
        );
      }
      materialized.add(partialMember);
    }
    return materialized;
  }

  /**
   * Validates and defensively copies one member load spec. A member must carry a partial-load {@link #TYPE_FIELD} and
   * must not carry a {@link #DELEGATE_FIELD} of its own; see the class doc for why.
   */
  private static Map<String, Object> validateMember(Map<String, Object> member, int index)
  {
    if (member == null || member.isEmpty()) {
      throw DruidException.defensive("members[%s] must not be null or empty", index);
    }
    if (!hasPartialTypePrefix(member)) {
      throw DruidException.defensive(
          "members[%s] must be a partial load spec with a type starting with [%s], got type[%s]",
          index,
          TYPE_PREFIX,
          member.get(TYPE_FIELD)
      );
    }
    if (member.containsKey(DELEGATE_FIELD)) {
      throw DruidException.defensive(
          "members[%s] of type[%s] must not carry its own [%s]; the composite supplies it",
          index,
          member.get(TYPE_FIELD),
          DELEGATE_FIELD
      );
    }
    return Map.copyOf(member);
  }
}
