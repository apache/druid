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

package org.apache.druid.segment.incremental;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.SortedDimensionDictionary;
import org.apache.druid.segment.StringDimensionDictionary;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ClusteringDictionaries;
import org.apache.druid.segment.projections.TableClusterGroupSpec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Segment-wide root of the clustered base-table machinery for {@link OnheapIncrementalIndex}: holds the per-type
 * shared clustering dictionaries, the clustering virtual-column selector factory, and the map of
 * {@link OnHeapClusterGroup} instances keyed by the clustering-value dictionary-ID tuple. The parent
 * {@code OnheapIncrementalIndex} delegates row ingestion here when its schema carries a
 * {@link ClusteredValueGroupsBaseTableProjectionSpec}, and the base facts holder stays empty in that mode.
 * <p>
 * On row arrival: each clustering column's value is resolved through the virtual-column selector, then
 * dictionary-encoded into the per-type dictionary for the column's clustering type. The resulting dictionary-ID tuple
 * becomes the lookup key for the {@link OnHeapClusterGroup} that owns the row, new groups are materialized lazily as
 * new tuples arrive.
 * <p>
 * Dictionaries here are insertion-order (id = first-seen position). The persist path sorts + remaps them into the
 * read-side {@link ClusteringDictionaries} shape (sorted, nulls first) at segment-write time.
 */
public final class OnHeapClusteredBaseTable
{
  private static final Comparator<TableClusterGroupSpec> BY_CLUSTERING_VALUE_IDS =
      Comparator.comparing(TableClusterGroupSpec::getClusteringValueIds, Ordering.<Integer>natural().lexicographical());

  private final ClusteredValueGroupsBaseTableProjectionSpec spec;
  private final RowSignature clusteringColumns;
  private final VirtualColumns virtualColumns;
  private final ColumnSelectorFactory virtualSelectorFactory;
  private final IncrementalIndex.InputRowHolder inputRowHolder;

  // template state for instantiating new OnHeapClusterGroups
  private final List<DimensionSchema> nonClusteringDimensions;
  private final AggregatorFactory[] aggregatorFactories;
  private final boolean rollup;
  private final int timePosition;

  private final DimensionDictionary<String> stringDictionary = new StringDimensionDictionary();
  private final DimensionDictionary<Long> longDictionary = new DimensionDictionary<>(Long.class)
  {
    @Override
    public long estimateSizeOfValue(Long value)
    {
      return Long.BYTES;
    }
  };
  private final DimensionDictionary<Double> doubleDictionary = new DimensionDictionary<>(Double.class)
  {
    @Override
    public long estimateSizeOfValue(Double value)
    {
      return Double.BYTES;
    }
  };
  private final DimensionDictionary<Float> floatDictionary = new DimensionDictionary<>(Float.class)
  {
    @Override
    public long estimateSizeOfValue(Float value)
    {
      return Float.BYTES;
    }
  };

  // Keyed by the clustering-value dictionary-ID tuple
  private final ConcurrentHashMap<List<Integer>, OnHeapClusterGroup> groups = new ConcurrentHashMap<>();
  private final AtomicInteger totalNumRows = new AtomicInteger(0);
  // min/max bucketed row timestamp across all groups; the base facts holder is empty in clustered mode so the
  // parent index's interval accessors delegate here instead.
  private final AtomicLong minTimeMillis = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong maxTimeMillis = new AtomicLong(Long.MIN_VALUE);

  public OnHeapClusteredBaseTable(
      ClusteredValueGroupsBaseTableProjectionSpec spec,
      VirtualColumns segmentVirtualColumns,
      IncrementalIndex.InputRowHolder inputRowHolder,
      boolean rollup,
      int timePosition
  )
  {
    this.spec = spec;
    this.inputRowHolder = inputRowHolder;
    this.rollup = rollup;
    this.timePosition = timePosition;
    this.aggregatorFactories = spec.getMetrics() == null ? new AggregatorFactory[0] : spec.getMetrics();
    this.nonClusteringDimensions = Collections.unmodifiableList(
        new ArrayList<>(spec.getNonClusteringDimensions())
    );

    final RowSignature.Builder sigBuilder = RowSignature.builder();
    for (DimensionSchema c : spec.getClusteringColumns()) {
      sigBuilder.add(c.getName(), c.getColumnType());
    }
    this.clusteringColumns = sigBuilder.build();

    this.virtualColumns = mergeVirtualColumns(segmentVirtualColumns, spec.getVirtualColumns());
    this.virtualSelectorFactory = new OnheapIncrementalIndex.CachingColumnSelectorFactory(
        IncrementalIndex.makeColumnSelectorFactory(this.virtualColumns, inputRowHolder, null)
    );
  }

  /**
   * Resolve the clustering tuple for {@code row}, locate (or create) the matching {@link OnHeapClusterGroup}, and
   * dispatch the row to it. {@code key} carries the bucketed timestamp from the parent's {@code toIncrementalIndexRow}
   * pre-processing, its dim slots are ignored here since in clustered mode the non-clustering data lives only on the
   * per-group facts holders. Returns true when the row created a new fact entry in its group (vs. rolling up into an
   * existing one).
   */
  boolean addToFacts(
      IncrementalIndexRow key,
      InputRow row,
      List<String> parseExceptionMessages,
      AtomicLong totalSizeInBytes
  )
  {
    final Object[] clusteringValues = new Object[clusteringColumns.size()];
    final List<Integer> clusteringValueIds = new ArrayList<>(clusteringColumns.size());
    for (int i = 0; i < clusteringColumns.size(); i++) {
      final String name = clusteringColumns.getColumnName(i);
      final ColumnType type = clusteringColumns.getColumnType(i)
                                               .orElseThrow(() -> DruidException.defensive(
                                                   "clustering column [%s] has no type",
                                                   name
                                               ));
      final ColumnValueSelector<?> selector = virtualSelectorFactory.makeColumnValueSelector(name);
      final Object raw = selector.getObject();
      Object coerced;
      try {
        coerced = coerceClusteringValue(name, type, raw);
      }
      catch (ParseException pe) {
        parseExceptionMessages.add(pe.getMessage());
        coerced = null;
      }
      clusteringValues[i] = coerced;
      clusteringValueIds.add(internClusteringValue(type, coerced));
    }

    final OnHeapClusterGroup group = groups.computeIfAbsent(
        clusteringValueIds,
        ids -> new OnHeapClusterGroup(
            this,
            clusteringValues,
            ids,
            nonClusteringDimensions,
            aggregatorFactories,
            virtualColumns,
            inputRowHolder,
            rollup,
            timePosition
        )
    );
    final boolean isNewEntry = group.addToFacts(row, key.getTimestamp(), parseExceptionMessages, totalSizeInBytes);
    totalNumRows.incrementAndGet();
    minTimeMillis.accumulateAndGet(key.getTimestamp(), Math::min);
    maxTimeMillis.accumulateAndGet(key.getTimestamp(), Math::max);
    return isNewEntry;
  }

  /**
   * Minimum bucketed row timestamp across all groups. Throws {@link NoSuchElementException} when no rows
   * have been added.
   */
  public long getMinTimeMillis()
  {
    if (totalNumRows.get() == 0) {
      throw new NoSuchElementException("no rows");
    }
    return minTimeMillis.get();
  }

  /**
   * Maximum bucketed row timestamp across all groups. See {@link #getMinTimeMillis()} for empty-index behavior.
   */
  public long getMaxTimeMillis()
  {
    if (totalNumRows.get() == 0) {
      throw new NoSuchElementException("no rows");
    }
    return maxTimeMillis.get();
  }

  public IncrementalIndex.InputRowHolder getInputRowHolder()
  {
    return inputRowHolder;
  }

  public ClusteredValueGroupsBaseTableProjectionSpec getSpec()
  {
    return spec;
  }

  public RowSignature getClusteringColumns()
  {
    return clusteringColumns;
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  public Map<List<Integer>, OnHeapClusterGroup> getGroups()
  {
    return Collections.unmodifiableMap(groups);
  }

  /**
   * Look up the {@link OnHeapClusterGroup} whose clustering tuple matches {@code clusteringValues}. Returns null
   * when no group with that tuple has been materialized yet. Used by {@link IncrementalIndexAdapter} to resolve a
   * {@code TableClusterGroupSpec} (from {@link #toMetadataSchema()}) back to the in-memory group that produced it.
   */
  @Nullable
  public OnHeapClusterGroup getGroupForClusteringValues(Object[] clusteringValues)
  {
    for (OnHeapClusterGroup group : groups.values()) {
      if (Arrays.equals(clusteringValues, group.getClusteringValues())) {
        return group;
      }
    }
    return null;
  }

  public int numRows()
  {
    return totalNumRows.get();
  }

  @VisibleForTesting
  public List<String> getStringDictionary()
  {
    return insertionOrder(stringDictionary);
  }

  public List<Long> getLongDictionary()
  {
    return insertionOrder(longDictionary);
  }

  public List<Double> getDoubleDictionary()
  {
    return insertionOrder(doubleDictionary);
  }

  public List<Float> getFloatDictionary()
  {
    return insertionOrder(floatDictionary);
  }

  /**
   * Reconstruct a dictionary's values in insertion (id) order, including null at whatever slot it was first
   * observed, preserving the legacy {@code List}-backed accessor contract on top of {@link DimensionDictionary}.
   */
  private static <T extends Comparable<T>> List<T> insertionOrder(DimensionDictionary<T> dictionary)
  {
    final int size = dictionary.size();
    final List<T> values = new ArrayList<>(size);
    for (int id = 0; id < size; id++) {
      values.add(dictionary.getValue(id));
    }
    return Collections.unmodifiableList(values);
  }

  /**
   * Snapshot the current cluster-group state as a read-side {@link ClusteredValueGroupsBaseTableSchema}: per-type
   * dictionaries sort-nulls-first (with old insertion-order IDs remapped to the sorted positions), and each
   * {@link TableClusterGroupSpec} carries the remapped clustering-value-ID tuple plus the group's current row
   * count. Intended for use by {@link OnheapIncrementalIndex#getMetadata()} at persist time.
   */
  public ClusteredValueGroupsBaseTableSchema toMetadataSchema()
  {
    // Snapshot the groups BEFORE sorting the dictionaries. A group only exists after its clustering values were
    // interned (add happens-before the computeIfAbsent that creates the group), so every id referenced by a
    // captured group is already present when sort() runs below. Sorting first instead would let a group created
    // concurrently carry an id beyond the sorted dictionary, indexing out of bounds in the remap.
    final List<OnHeapClusterGroup> snapshotGroups = new ArrayList<>(groups.values());

    final SortedDimensionDictionary<String> stringSorted = stringDictionary.sort();
    final SortedDimensionDictionary<Long> longSorted = longDictionary.sort();
    final SortedDimensionDictionary<Double> doubleSorted = doubleDictionary.sort();
    final SortedDimensionDictionary<Float> floatSorted = floatDictionary.sort();

    // Size everything from the sorted views' own captured lengths, never by re-reading the live dictionary sizes —
    // a concurrent add between sort() and a size() read would otherwise make them disagree (index out of bounds).
    final int[] stringRemap = buildRemap(stringSorted);
    final int[] longRemap = buildRemap(longSorted);
    final int[] doubleRemap = buildRemap(doubleSorted);
    final int[] floatRemap = buildRemap(floatSorted);

    final ClusteringDictionaries sortedDicts = new ClusteringDictionaries(
        sortedValues(stringSorted),
        sortedValues(longSorted),
        sortedValues(doubleSorted),
        sortedValues(floatSorted)
    );

    final List<TableClusterGroupSpec> groupSpecs = new ArrayList<>(snapshotGroups.size());
    for (OnHeapClusterGroup group : snapshotGroups) {
      final List<Integer> oldIds = group.getClusteringValueIds();
      final List<Integer> newIds = new ArrayList<>(oldIds.size());
      for (int i = 0; i < oldIds.size(); i++) {
        final ColumnType type = clusteringColumns.getColumnType(i).orElseThrow();
        final int[] remap = remapForType(type, stringRemap, longRemap, doubleRemap, floatRemap);
        newIds.add(remap[oldIds.get(i)]);
      }
      groupSpecs.add(new TableClusterGroupSpec(newIds, group.numRows()));
    }
    // Emit groups in clustering-value sort order, matching the on-disk writer (IndexMergerV10) and the read-side
    // contract that {@link ClusteredValueGroupsBaseTableSchema#getClusterGroups()} is clustering-sorted. Because
    // the dictionaries above are sorted-nulls-first, lexicographic order over the remapped IDs equals clustering
    // value order — so back-to-back group walking yields rows in the full segment ordering.
    groupSpecs.sort(BY_CLUSTERING_VALUE_IDS);

    // Note: aggregator names are NOT included here — the schema's getColumnNames() appends them from the
    // aggregators array.
    final List<String> columnNames = new ArrayList<>();
    columnNames.addAll(clusteringColumns.getColumnNames());
    columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
    for (DimensionSchema d : nonClusteringDimensions) {
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(d.getName())) {
        columnNames.add(d.getName());
      }
    }

    return new ClusteredValueGroupsBaseTableSchema(
        spec.getVirtualColumns(),
        columnNames,
        aggregatorFactories,
        spec.getOrdering(),
        clusteringColumns,
        null,
        sortedDicts,
        groupSpecs
    );
  }


  /**
   * Build an old-id → new-sorted-id remap from a {@link DimensionDictionary#sort()} result, sized by the sorted
   * view's own captured length (both id spaces cover {@code [0, size())}).
   */
  private static int[] buildRemap(SortedDimensionDictionary<?> sorted)
  {
    final int size = sorted.size();
    final int[] remap = new int[size];
    for (int oldId = 0; oldId < size; oldId++) {
      remap[oldId] = sorted.getSortedIdFromUnsortedId(oldId);
    }
    return remap;
  }

  /**
   * Materialize a per-type dictionary's values in sorted-nulls-first order from {@link DimensionDictionary#sort()}.
   */
  private static <T extends Comparable<T>> List<T> sortedValues(SortedDimensionDictionary<T> sorted)
  {
    final int size = sorted.size();
    if (size == 0) {
      return List.of();
    }
    final List<T> values = new ArrayList<>(size);
    for (int sortedId = 0; sortedId < size; sortedId++) {
      values.add(sorted.getValueFromSortedId(sortedId));
    }
    return Collections.unmodifiableList(values);
  }

  private static int[] remapForType(
      ColumnType type,
      int[] stringRemap,
      int[] longRemap,
      int[] doubleRemap,
      int[] floatRemap
  )
  {
    if (type.is(ValueType.STRING)) {
      return stringRemap;
    }
    if (type.is(ValueType.LONG)) {
      return longRemap;
    }
    if (type.is(ValueType.DOUBLE)) {
      return doubleRemap;
    }
    if (type.is(ValueType.FLOAT)) {
      return floatRemap;
    }
    throw DruidException.defensive("unsupported clustering type [%s]", type);
  }

  /**
   * Intern an already-{@link #coerceClusteringValue coerced} clustering value into the segment-wide dictionary for
   * its type, returning the insertion-order id (existing id on repeat, null handled natively by
   * {@link DimensionDictionary}). The value's runtime class matches {@code type} by construction (the coercion step
   * produces String/Long/Double/Float or null).
   */
  private int internClusteringValue(ColumnType type, @Nullable Object value)
  {
    if (type.is(ValueType.STRING)) {
      return stringDictionary.add((String) value);
    }
    if (type.is(ValueType.LONG)) {
      return longDictionary.add((Long) value);
    }
    if (type.is(ValueType.DOUBLE)) {
      return doubleDictionary.add((Double) value);
    }
    if (type.is(ValueType.FLOAT)) {
      return floatDictionary.add((Float) value);
    }
    throw DruidException.defensive("unsupported clustering type [%s]", type);
  }

  /**
   * Coerce a raw clustering value (read uncoerced from the input row) to its declared clustering type. Bad input
   * data — a non-numeric string, or a non-scalar value, bound to a numeric clustering column — raises a recoverable
   * {@link ParseException} (collected per row by {@link #addToFacts}, subject to {@code maxParseExceptions}), NOT a
   * fatal defensive error. A genuinely unsupported clustering type (the type system should have rejected it upstream)
   * stays a defensive error.
   */
  @Nullable
  private static Object coerceClusteringValue(String columnName, ColumnType type, @Nullable Object raw)
  {
    if (raw == null) {
      return null;
    }
    if (type.is(ValueType.STRING)) {
      return raw instanceof String ? raw : raw.toString();
    }
    if (type.is(ValueType.LONG)) {
      if (raw instanceof Number) {
        return ((Number) raw).longValue();
      }
      if (raw instanceof String) {
        try {
          return Long.parseLong(((String) raw).trim());
        }
        catch (NumberFormatException e) {
          throw unparseableClusteringValue(columnName, type, raw);
        }
      }
      throw unparseableClusteringValue(columnName, type, raw);
    }
    if (type.is(ValueType.DOUBLE)) {
      if (raw instanceof Number) {
        return ((Number) raw).doubleValue();
      }
      if (raw instanceof String) {
        try {
          return Double.parseDouble(((String) raw).trim());
        }
        catch (NumberFormatException e) {
          throw unparseableClusteringValue(columnName, type, raw);
        }
      }
      throw unparseableClusteringValue(columnName, type, raw);
    }
    if (type.is(ValueType.FLOAT)) {
      if (raw instanceof Number) {
        return ((Number) raw).floatValue();
      }
      if (raw instanceof String) {
        try {
          return Float.parseFloat(((String) raw).trim());
        }
        catch (NumberFormatException e) {
          throw unparseableClusteringValue(columnName, type, raw);
        }
      }
      throw unparseableClusteringValue(columnName, type, raw);
    }
    throw DruidException.defensive(
        "unsupported clustering column type [%s] for column [%s]",
        type,
        columnName
    );
  }

  private static ParseException unparseableClusteringValue(String columnName, ColumnType type, Object raw)
  {
    return new ParseException(
        String.valueOf(raw),
        "Unparseable clustering value [%s] for column [%s] of type [%s]",
        raw,
        columnName,
        type
    );
  }

  private static VirtualColumns mergeVirtualColumns(
      @Nullable VirtualColumns segmentVcs,
      @Nullable VirtualColumns clusteringVcs
  )
  {
    if (clusteringVcs == null || clusteringVcs.isEmpty()) {
      return segmentVcs == null ? VirtualColumns.EMPTY : segmentVcs;
    }
    if (segmentVcs == null || segmentVcs.isEmpty()) {
      return clusteringVcs;
    }
    final List<VirtualColumn> merged = new ArrayList<>(
        segmentVcs.getVirtualColumns().length + clusteringVcs.getVirtualColumns().length
    );
    Collections.addAll(merged, segmentVcs.getVirtualColumns());
    Collections.addAll(merged, clusteringVcs.getVirtualColumns());
    return VirtualColumns.fromIterable(merged);
  }
}
