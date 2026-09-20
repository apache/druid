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

package org.apache.druid.segment.vector;

import org.apache.druid.error.NotYetImplemented;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.DeferExpressionDimensions;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Wraps a {@link VectorColumnSelectorFactory} whose underlying offset iterates batches in descending order (see
 * {@link DescendingNoFilterVectorOffset} and {@link DescendingBitmapVectorOffset}). The underlying offset generates
 * batches in descending order (from the end of the segment to the start), but each batch is internally in ascending
 * order. This class reverses each batch internally, to provide an overall descending order.
 *
 * <p>Reversing is done here (on already-decoded vectors) so that low-level column readers only ever need to return
 * batches in internally-ascending order. This allows them to be simpler, as they do not need to have handling for
 * both ascending and descending order.
 */
public class ReverseVectorColumnSelectorFactory implements VectorColumnSelectorFactory
{
  private final VectorColumnSelectorFactory delegate;
  private final ReadableVectorInspector inspector;

  private final Map<DimensionSpec, SingleValueDimensionVectorSelector> singleValueDimensionSelectorCache = new HashMap<>();
  private final Map<DimensionSpec, MultiValueDimensionVectorSelector> multiValueDimensionSelectorCache = new HashMap<>();
  private final Map<String, VectorValueSelector> valueSelectorCache = new HashMap<>();
  private final Map<String, VectorObjectSelector> objectSelectorCache = new HashMap<>();

  public ReverseVectorColumnSelectorFactory(final VectorColumnSelectorFactory delegate)
  {
    this.delegate = delegate;
    this.inspector = delegate.getReadableVectorInspector();
  }

  @Override
  public ReadableVectorInspector getReadableVectorInspector()
  {
    return inspector;
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(final DimensionSpec dimensionSpec)
  {
    return singleValueDimensionSelectorCache.computeIfAbsent(
        dimensionSpec,
        spec -> new ReverseSingleValueDimensionVectorSelector(
            delegate.makeSingleValueDimensionSelector(spec),
            inspector
        )
    );
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(final DimensionSpec dimensionSpec)
  {
    return multiValueDimensionSelectorCache.computeIfAbsent(
        dimensionSpec,
        spec -> new ReverseMultiValueDimensionVectorSelector(delegate.makeMultiValueDimensionSelector(spec), inspector)
    );
  }

  @Override
  public VectorValueSelector makeValueSelector(final String column)
  {
    return valueSelectorCache.computeIfAbsent(
        column,
        c -> new ReverseVectorValueSelector(delegate.makeValueSelector(c), inspector)
    );
  }

  @Override
  public VectorObjectSelector makeObjectSelector(final String column)
  {
    return objectSelectorCache.computeIfAbsent(
        column,
        c -> new ReverseVectorObjectSelector(delegate.makeObjectSelector(c), inspector)
    );
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String column)
  {
    return delegate.getColumnCapabilities(column);
  }

  /**
   * Reverse n items of src into dst.
   */
  private static void reverseInto(final long[] src, final long[] dst, final int n)
  {
    for (int i = 0; i < n; i++) {
      dst[i] = src[n - 1 - i];
    }
  }

  /**
   * Reverse n items of src into dst.
   */
  private static void reverseInto(final float[] src, final float[] dst, final int n)
  {
    for (int i = 0; i < n; i++) {
      dst[i] = src[n - 1 - i];
    }
  }

  /**
   * Reverse n items of src into dst.
   */
  private static void reverseInto(final double[] src, final double[] dst, final int n)
  {
    for (int i = 0; i < n; i++) {
      dst[i] = src[n - 1 - i];
    }
  }

  /**
   * Reverse n items of src into dst.
   */
  private static void reverseInto(final boolean[] src, final boolean[] dst, final int n)
  {
    for (int i = 0; i < n; i++) {
      dst[i] = src[n - 1 - i];
    }
  }

  /**
   * Reverse n items of src into dst.
   */
  private static void reverseInto(final int[] src, final int[] dst, final int n)
  {
    for (int i = 0; i < n; i++) {
      dst[i] = src[n - 1 - i];
    }
  }

  /**
   * Reverse n items of src into dst.
   */
  private static void reverseInto(final Object[] src, final Object[] dst, final int n)
  {
    for (int i = 0; i < n; i++) {
      dst[i] = src[n - 1 - i];
    }
  }

  /**
   * Base class for the reversing selectors. Each one reverses the valid entries of a freshly-decoded vector into a
   * scratch array, caching by vector id so that a given batch is only reversed once.
   */
  private abstract static class ReverseVectorSelector implements VectorSizeInspector
  {
    final ReadableVectorInspector inspector;

    ReverseVectorSelector(final ReadableVectorInspector inspector)
    {
      this.inspector = inspector;
    }

    @Override
    public int getMaxVectorSize()
    {
      return inspector.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return inspector.getCurrentVectorSize();
    }
  }

  /**
   * Base class for the reversing dimension selectors, which delegate all the dictionary lookups.
   */
  private abstract static class ReverseDimensionVectorSelector extends ReverseVectorSelector
      implements DimensionDictionarySelector
  {
    private final DimensionDictionarySelector delegate;

    ReverseDimensionVectorSelector(final DimensionDictionarySelector delegate, final ReadableVectorInspector inspector)
    {
      super(inspector);
      this.delegate = delegate;
    }

    @Override
    public int getValueCardinality()
    {
      return delegate.getValueCardinality();
    }

    @Nullable
    @Override
    public String lookupName(final int id)
    {
      return delegate.lookupName(id);
    }

    @Nullable
    @Override
    public ByteBuffer lookupNameUtf8(final int id)
    {
      return delegate.lookupNameUtf8(id);
    }

    @Override
    public boolean supportsLookupNameUtf8()
    {
      return delegate.supportsLookupNameUtf8();
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return delegate.nameLookupPossibleInAdvance();
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return delegate.idLookup();
    }
  }

  private static class ReverseVectorValueSelector extends ReverseVectorSelector implements VectorValueSelector
  {
    private final VectorValueSelector delegate;

    @Nullable
    private long[] longs;
    @Nullable
    private float[] floats;
    @Nullable
    private double[] doubles;
    @Nullable
    private boolean[] nulls;

    private int longId = ReadableVectorInspector.NULL_ID;
    private int floatId = ReadableVectorInspector.NULL_ID;
    private int doubleId = ReadableVectorInspector.NULL_ID;
    private int nullId = ReadableVectorInspector.NULL_ID;

    // Null when the current batch has no nulls at all, in which case getNullVector returns null.
    @Nullable
    private boolean[] currentNulls;

    ReverseVectorValueSelector(final VectorValueSelector delegate, final ReadableVectorInspector inspector)
    {
      super(inspector);
      this.delegate = delegate;
    }

    @Override
    public long[] getLongVector()
    {
      if (longs == null) {
        longs = new long[inspector.getMaxVectorSize()];
      }
      if (longId != inspector.getId()) {
        reverseInto(delegate.getLongVector(), longs, inspector.getCurrentVectorSize());
        longId = inspector.getId();
      }
      return longs;
    }

    @Override
    public float[] getFloatVector()
    {
      if (floats == null) {
        floats = new float[inspector.getMaxVectorSize()];
      }
      if (floatId != inspector.getId()) {
        reverseInto(delegate.getFloatVector(), floats, inspector.getCurrentVectorSize());
        floatId = inspector.getId();
      }
      return floats;
    }

    @Override
    public double[] getDoubleVector()
    {
      if (doubles == null) {
        doubles = new double[inspector.getMaxVectorSize()];
      }
      if (doubleId != inspector.getId()) {
        reverseInto(delegate.getDoubleVector(), doubles, inspector.getCurrentVectorSize());
        doubleId = inspector.getId();
      }
      return doubles;
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      if (nullId != inspector.getId()) {
        final boolean[] src = delegate.getNullVector();
        if (src == null) {
          currentNulls = null;
        } else {
          if (nulls == null) {
            nulls = new boolean[inspector.getMaxVectorSize()];
          }
          reverseInto(src, nulls, inspector.getCurrentVectorSize());
          currentNulls = nulls;
        }
        nullId = inspector.getId();
      }
      return currentNulls;
    }
  }

  @Override
  public GroupByVectorColumnSelector makeGroupByVectorColumnSelector(
      final String column,
      final DeferExpressionDimensions deferExpressionDimensions
  )
  {
    // groupBy does not use descending cursors, so this method is not needed.
    throw NotYetImplemented.ex(null, "makeGroupByVectorColumnSelector is not needed for descending cursors");
  }

  private static class ReverseVectorObjectSelector extends ReverseVectorSelector implements VectorObjectSelector
  {
    private final VectorObjectSelector delegate;
    private final Object[] objects;

    private int id = ReadableVectorInspector.NULL_ID;

    ReverseVectorObjectSelector(final VectorObjectSelector delegate, final ReadableVectorInspector inspector)
    {
      super(inspector);
      this.delegate = delegate;
      this.objects = new Object[inspector.getMaxVectorSize()];
    }

    @Override
    public Object[] getObjectVector()
    {
      if (id != inspector.getId()) {
        reverseInto(delegate.getObjectVector(), objects, inspector.getCurrentVectorSize());
        id = inspector.getId();
      }
      return objects;
    }
  }

  private static class ReverseSingleValueDimensionVectorSelector extends ReverseDimensionVectorSelector
      implements SingleValueDimensionVectorSelector
  {
    private final SingleValueDimensionVectorSelector delegate;
    private final int[] rows;

    private int id = ReadableVectorInspector.NULL_ID;

    ReverseSingleValueDimensionVectorSelector(
        final SingleValueDimensionVectorSelector delegate,
        final ReadableVectorInspector inspector
    )
    {
      super(delegate, inspector);
      this.delegate = delegate;
      this.rows = new int[inspector.getMaxVectorSize()];
    }

    @Override
    public int[] getRowVector()
    {
      if (id != inspector.getId()) {
        reverseInto(delegate.getRowVector(), rows, inspector.getCurrentVectorSize());
        id = inspector.getId();
      }
      return rows;
    }
  }

  private static class ReverseMultiValueDimensionVectorSelector extends ReverseDimensionVectorSelector
      implements MultiValueDimensionVectorSelector
  {
    private final MultiValueDimensionVectorSelector delegate;
    private final IndexedInts[] rows;

    private int id = ReadableVectorInspector.NULL_ID;

    ReverseMultiValueDimensionVectorSelector(
        final MultiValueDimensionVectorSelector delegate,
        final ReadableVectorInspector inspector
    )
    {
      super(delegate, inspector);
      this.delegate = delegate;
      this.rows = new IndexedInts[inspector.getMaxVectorSize()];
    }

    @Override
    public IndexedInts[] getRowVector()
    {
      if (id != inspector.getId()) {
        reverseInto(delegate.getRowVector(), rows, inspector.getCurrentVectorSize());
        id = inspector.getId();
      }
      return rows;
    }
  }
}
