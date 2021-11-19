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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Striped;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRow;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StringDimensionIndexer extends DictionaryEncodedColumnIndexer<int[], String>
{
  @VisibleForTesting
  public static final String BITMAP_INDEX_DISABLED_IN_SCHEMA_ERR_MSG = "This column disabled bitmap indexes in schema";

  @VisibleForTesting
  public static final String IN_MEMORY_BITMAP_INDEX_DISABLED_ERR_MSG = "This column disabled in-memory bitmap indexes";

  @Nullable
  private static String emptyToNullIfNeeded(@Nullable Object o)
  {
    return o != null ? NullHandling.emptyToNullIfNeeded(o.toString()) : null;
  }

  /**
   * Number of small locks to replace a single giant lock to reduce lock contention
   */
  private static final int IN_MEMORY_BITMAP_STRIPED_LOCK_NUM_STRIPES = 64;

  private final MultiValueHandling multiValueHandling;
  private final boolean hasBitmapIndexes;
  private final boolean hasSpatialIndexes;
  private volatile boolean hasMultipleValues = false;

  /**
   * Whether in-memory bitmap is enabled for this dimension
   */
  private final boolean enableInMemoryBitmap;
  /**
   * The kind of bitmap to create for in-memory bitmap
   */
  @Nullable
  private final BitmapFactory inMemoryBitmapFactory;
  /**
   * A list of in-memory bitmaps for each distinct value of this dimension
   */
  @Nullable
  private final List<MutableBitmap> inMemoryBitmaps;
  /**
   * Read-write lock for {@link #inMemoryBitmaps}. Synchronization is needed because the list can be read or mutated in
   * {@link #fillInMemoryBitmapsFromUnsortedEncodedKeyComponent} by one thread and read in {@link #getBitmap} by another
   * thread concurrently. Since both read and write access patterns exist, use a read-write lock.
   */
  @Nullable
  private final ReentrantReadWriteLock inMemoryBitmapsLock;
  /**
   * Exclusive lock for individual element in {@link #inMemoryBitmaps}. Synchronization is needed because each mutable
   * bitmap in the list can be mutated in {@link #fillInMemoryBitmapsFromUnsortedEncodedKeyComponent} by one thread and
   * mutated in {@link #getBitmap} by another thread concurrently. Since only write access pattern exists, use an
   * exclusive lock. Use {@link Striped}: multiple small locks instead of a giant lock to reduce lock contention: the
   * cardinality of distinct values of this dimension can be high so the size of in-memory bitmap list can be large so
   * we want finer granular lock control.
   */
  @Nullable
  private final Striped<Lock> inMemoryBitmapStripedLock;

  public StringDimensionIndexer(MultiValueHandling multiValueHandling, boolean hasBitmapIndexes,
                                boolean hasSpatialIndexes, boolean enableInMemoryBitmap)
  {
    this.multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;
    this.hasBitmapIndexes = hasBitmapIndexes;
    this.hasSpatialIndexes = hasSpatialIndexes;

    if (enableInMemoryBitmap) {
      this.enableInMemoryBitmap = enableInMemoryBitmap;
      this.inMemoryBitmaps = new ArrayList<>();
      this.inMemoryBitmapFactory = new RoaringBitmapFactory();
      this.inMemoryBitmapsLock = new ReentrantReadWriteLock();
      this.inMemoryBitmapStripedLock = Striped.lock(IN_MEMORY_BITMAP_STRIPED_LOCK_NUM_STRIPES);
    } else {
      this.enableInMemoryBitmap = false;
      this.inMemoryBitmaps = null;
      this.inMemoryBitmapFactory = null;
      this.inMemoryBitmapsLock = null;
      this.inMemoryBitmapStripedLock = null;
    }
  }

  @Override
  public int[] processRowValsToUnsortedEncodedKeyComponent(@Nullable Object dimValues, boolean reportParseExceptions)
  {
    final int[] encodedDimensionValues;
    final int oldDictSize = dimLookup.size();

    if (dimValues == null) {
      final int nullId = dimLookup.getId(null);
      encodedDimensionValues = nullId == DimensionDictionary.ABSENT_VALUE_ID ? new int[]{dimLookup.add(null)} : new int[]{nullId};
    } else if (dimValues instanceof List) {
      List<Object> dimValuesList = (List<Object>) dimValues;
      if (dimValuesList.isEmpty()) {
        dimLookup.add(null);
        encodedDimensionValues = IntArrays.EMPTY_ARRAY;
      } else if (dimValuesList.size() == 1) {
        encodedDimensionValues = new int[]{dimLookup.add(emptyToNullIfNeeded(dimValuesList.get(0)))};
      } else {
        hasMultipleValues = true;
        final String[] dimensionValues = new String[dimValuesList.size()];
        for (int i = 0; i < dimValuesList.size(); i++) {
          dimensionValues[i] = emptyToNullIfNeeded(dimValuesList.get(i));
        }
        if (multiValueHandling.needSorting()) {
          // Sort multival row by their unencoded values first.
          Arrays.sort(dimensionValues, Comparators.naturalNullsFirst());
        }

        final int[] retVal = new int[dimensionValues.length];

        int prevId = -1;
        int pos = 0;
        for (String dimensionValue : dimensionValues) {
          if (multiValueHandling != MultiValueHandling.SORTED_SET) {
            retVal[pos++] = dimLookup.add(dimensionValue);
            continue;
          }
          int index = dimLookup.add(dimensionValue);
          if (index != prevId) {
            prevId = retVal[pos++] = index;
          }
        }

        encodedDimensionValues = pos == retVal.length ? retVal : Arrays.copyOf(retVal, pos);
      }
    } else {
      encodedDimensionValues = new int[]{dimLookup.add(emptyToNullIfNeeded(dimValues))};
    }

    // If dictionary size has changed, the sorted lookup is no longer valid.
    if (oldDictSize != dimLookup.size()) {
      sortedLookup = null;
    }

    return encodedDimensionValues;
  }

  @Override
  public long estimateEncodedKeyComponentSize(int[] key)
  {
    // string length is being accounted for each time they are referenced, based on dimension handler interface,
    // even though they are stored just once. It may overestimate the size by a bit, but we wanted to leave
    // more buffer to be safe
    long estimatedSize = key.length * Integer.BYTES;
    for (int element : key) {
      String val = dimLookup.getValue(element);
      if (val != null) {
        // According to https://www.ibm.com/developerworks/java/library/j-codetoheap/index.html
        // String has the following memory usuage...
        // 28 bytes of data for String metadata (class pointer, flags, locks, hash, count, offset, reference to char array)
        // 16 bytes of data for the char array metadata (class pointer, flags, locks, size)
        // 2 bytes for every letter of the string
        int sizeOfString = 28 + 16 + (2 * val.length());
        estimatedSize += sizeOfString;
      }
    }
    return estimatedSize;
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(int[] lhs, int[] rhs)
  {
    int lhsLen = lhs.length;
    int rhsLen = rhs.length;

    int lenCompareResult = Ints.compare(lhsLen, rhsLen);
    if (lenCompareResult != 0) {
      // if the values don't have the same length, check if we're comparing [] and [null], which are equivalent
      if (lhsLen + rhsLen == 1) {
        int[] longerVal = rhsLen > lhsLen ? rhs : lhs;
        if (longerVal[0] == dimLookup.getIdForNull()) {
          return 0;
        } else {
          //noinspection ArrayEquality -- longerVal is explicitly set to only lhs or rhs
          return longerVal == lhs ? 1 : -1;
        }
      }
    }

    int valsIndex = 0;
    int lenToCompare = Math.min(lhsLen, rhsLen);
    while (valsIndex < lenToCompare) {
      int lhsVal = lhs[valsIndex];
      int rhsVal = rhs[valsIndex];
      if (lhsVal != rhsVal) {
        final String lhsValActual = getActualValue(lhsVal, false);
        final String rhsValActual = getActualValue(rhsVal, false);
        int valueCompareResult = 0;
        if (lhsValActual != null && rhsValActual != null) {
          valueCompareResult = lhsValActual.compareTo(rhsValActual);
        } else if (lhsValActual == null ^ rhsValActual == null) {
          valueCompareResult = lhsValActual == null ? -1 : 1;
        }
        if (valueCompareResult != 0) {
          return valueCompareResult;
        }
      }
      ++valsIndex;
    }

    return lenCompareResult;
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(int[] lhs, int[] rhs)
  {
    return Arrays.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(int[] key)
  {
    return Arrays.hashCode(key);
  }

  @Override
  public ColumnCapabilities getColumnCapabilities()
  {
    ColumnCapabilitiesImpl capabilites = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                     .setHasBitmapIndexes(hasBitmapIndexes)
                                                                     .setHasSpatialIndexes(hasSpatialIndexes)
                                                                     .setDictionaryValuesUnique(true)
                                                                     .setDictionaryValuesSorted(false);

    // Strings are opportunistically multi-valued, but the capabilities are initialized as 'unknown', since a
    // multi-valued row might be processed at any point during ingestion.
    // We only explicitly set multiple values if we are certain that there are multiple values, otherwise, a race
    // condition might occur where this indexer might process a multi-valued row in the period between obtaining the
    // capabilities, and actually processing the rows with a selector. Leaving as unknown allows the caller to decide
    // how to handle this.
    if (hasMultipleValues) {
      capabilites.setHasMultipleValues(true);
    }
    // Likewise, only set dictionaryEncoded if explicitly if true for a similar reason as multi-valued handling. The
    // dictionary is populated as rows are processed, but there might be implicit default values not accounted for in
    // the dictionary yet. We can be certain that the dictionary has an entry for every value if either of
    //    a) we have already processed an explitic default (null) valued row for this column
    //    b) the processing was not 'sparse', meaning that this indexer has processed an explict value for every row
    // is true.
    final boolean allValuesEncoded = dictionaryEncodesAllValues();
    if (allValuesEncoded) {
      capabilites.setDictionaryEncoded(true);
    }

    if (isSparse || dimLookup.getIdForNull() != DimensionDictionary.ABSENT_VALUE_ID) {
      capabilites.setHasNulls(true);
    }
    return capabilites;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      final DimensionSpec spec,
      final IncrementalIndexRowHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final ExtractionFn extractionFn = spec.getExtractionFn();

    final int dimIndex = desc.getIndex();

    // maxId is used in concert with getLastRowIndex() in IncrementalIndex to ensure that callers do not encounter
    // rows that contain IDs over the initially-reported cardinality. The main idea is that IncrementalIndex establishes
    // a watermark at the time a cursor is created, and doesn't allow the cursor to walk past that watermark.
    //
    // Additionally, this selector explicitly blocks knowledge of IDs past maxId that may occur from other causes
    // (for example: nulls getting generated for empty arrays, or calls to lookupId).
    final int maxId = getCardinality();

    class IndexerDimensionSelector implements DimensionSelector, IdLookup
    {
      private final ArrayBasedIndexedInts indexedInts = new ArrayBasedIndexedInts();

      @Nullable
      @MonotonicNonNull
      private int[] nullIdIntArray;

      @Override
      public IndexedInts getRow()
      {
        final Object[] dims = currEntry.get().getDims();

        int[] indices;
        if (dimIndex < dims.length) {
          indices = (int[]) dims[dimIndex];
        } else {
          indices = null;
        }

        int[] row = null;
        int rowSize = 0;

        // usually due to currEntry's rowIndex is smaller than the row's rowIndex in which this dim first appears
        if (indices == null || indices.length == 0) {
          if (hasMultipleValues) {
            row = IntArrays.EMPTY_ARRAY;
            rowSize = 0;
          } else {
            final int nullId = getEncodedValue(null, false);
            if (nullId >= 0 && nullId < maxId) {
              // null was added to the dictionary before this selector was created; return its ID.
              if (nullIdIntArray == null) {
                nullIdIntArray = new int[]{nullId};
              }
              row = nullIdIntArray;
              rowSize = 1;
            } else {
              // null doesn't exist in the dictionary; return an empty array.
              // Choose to use ArrayBasedIndexedInts later, instead of special "empty" IndexedInts, for monomorphism
              row = IntArrays.EMPTY_ARRAY;
              rowSize = 0;
            }
          }
        }

        if (row == null && indices != null && indices.length > 0) {
          row = indices;
          rowSize = indices.length;
        }

        indexedInts.setValues(row, rowSize);
        return indexedInts;
      }

      @Override
      public ValueMatcher makeValueMatcher(final String value)
      {
        if (extractionFn == null) {
          final int valueId = lookupId(value);
          if (valueId >= 0 || value == null) {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                Object[] dims = currEntry.get().getDims();
                if (dimIndex >= dims.length) {
                  return value == null;
                }

                int[] dimsInt = (int[]) dims[dimIndex];
                if (dimsInt == null || dimsInt.length == 0) {
                  return value == null;
                }

                for (int id : dimsInt) {
                  if (id == valueId) {
                    return true;
                  }
                }
                return false;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                // nothing to inspect
              }
            };
          } else {
            return BooleanValueMatcher.of(false);
          }
        } else {
          // Employ caching BitSet optimization
          return makeValueMatcher(Predicates.equalTo(value));
        }
      }

      @Override
      public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
      {
        final BitSet checkedIds = new BitSet(maxId);
        final BitSet matchingIds = new BitSet(maxId);
        final boolean matchNull = predicate.apply(null);

        // Lazy matcher; only check an id if matches() is called.
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            Object[] dims = currEntry.get().getDims();
            if (dimIndex >= dims.length) {
              return matchNull;
            }

            int[] dimsInt = (int[]) dims[dimIndex];
            if (dimsInt == null || dimsInt.length == 0) {
              return matchNull;
            }

            for (int id : dimsInt) {
              if (checkedIds.get(id)) {
                if (matchingIds.get(id)) {
                  return true;
                }
              } else {
                final boolean matches = predicate.apply(lookupName(id));
                checkedIds.set(id);
                if (matches) {
                  matchingIds.set(id);
                  return true;
                }
              }
            }
            return false;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            // nothing to inspect
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return maxId;
      }

      @Override
      public String lookupName(int id)
      {
        if (id >= maxId) {
          // Sanity check; IDs beyond maxId should not be known to callers. (See comment above.)
          throw new ISE("id[%d] >= maxId[%d]", id, maxId);
        }
        final String strValue = getActualValue(id, false);
        return extractionFn == null ? strValue : extractionFn.apply(strValue);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return dictionaryEncodesAllValues();
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return extractionFn == null ? this : null;
      }

      @Override
      public int lookupId(String name)
      {
        if (extractionFn != null) {
          throw new UnsupportedOperationException(
              "cannot perform lookup when applying an extraction function"
          );
        }

        final int id = getEncodedValue(name, false);

        if (id < maxId) {
          return id;
        } else {
          // Can happen if a value was added to our dimLookup after this selector was created. Act like it
          // doesn't exist.
          return DimensionDictionary.ABSENT_VALUE_ID;
        }
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Object getObject()
      {
        IncrementalIndexRow key = currEntry.get();
        if (key == null) {
          return null;
        }

        Object[] dims = key.getDims();
        if (dimIndex >= dims.length) {
          return null;
        }

        return convertUnsortedEncodedKeyComponentToActualList((int[]) dims[dimIndex]);
      }

      @SuppressWarnings("deprecation")
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }
    return new IndexerDimensionSelector();
  }


  @Nullable
  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(int[] key)
  {
    if (key == null || key.length == 0) {
      return null;
    }
    if (key.length == 1) {
      return getActualValue(key[0], false);
    } else {
      String[] rowArray = new String[key.length];
      for (int i = 0; i < key.length; i++) {
        String val = getActualValue(key[i], false);
        rowArray[i] = NullHandling.nullToEmptyIfNeeded(val);
      }
      return Arrays.asList(rowArray);
    }
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      int[] key,
      int rowNum,
      MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    if (!hasBitmapIndexes) {
      throw new UnsupportedOperationException("This column does not include bitmap indexes");
    }

    for (int dimValIdx : key) {
      if (bitmapIndexes[dimValIdx] == null) {
        bitmapIndexes[dimValIdx] = factory.makeEmptyMutableBitmap();
      }
      bitmapIndexes[dimValIdx].add(rowNum);
    }
  }

  /**
   * Update in-memory bitmaps to associate the distinct values in `key` with the `rowNum` containing them.
   * Similar to {@link DimensionIndexer#fillBitmapsFromUnsortedEncodedKeyComponent} which is used during incremental
   * index segment finalization but this function is used during real-time writes to incremental index before segment
   * finalization happens therefore it contains extra locking.
   *
   * Thread safety: this method is called from
   * {@link org.apache.druid.segment.incremental.OnheapIncrementalIndex#addToFacts} which is an implementation of
   * {@link IncrementalIndex#addToFacts} whose documentation indicates its thread safety. As a result, this method is
   * made thread safe as well.
   *
   * @param key dimension value array from a Row key
   * @param rowNum current row number
   * @param factory bitmap factory
   */
  public void fillInMemoryBitmapsFromUnsortedEncodedKeyComponent(
      int[] key,
      int rowNum,
      BitmapFactory factory
  )
  {
    if (!hasBitmapIndexes) {
      throw new UnsupportedOperationException(BITMAP_INDEX_DISABLED_IN_SCHEMA_ERR_MSG);
    } else if (!enableInMemoryBitmap) {
      throw new UnsupportedOperationException(IN_MEMORY_BITMAP_INDEX_DISABLED_ERR_MSG);
    }

    int maxIndex = Arrays.stream(key)
                         .max()
                         .getAsInt();
    List<MutableBitmap> bitmapsToUpdate = new ArrayList<>();

    boolean newValueEncountered = false;
    inMemoryBitmapsLock.readLock().lock();
    try {
      // Check if we need to expand bitmap list to insert values not seen before
      if (inMemoryBitmaps.size() <= maxIndex) {
        newValueEncountered = true;
      } else {
        // Get bitmaps to update
        for (int dimValIdx : key) {
          bitmapsToUpdate.add(inMemoryBitmaps.get(dimValIdx));
        }
      }
    }
    finally {
      inMemoryBitmapsLock.readLock().unlock();
    }

    if (newValueEncountered) {
      // Acquire a write lock while expanding the list
      inMemoryBitmapsLock.writeLock().lock();
      try {
        while (inMemoryBitmaps.size() <= maxIndex) {
          inMemoryBitmaps.add(factory.makeEmptyMutableBitmap());
        }
        // Get bitmaps to update
        for (int dimValIdx : key) {
          bitmapsToUpdate.add(inMemoryBitmaps.get(dimValIdx));
        }
      }
      finally {
        inMemoryBitmapsLock.writeLock().unlock();
      }
    }

    // Update indivudal bitmaps
    for (int i = 0; i < key.length; i++) {
      int dimValIdx = key[i];
      MutableBitmap b = bitmapsToUpdate.get(i);

      // Acquire the exclusive lock before changing the mutable bitmap
      Lock lock = inMemoryBitmapStripedLock.getAt(lockIndex(dimValIdx));
      lock.lock();
      try {
        b.add(rowNum);
      }
      finally {
        lock.unlock();
      }
    }
  }

  public String getValue(int index)
  {
    return dimLookup.getValue(index);
  }

  public boolean hasNulls()
  {
    return dimLookup.getIdForNull() != DimensionDictionary.ABSENT_VALUE_ID;
  }

  public int getIndex(@Nullable String value)
  {
    return dimLookup.getId(value);
  }

  /**
   * Get an immutable version of the bitmap of the given `idx`
   *
   * @param idx The index of the bitmap to get
   * @return An immutable version of the bitmap
   */
  public ImmutableBitmap getBitmap(int idx)
  {
    if (!hasBitmapIndexes) {
      throw new UnsupportedOperationException(BITMAP_INDEX_DISABLED_IN_SCHEMA_ERR_MSG);
    } else if (!enableInMemoryBitmap) {
      throw new UnsupportedOperationException(IN_MEMORY_BITMAP_INDEX_DISABLED_ERR_MSG);
    }

    // Step 1: use a read lock to prevent inMemoryBitmaps from being adding or removing elements while we get element
    // from it
    inMemoryBitmapsLock.readLock().lock();
    MutableBitmap mutableBitmapToClone;
    try {
      mutableBitmapToClone =
          (idx < 0 || idx >= inMemoryBitmaps.size()) ? null : inMemoryBitmaps.get(idx);
    }
    finally {
      inMemoryBitmapsLock.readLock().unlock();
    }

    // Step 2 clone the mutable bitmap to an immutable bitmap
    // Note that it's possible more writes have happened so the mutable bitmap to clone have been mutated in
    // {@link #fillInMemoryBitmapsFromUnsortedEncodedKeyComponent} between step 1 and step 2, but it's OK because it
    // just means we end up with more records to filter on.
    if (mutableBitmapToClone == null) {
      return inMemoryBitmapFactory.makeEmptyImmutableBitmap();
    } else {
      // Acquire the exclusive lock of the mutable bitmap before trying converting it to an immutable bitmap: if the
      // underlying mutable bitmap has an internal buffer, a flush will be triggered during clone to get an immutable
      // copy which is a write operation, use an exclusive lock here
      Lock lock = inMemoryBitmapStripedLock.getAt(lockIndex(idx));
      lock.lock();
      try {
        return inMemoryBitmapFactory.makeImmutableBitmap(mutableBitmapToClone);
      }
      finally {
        lock.unlock();
      }
    }
  }

  private static int lockIndex(final int position)
  {
    return smear(position) % IN_MEMORY_BITMAP_STRIPED_LOCK_NUM_STRIPES;
  }

  /**
   * see https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/Striped.java#L536-L548
   *
   * @param hashCode
   *
   * @return smeared hashCode
   */
  private static int smear(int hashCode)
  {
    hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
    return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
  }
}
