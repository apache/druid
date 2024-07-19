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

package org.apache.druid.segment.join.table;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.ints.IntSortedSets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ConstantDimensionSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntFunction;

public class IndexedTableJoinMatcherTest
{
  private static final int SIZE = 3;

  public static class ConditionMatcherFactoryTest
  {
    public static class MakeLongProcessorTest extends InitializedNullHandlingTest
    {
      @Mock
      private BaseLongColumnValueSelector selector;
      private AutoCloseable mocks;

      @Before
      public void setUp()
      {
        mocks = MockitoAnnotations.openMocks(this);

        if (NullHandling.sqlCompatible()) {
          Mockito.doReturn(false).when(selector).isNull();
        }

        Mockito.doReturn(1L).when(selector).getLong();
      }

      @After
      public void tearDown() throws Exception
      {
        mocks.close();
      }

      @Test
      public void testMatchToUniqueLongIndex()
      {
        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(longPlusOneIndex(), false);
        final IndexedTableJoinMatcher.ConditionMatcher processor = conditionMatcherFactory.makeLongProcessor(selector);

        Assert.assertEquals(ImmutableList.of(2), ImmutableList.copyOf(processor.match()));
      }

      @Test
      public void testMatchSingleRowToUniqueLongIndex()
      {
        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(longPlusOneIndex(), false);
        final IndexedTableJoinMatcher.ConditionMatcher processor = conditionMatcherFactory.makeLongProcessor(selector);

        Assert.assertEquals(2, processor.matchSingleRow());
      }

      @Test
      public void testMatchToNonUniqueLongIndex()
      {
        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(longAlwaysOneTwoThreeIndex(), false);
        final IndexedTableJoinMatcher.ConditionMatcher processor = conditionMatcherFactory.makeLongProcessor(selector);

        Assert.assertEquals(ImmutableList.of(1, 2, 3), ImmutableList.copyOf(processor.match()));
      }

      @Test
      public void testMatchSingleRowToNonUniqueLongIndex()
      {
        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(longAlwaysOneTwoThreeIndex(), false);
        final IndexedTableJoinMatcher.ConditionMatcher processor = conditionMatcherFactory.makeLongProcessor(selector);

        Assert.assertThrows(UnsupportedOperationException.class, processor::matchSingleRow);
      }

      @Test
      public void testMatchToUniqueStringIndex()
      {
        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(certainStringToThreeIndex(), false);
        final IndexedTableJoinMatcher.ConditionMatcher processor = conditionMatcherFactory.makeLongProcessor(selector);

        Assert.assertEquals(ImmutableList.of(3), ImmutableList.copyOf(processor.match()));
      }

      @Test
      public void testMatchSingleRowToUniqueStringIndex()
      {
        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(certainStringToThreeIndex(), false);
        final IndexedTableJoinMatcher.ConditionMatcher processor = conditionMatcherFactory.makeLongProcessor(selector);

        Assert.assertEquals(3, processor.matchSingleRow());
      }
    }

    public static class MakeComplexProcessorTest extends InitializedNullHandlingTest
    {
      @Rule
      public ExpectedException expectedException = ExpectedException.none();

      @Mock
      private BaseObjectColumnValueSelector<?> selector;
      private AutoCloseable mocks;

      @Before
      public void setUp()
      {
        mocks = MockitoAnnotations.openMocks(this);
      }

      @After
      public void tearDown() throws Exception
      {
        mocks.close();
      }

      @Test
      public void testMatch()
      {
        final IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(longPlusOneIndex(), false);

        final IndexedTableJoinMatcher.ConditionMatcher processor =
            conditionMatcherFactory.makeComplexProcessor(selector);

        Assert.assertEquals(ImmutableList.of(), ImmutableList.copyOf(processor.match()));
      }

      @Test
      public void testMatchSingleRow()
      {
        final IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(longPlusOneIndex(), false);

        final IndexedTableJoinMatcher.ConditionMatcher processor =
            conditionMatcherFactory.makeComplexProcessor(selector);

        Assert.assertEquals(IndexedTableJoinMatcher.NO_CONDITION_MATCH, processor.matchSingleRow());
      }
    }

    public static class MakeDimensionProcessorTest extends InitializedNullHandlingTest
    {
      @Rule
      public ExpectedException expectedException = ExpectedException.none();

      @Mock
      private DimensionSelector dimensionSelector;

      private static final String KEY = "key";

      @Test
      public void testMatchMultiValuedRowCardinalityUnknownShouldThrowException() throws Exception
      {
        try (final AutoCloseable mocks = MockitoAnnotations.openMocks(this)) {
          ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{2, 4, 6});
          Mockito.doReturn(row).when(dimensionSelector).getRow();
          Mockito.doReturn(DimensionDictionarySelector.CARDINALITY_UNKNOWN)
                 .when(dimensionSelector)
                 .getValueCardinality();

          IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
              new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex(), false);
          IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
              dimensionSelector,
              false
          );

          // Test match should throw exception
          expectedException.expect(QueryUnsupportedException.class);
          dimensionProcessor.match();
        }
      }

      @Test
      public void testMatchMultiValuedRowCardinalityKnownShouldThrowException() throws Exception
      {
        try (final AutoCloseable mocks = MockitoAnnotations.openMocks(this)) {
          ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{2, 4, 6});
          Mockito.doReturn(row).when(dimensionSelector).getRow();
          Mockito.doReturn(3).when(dimensionSelector).getValueCardinality();

          IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
              new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex(), false);
          IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
              dimensionSelector,
              false
          );

          // Test match should throw exception
          expectedException.expect(QueryUnsupportedException.class);
          dimensionProcessor.match();
        }
      }

      @Test
      public void testMatchEmptyRowCardinalityUnknown() throws Exception
      {
        try (final AutoCloseable mocks = MockitoAnnotations.openMocks(this)) {
          ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{});
          Mockito.doReturn(row).when(dimensionSelector).getRow();
          Mockito.doReturn(DimensionDictionarySelector.CARDINALITY_UNKNOWN)
                 .when(dimensionSelector)
                 .getValueCardinality();

          IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
              new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex(), false);
          IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
              dimensionSelector,
              false
          );

          Assert.assertNotNull(dimensionProcessor.match());
          Assert.assertTrue(dimensionProcessor.match().isEmpty());

          Assert.assertEquals(IndexedTableJoinMatcher.NO_CONDITION_MATCH, dimensionProcessor.matchSingleRow());
        }
      }

      @Test
      public void testMatchEmptyRowCardinalityKnown() throws Exception
      {
        try (final AutoCloseable mocks = MockitoAnnotations.openMocks(this)) {
          ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{});
          Mockito.doReturn(row).when(dimensionSelector).getRow();
          Mockito.doReturn(0).when(dimensionSelector).getValueCardinality();

          IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
              new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex(), false);
          IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
              dimensionSelector,
              false
          );

          Assert.assertNotNull(dimensionProcessor.match());
          Assert.assertTrue(dimensionProcessor.match().isEmpty());

          Assert.assertEquals(IndexedTableJoinMatcher.NO_CONDITION_MATCH, dimensionProcessor.matchSingleRow());
        }
      }

      @Test
      public void getsCorrectResultWhenSelectorCardinalityUnknown()
      {
        IndexedTableJoinMatcher.ConditionMatcher target =
            makeConditionMatcher(DimensionDictionarySelector.CARDINALITY_UNKNOWN);

        Assert.assertEquals(ImmutableList.of(KEY.length()), new IntArrayList(target.match()));
        Assert.assertEquals(KEY.length(), target.matchSingleRow());
      }

      @Test
      public void getsCorrectResultWhenSelectorCardinalityLow()
      {
        int lowCardinality = IndexedTableJoinMatcher.ConditionMatcherFactory.CACHE_MAX_SIZE / 10;
        IndexedTableJoinMatcher.ConditionMatcher target = makeConditionMatcher(lowCardinality);

        Assert.assertEquals(ImmutableList.of(KEY.length()), new IntArrayList(target.match()));
        Assert.assertEquals(KEY.length(), target.matchSingleRow());
      }

      @Test
      public void getsCorrectResultWhenSelectorCardinalityHigh()
      {
        int highCardinality = IndexedTableJoinMatcher.ConditionMatcherFactory.CACHE_MAX_SIZE / 10;
        IndexedTableJoinMatcher.ConditionMatcher target = makeConditionMatcher(highCardinality);

        Assert.assertEquals(ImmutableList.of(KEY.length()), new IntArrayList(target.match()));
        Assert.assertEquals(KEY.length(), target.matchSingleRow());
      }

      private static IndexedTableJoinMatcher.ConditionMatcher makeConditionMatcher(int valueCardinality)
      {
        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex(), false);
        return conditionMatcherFactory.makeDimensionProcessor(
            new TestDimensionSelector(KEY, valueCardinality),
            false
        );
      }

      private static class TestDimensionSelector extends ConstantDimensionSelector
      {
        private final int valueCardinality;

        TestDimensionSelector(String value, int valueCardinality)
        {
          super(value);
          this.valueCardinality = valueCardinality;
        }

        @Override
        public int getValueCardinality()
        {
          return valueCardinality;
        }
      }
    }
  }

  public static class LruLoadingHashMapTest
  {
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")  // updated via computeIfAbsent
    private IndexedTableJoinMatcher.LruLoadingHashMap<Long, Long> target;

    private AtomicLong counter;

    @Before
    public void setup()
    {
      counter = new AtomicLong(0);
      Function<Long, Long> loader = key -> {
        counter.incrementAndGet();
        return key;
      };

      target = new IndexedTableJoinMatcher.LruLoadingHashMap<>(SIZE, loader);
    }

    @Test
    public void loadsValueIfAbsent()
    {
      Long key = 1L;
      Assert.assertEquals(key, target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void doesNotLoadIfPresent()
    {
      Long key = 1L;
      Assert.assertEquals(key, target.getAndLoadIfAbsent(key));
      Assert.assertEquals(key, target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void evictsLeastRecentlyUsed()
    {
      Long start = 1L;
      Long next = start + SIZE;

      for (long i = start; i < next; i++) {
        Long key = i;
        Assert.assertEquals(key, target.getAndLoadIfAbsent(key));
      }

      Assert.assertEquals(next, target.getAndLoadIfAbsent(next));
      Assert.assertNull(target.get(start));

      Assert.assertEquals(SIZE + 1, counter.longValue());
    }
  }

  public static class Int2IntListLookupTableTest
  {
    private IndexedTableJoinMatcher.Int2IntSortedSetLookupTable target;

    private AtomicLong counter;

    @Before
    public void setup()
    {
      counter = new AtomicLong(0);
      IntFunction<IntSortedSet> loader = key -> {
        counter.incrementAndGet();
        return IntSortedSets.singleton(key);
      };

      target = new IndexedTableJoinMatcher.Int2IntSortedSetLookupTable(SIZE, loader);
    }

    @Test
    public void loadsValueIfAbsent()
    {
      int key = 1;
      Assert.assertEquals(IntSortedSets.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void doesNotLoadIfPresent()
    {
      int key = 1;
      Assert.assertEquals(IntSortedSets.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(IntSortedSets.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }
  }

  public static class Int2IntSortedSetLruCache
  {
    private IndexedTableJoinMatcher.Int2IntSortedSetLruCache target;

    private AtomicLong counter;

    @Before
    public void setup()
    {
      counter = new AtomicLong(0);
      IntFunction<IntSortedSet> loader = key -> {
        counter.incrementAndGet();
        return IntSortedSets.singleton(key);
      };

      target = new IndexedTableJoinMatcher.Int2IntSortedSetLruCache(SIZE, loader);
    }

    @Test
    public void loadsValueIfAbsent()
    {
      int key = 1;
      Assert.assertEquals(IntSortedSets.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void doesNotLoadIfPresent()
    {
      int key = 1;
      Assert.assertEquals(IntSortedSets.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(IntSortedSets.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void evictsLeastRecentlyUsed()
    {
      int start = 1;
      int next = start + SIZE;

      for (int key = start; key < next; key++) {
        Assert.assertEquals(IntSortedSets.singleton(key), target.getAndLoadIfAbsent(key));
      }

      Assert.assertEquals(IntSortedSets.singleton(next), target.getAndLoadIfAbsent(next));
      Assert.assertNull(target.get(start));

      Assert.assertEquals(SIZE + 1, counter.longValue());
    }
  }

  private static IndexedTable.Index stringToLengthIndex()
  {
    return new IndexedTable.Index()
    {
      @Override
      public ColumnType keyType()
      {
        return ColumnType.STRING;
      }

      @Override
      public boolean areKeysUnique(boolean includeNull)
      {
        return false;
      }

      @Override
      public IntSortedSet find(Object key)
      {
        return IntSortedSets.singleton(((String) key).length());
      }

      @Override
      public int findUniqueLong(long key)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static IndexedTable.Index certainStringToThreeIndex()
  {
    return new IndexedTable.Index()
    {
      @Override
      public ColumnType keyType()
      {
        return ColumnType.STRING;
      }

      @Override
      public boolean areKeysUnique(boolean includeNull)
      {
        return true;
      }

      @Override
      public IntSortedSet find(Object key)
      {
        if ("1".equals(DimensionHandlerUtils.convertObjectToString(key))) {
          return IntSortedSets.singleton(3);
        } else {
          return IntSortedSets.EMPTY_SET;
        }
      }

      @Override
      public int findUniqueLong(long key)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static IndexedTable.Index longPlusOneIndex()
  {
    return new IndexedTable.Index()
    {
      @Override
      public ColumnType keyType()
      {
        return ColumnType.LONG;
      }

      @Override
      public boolean areKeysUnique(boolean includeNull)
      {
        return true;
      }

      @Override
      public IntSortedSet find(Object key)
      {
        final Long l = DimensionHandlerUtils.convertObjectToLong(key);

        if (l == null && NullHandling.sqlCompatible()) {
          return IntSortedSets.EMPTY_SET;
        } else {
          return IntSortedSets.singleton(Ints.checkedCast((l == null ? 0L : l) + 1));
        }
      }

      @Override
      public int findUniqueLong(long key)
      {
        return Ints.checkedCast(key + 1);
      }
    };
  }

  private static IndexedTable.Index longAlwaysOneTwoThreeIndex()
  {
    return new IndexedTable.Index()
    {
      @Override
      public ColumnType keyType()
      {
        return ColumnType.LONG;
      }

      @Override
      public boolean areKeysUnique(boolean includeNull)
      {
        return false;
      }

      @Override
      public IntSortedSet find(Object key)
      {
        return new IntAVLTreeSet(new int[]{1, 2, 3});
      }

      @Override
      public int findUniqueLong(long key)
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
