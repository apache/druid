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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.segment.ConstantDimensionSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntFunction;

@RunWith(Enclosed.class)
public class IndexedTableJoinMatcherTest
{
  private static final int SIZE = 3;

  @RunWith(Enclosed.class)
  public static class ConditionMatcherFactoryTest
  {
    public static class MakeDimensionProcessorTest
    {
      @Rule
      public ExpectedException expectedException = ExpectedException.none();

      @Mock
      private DimensionSelector dimensionSelector;

      private static final String KEY = "key";

      static {
        NullHandling.initializeForTests();
      }

      @Test
      public void testMatchMultiValuedRowCardinalityUnknownShouldThrowException()
      {
        MockitoAnnotations.initMocks(this);
        ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{2, 4, 6});
        Mockito.doReturn(row).when(dimensionSelector).getRow();
        Mockito.doReturn(DimensionDictionarySelector.CARDINALITY_UNKNOWN).when(dimensionSelector).getValueCardinality();

        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex());
        IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
            dimensionSelector,
            false
        );

        // Test match should throw exception
        expectedException.expect(QueryUnsupportedException.class);
        dimensionProcessor.match();
      }

      @Test
      public void testMatchMultiValuedRowCardinalityKnownShouldThrowException()
      {
        MockitoAnnotations.initMocks(this);
        ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{2, 4, 6});
        Mockito.doReturn(row).when(dimensionSelector).getRow();
        Mockito.doReturn(3).when(dimensionSelector).getValueCardinality();

        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex());
        IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
            dimensionSelector,
            false
        );

        // Test match should throw exception
        expectedException.expect(QueryUnsupportedException.class);
        dimensionProcessor.match();
      }

      @Test
      public void testMatchEmptyRowCardinalityUnknown()
      {
        MockitoAnnotations.initMocks(this);
        ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{});
        Mockito.doReturn(row).when(dimensionSelector).getRow();
        Mockito.doReturn(DimensionDictionarySelector.CARDINALITY_UNKNOWN).when(dimensionSelector).getValueCardinality();

        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex());
        IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
            dimensionSelector,
            false
        );

        Assert.assertNotNull(dimensionProcessor.match());
        Assert.assertFalse(dimensionProcessor.match().hasNext());

        Assert.assertEquals(IndexedTableJoinMatcher.NO_CONDITION_MATCH, dimensionProcessor.matchSingleRow());
      }

      @Test
      public void testMatchEmptyRowCardinalityKnown()
      {
        MockitoAnnotations.initMocks(this);
        ArrayBasedIndexedInts row = new ArrayBasedIndexedInts(new int[]{});
        Mockito.doReturn(row).when(dimensionSelector).getRow();
        Mockito.doReturn(0).when(dimensionSelector).getValueCardinality();

        IndexedTableJoinMatcher.ConditionMatcherFactory conditionMatcherFactory =
            new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex());
        IndexedTableJoinMatcher.ConditionMatcher dimensionProcessor = conditionMatcherFactory.makeDimensionProcessor(
            dimensionSelector,
            false
        );

        Assert.assertNotNull(dimensionProcessor.match());
        Assert.assertFalse(dimensionProcessor.match().hasNext());

        Assert.assertEquals(IndexedTableJoinMatcher.NO_CONDITION_MATCH, dimensionProcessor.matchSingleRow());
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
            new IndexedTableJoinMatcher.ConditionMatcherFactory(stringToLengthIndex());
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
    private IndexedTableJoinMatcher.Int2IntListLookupTable target;

    private AtomicLong counter;

    @Before
    public void setup()
    {
      counter = new AtomicLong(0);
      IntFunction<IntList> loader = key -> {
        counter.incrementAndGet();
        return IntLists.singleton(key);
      };

      target = new IndexedTableJoinMatcher.Int2IntListLookupTable(SIZE, loader);
    }

    @Test
    public void loadsValueIfAbsent()
    {
      int key = 1;
      Assert.assertEquals(IntLists.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void doesNotLoadIfPresent()
    {
      int key = 1;
      Assert.assertEquals(IntLists.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(IntLists.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }
  }

  public static class Int2IntListLruCache
  {
    private IndexedTableJoinMatcher.Int2IntListLruCache target;

    private AtomicLong counter;

    @Before
    public void setup()
    {
      counter = new AtomicLong(0);
      IntFunction<IntList> loader = key -> {
        counter.incrementAndGet();
        return IntLists.singleton(key);
      };

      target = new IndexedTableJoinMatcher.Int2IntListLruCache(SIZE, loader);
    }

    @Test
    public void loadsValueIfAbsent()
    {
      int key = 1;
      Assert.assertEquals(IntLists.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void doesNotLoadIfPresent()
    {
      int key = 1;
      Assert.assertEquals(IntLists.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(IntLists.singleton(key), target.getAndLoadIfAbsent(key));
      Assert.assertEquals(1L, counter.longValue());
    }

    @Test
    public void evictsLeastRecentlyUsed()
    {
      int start = 1;
      int next = start + SIZE;

      for (int key = start; key < next; key++) {
        Assert.assertEquals(IntLists.singleton(key), target.getAndLoadIfAbsent(key));
      }

      Assert.assertEquals(IntLists.singleton(next), target.getAndLoadIfAbsent(next));
      Assert.assertNull(target.get(start));

      Assert.assertEquals(SIZE + 1, counter.longValue());
    }
  }

  private static IndexedTable.Index stringToLengthIndex()
  {
    return new IndexedTable.Index()
    {
      @Override
      public ValueType keyType()
      {
        return ValueType.STRING;
      }

      @Override
      public boolean areKeysUnique()
      {
        return false;
      }

      @Override
      public IntList find(Object key)
      {
        return IntLists.singleton(((String) key).length());
      }

      @Override
      public int findUniqueLong(long key)
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
