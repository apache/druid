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

package org.apache.druid.query.lookup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.RowBasedStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LookupSegmentTest
{
  public static final String LOOKUP_NAME = "mylookup";

  public static final Map<String, String> LOOKUP_MAP =
      ImmutableSortedMap.of(
          "a", "b",
          "x", "y"
      );

  public static final LookupExtractorFactory LOOKUP_EXTRACTOR_FACTORY =
      new LookupExtractorFactory()
      {
        @Override
        public boolean start()
        {
          return true;
        }

        @Override
        public boolean close()
        {
          return true;
        }

        @Override
        public boolean replaces(@Nullable LookupExtractorFactory other)
        {
          return true;
        }

        @Nullable
        @Override
        public LookupIntrospectHandler getIntrospectHandler()
        {
          throw new UnsupportedOperationException("not needed for this test");
        }

        @Override
        public void awaitInitialization()
        {
        }

        @Override
        public boolean isInitialized()
        {
          return true;
        }

        @Override
        public LookupExtractor get()
        {
          return new MapLookupExtractor(LOOKUP_MAP, false);
        }
      };

  private static final LookupSegment LOOKUP_SEGMENT = new LookupSegment(LOOKUP_NAME, LOOKUP_EXTRACTOR_FACTORY);

  @Test
  public void test_getId()
  {
    Assert.assertEquals(SegmentId.dummy(LOOKUP_NAME), LOOKUP_SEGMENT.getId());
  }

  @Test
  public void test_getDataInterval()
  {
    Assert.assertEquals(Intervals.ETERNITY, LOOKUP_SEGMENT.getDataInterval());
  }

  @Test
  public void test_asQueryableIndex()
  {
    Assert.assertNull(LOOKUP_SEGMENT.asQueryableIndex());
  }

  @Test
  public void test_asStorageAdapter_getAvailableDimensions()
  {
    Assert.assertEquals(
        ImmutableList.of("k", "v"),
        Lists.newArrayList(LOOKUP_SEGMENT.asStorageAdapter().getAvailableDimensions().iterator())
    );
  }

  @Test
  public void test_asStorageAdapter_getAvailableMetrics()
  {
    Assert.assertEquals(
        ImmutableList.of(),
        Lists.newArrayList(LOOKUP_SEGMENT.asStorageAdapter().getAvailableMetrics())
    );
  }

  @Test
  public void test_asStorageAdapter_getColumnCapabilitiesK()
  {
    final ColumnCapabilities capabilities = LOOKUP_SEGMENT.asStorageAdapter().getColumnCapabilities("k");

    Assert.assertEquals(ValueType.STRING, capabilities.getType());

    // Note: the "k" column does not actually have multiple values, but the RowBasedStorageAdapter doesn't allow
    // reporting complete single-valued capabilities. It would be good to change this in the future, so query engines
    // running on top of lookups can take advantage of singly-valued optimizations.
    Assert.assertTrue(capabilities.hasMultipleValues().isUnknown());
    Assert.assertFalse(capabilities.isDictionaryEncoded().isTrue());
  }

  @Test
  public void test_asStorageAdapter_getColumnCapabilitiesV()
  {
    final ColumnCapabilities capabilities = LOOKUP_SEGMENT.asStorageAdapter().getColumnCapabilities("v");

    // Note: the "v" column does not actually have multiple values, but the RowBasedStorageAdapter doesn't allow
    // reporting complete single-valued capabilities. It would be good to change this in the future, so query engines
    // running on top of lookups can take advantage of singly-valued optimizations.
    Assert.assertEquals(ValueType.STRING, capabilities.getType());
    Assert.assertTrue(capabilities.hasMultipleValues().isUnknown());
    Assert.assertFalse(capabilities.isDictionaryEncoded().isTrue());
  }

  @Test
  public void test_asStorageAdapter_getInterval()
  {
    Assert.assertEquals(Intervals.ETERNITY, LOOKUP_SEGMENT.asStorageAdapter().getInterval());
  }

  @Test
  public void test_asStorageAdapter_getDimensionCardinalityK()
  {
    Assert.assertEquals(
        DimensionDictionarySelector.CARDINALITY_UNKNOWN,
        LOOKUP_SEGMENT.asStorageAdapter().getDimensionCardinality("k")
    );
  }

  @Test
  public void test_asStorageAdapter_getDimensionCardinalityV()
  {
    Assert.assertEquals(
        DimensionDictionarySelector.CARDINALITY_UNKNOWN,
        LOOKUP_SEGMENT.asStorageAdapter().getDimensionCardinality("v")
    );
  }

  @Test
  public void test_asStorageAdapter_makeCursors()
  {
    final Sequence<Cursor> cursors = LOOKUP_SEGMENT.asStorageAdapter().makeCursors(
        null,
        Intervals.of("1970/PT1H"),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );

    final List<Pair<String, String>> kvs = new ArrayList<>();

    cursors.accumulate(
        null,
        (ignored, cursor) -> {
          final ColumnValueSelector keySelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("k");
          final ColumnValueSelector valueSelector = cursor.getColumnSelectorFactory().makeColumnValueSelector("v");

          while (!cursor.isDone()) {
            kvs.add(Pair.of(String.valueOf(keySelector.getObject()), String.valueOf(valueSelector.getObject())));
            cursor.advanceUninterruptibly();
          }

          return null;
        }
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of("a", "b"),
            Pair.of("x", "y")
        ),
        kvs
    );
  }

  @Test
  public void test_asStorageAdapter_isRowBasedAdapter()
  {
    // This allows us to assume that RowBasedStorageAdapterTest is further exercising makeCursors and verifying misc.
    // methods like getMinTime, getMaxTime, getMetadata, etc, without checking them explicitly in _this_ test class.
    Assert.assertThat(LOOKUP_SEGMENT.asStorageAdapter(), CoreMatchers.instanceOf(RowBasedStorageAdapter.class));
  }
}
