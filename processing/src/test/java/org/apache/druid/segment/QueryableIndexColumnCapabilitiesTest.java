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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryableIndexColumnCapabilitiesTest extends InitializedNullHandlingTest
{
  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static IncrementalIndex INC_INDEX;
  private static QueryableIndex MMAP_INDEX;
  private static IncrementalIndex INC_INDEX_WITH_NULLS;
  private static QueryableIndex MMAP_INDEX_WITH_NULLS;

  @BeforeClass
  public static void setup() throws IOException
  {
    MapInputRowParser parser = new MapInputRowParser(
        new TimeAndDimsParseSpec(
            new TimestampSpec("time", "auto", null),
            new DimensionsSpec(
                ImmutableList.<DimensionSchema>builder()
                    .addAll(DimensionsSpec.getDefaultSchemas(ImmutableList.of("d1", "d2")))
                    .add(new DoubleDimensionSchema("d3"))
                    .add(new FloatDimensionSchema("d4"))
                    .add(new LongDimensionSchema("d5"))
                    .build()
            )
        )
    );
    AggregatorFactory[] metricsSpecs = new AggregatorFactory[] {
        new CountAggregatorFactory("cnt"),
        new DoubleSumAggregatorFactory("m1", "d3"),
        new FloatSumAggregatorFactory("m2", "d4"),
        new LongSumAggregatorFactory("m3", "d5"),
        new HyperUniquesAggregatorFactory("m4", "d1")
    };
    List<InputRow> rows = new ArrayList<>();
    Map<String, Object> event =
        ImmutableMap.<String, Object>builder().put("time", DateTimes.nowUtc().getMillis())
                                              .put("d1", "some string")
                                              .put("d2", ImmutableList.of("some", "list"))
                                              .put("d3", 1.234)
                                              .put("d4", 1.234f)
                                              .put("d5", 10L)
                                              .build();
    rows.add(Iterables.getOnlyElement(parser.parseBatch(event)));

    IndexBuilder builder = IndexBuilder.create()
                                       .rows(rows)
                                       .schema(
                                           new IncrementalIndexSchema.Builder()
                                               .withMetrics(metricsSpecs)
                                               .withDimensionsSpec(parser)
                                               .withRollup(false)
                                               .build()
                                       )
                                       .tmpDir(temporaryFolder.newFolder());
    INC_INDEX = builder.buildIncrementalIndex();
    MMAP_INDEX = builder.buildMMappedIndex();

    List<InputRow> rowsWithNulls = new ArrayList<>();
    rowsWithNulls.add(Iterables.getOnlyElement(parser.parseBatch(event)));

    Map<String, Object> eventWithNulls = new HashMap<>();
    eventWithNulls.put("time", DateTimes.nowUtc().getMillis());
    eventWithNulls.put("d1", null);
    eventWithNulls.put("d2", ImmutableList.of());
    eventWithNulls.put("d3", null);
    eventWithNulls.put("d4", null);
    eventWithNulls.put("d5", null);

    rowsWithNulls.add(Iterables.getOnlyElement(parser.parseBatch(eventWithNulls)));

    IndexBuilder builderWithNulls = IndexBuilder.create()
                                                .rows(rowsWithNulls)
                                                .schema(
                                                    new IncrementalIndexSchema.Builder()
                                                        .withMetrics(metricsSpecs)
                                                        .withDimensionsSpec(parser)
                                                        .withRollup(false)
                                                        .build()
                                                )
                                                .tmpDir(temporaryFolder.newFolder());
    INC_INDEX_WITH_NULLS = builderWithNulls.buildIncrementalIndex();
    MMAP_INDEX_WITH_NULLS = builderWithNulls.buildMMappedIndex();
  }

  @AfterClass
  public static void teardown()
  {
    INC_INDEX.close();
    MMAP_INDEX.close();
    INC_INDEX_WITH_NULLS.close();
    MMAP_INDEX_WITH_NULLS.close();
  }

  @Test
  public void testNumericColumns()
  {
    // incremental index
    assertNonStringColumnCapabilities(INC_INDEX.getColumnCapabilities(ColumnHolder.TIME_COLUMN_NAME), ColumnType.LONG);
    assertNonStringColumnCapabilities(INC_INDEX.getColumnCapabilities("d3"), ColumnType.DOUBLE);
    assertNonStringColumnCapabilities(INC_INDEX.getColumnCapabilities("d4"), ColumnType.FLOAT);
    assertNonStringColumnCapabilities(INC_INDEX.getColumnCapabilities("d5"), ColumnType.LONG);
    assertNonStringColumnCapabilities(INC_INDEX.getColumnCapabilities("m1"), ColumnType.DOUBLE);
    assertNonStringColumnCapabilities(INC_INDEX.getColumnCapabilities("m2"), ColumnType.FLOAT);
    assertNonStringColumnCapabilities(INC_INDEX.getColumnCapabilities("m3"), ColumnType.LONG);

    // segment index
    assertNonStringColumnCapabilities(
        MMAP_INDEX.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getCapabilities(),
        ColumnType.LONG
    );
    assertNonStringColumnCapabilities(MMAP_INDEX.getColumnHolder("d3").getCapabilities(), ColumnType.DOUBLE);
    assertNonStringColumnCapabilities(MMAP_INDEX.getColumnHolder("d4").getCapabilities(), ColumnType.FLOAT);
    assertNonStringColumnCapabilities(MMAP_INDEX.getColumnHolder("d5").getCapabilities(), ColumnType.LONG);
    assertNonStringColumnCapabilities(MMAP_INDEX.getColumnHolder("m1").getCapabilities(), ColumnType.DOUBLE);
    assertNonStringColumnCapabilities(MMAP_INDEX.getColumnHolder("m2").getCapabilities(), ColumnType.FLOAT);
    assertNonStringColumnCapabilities(MMAP_INDEX.getColumnHolder("m3").getCapabilities(), ColumnType.LONG);
  }

  @Test
  public void testNumericColumnsWithNulls()
  {
    // incremental index
    // time does not have nulls
    assertNonStringColumnCapabilities(
        INC_INDEX_WITH_NULLS.getColumnCapabilities(ColumnHolder.TIME_COLUMN_NAME),
        ColumnType.LONG
    );
    assertNonStringColumnCapabilitiesWithNulls(INC_INDEX_WITH_NULLS.getColumnCapabilities("d3"), ColumnType.DOUBLE);
    assertNonStringColumnCapabilitiesWithNulls(INC_INDEX_WITH_NULLS.getColumnCapabilities("d4"), ColumnType.FLOAT);
    assertNonStringColumnCapabilitiesWithNulls(INC_INDEX_WITH_NULLS.getColumnCapabilities("d5"), ColumnType.LONG);
    assertNonStringColumnCapabilitiesWithNulls(INC_INDEX_WITH_NULLS.getColumnCapabilities("m1"), ColumnType.DOUBLE);
    assertNonStringColumnCapabilitiesWithNulls(INC_INDEX_WITH_NULLS.getColumnCapabilities("m2"), ColumnType.FLOAT);
    assertNonStringColumnCapabilitiesWithNulls(INC_INDEX_WITH_NULLS.getColumnCapabilities("m3"), ColumnType.LONG);

    // segment index
    assertNonStringColumnCapabilities(
        MMAP_INDEX_WITH_NULLS.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getCapabilities(),
        ColumnType.LONG
    );
    assertNonStringColumnCapabilitiesWithNulls(
        MMAP_INDEX_WITH_NULLS.getColumnHolder("d3").getCapabilities(),
        ColumnType.DOUBLE
    );
    assertNonStringColumnCapabilitiesWithNulls(
        MMAP_INDEX_WITH_NULLS.getColumnHolder("d4").getCapabilities(),
        ColumnType.FLOAT
    );
    assertNonStringColumnCapabilitiesWithNulls(
        MMAP_INDEX_WITH_NULLS.getColumnHolder("d5").getCapabilities(),
        ColumnType.LONG
    );
    assertNonStringColumnCapabilitiesWithNulls(
        MMAP_INDEX_WITH_NULLS.getColumnHolder("m1").getCapabilities(),
        ColumnType.DOUBLE
    );
    assertNonStringColumnCapabilitiesWithNulls(
        MMAP_INDEX_WITH_NULLS.getColumnHolder("m2").getCapabilities(),
        ColumnType.FLOAT
    );
    assertNonStringColumnCapabilitiesWithNulls(
        MMAP_INDEX_WITH_NULLS.getColumnHolder("m3").getCapabilities(),
        ColumnType.LONG
    );
  }

  @Test
  public void testStringColumn()
  {
    ColumnCapabilities caps = INC_INDEX.getColumnCapabilities("d1");
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isMaybeTrue());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    // multi-value is unknown unless explicitly set to 'true'
    Assert.assertTrue(caps.hasMultipleValues().isUnknown());
    // at index merge or query time we 'complete' the capabilities to take a snapshot of the current state,
    // coercing any 'UNKNOWN' values to false
    Assert.assertFalse(
        ColumnCapabilitiesImpl.snapshot(
            caps,
            IndexMergerV9.DIMENSION_CAPABILITY_MERGE_LOGIC
        ).hasMultipleValues().isMaybeTrue()
    );
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertTrue(caps.hasNulls().isUnknown());

    caps = MMAP_INDEX.getColumnHolder("d1").getCapabilities();
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertFalse(caps.hasNulls().isMaybeTrue());
  }


  @Test
  public void testStringColumnWithNulls()
  {
    ColumnCapabilities caps = INC_INDEX_WITH_NULLS.getColumnCapabilities("d1");
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    // multi-value is unknown unless explicitly set to 'true'
    Assert.assertTrue(caps.hasMultipleValues().isUnknown());
    // at index merge or query time we 'complete' the capabilities to take a snapshot of the current state,
    // coercing any 'UNKNOWN' values to false
    Assert.assertFalse(
        ColumnCapabilitiesImpl.snapshot(
            caps,
            IndexMergerV9.DIMENSION_CAPABILITY_MERGE_LOGIC
        ).hasMultipleValues().isMaybeTrue()
    );
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertTrue(caps.hasNulls().isTrue());

    caps = MMAP_INDEX_WITH_NULLS.getColumnHolder("d1").getCapabilities();
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertTrue(caps.hasNulls().isTrue());
  }

  @Test
  public void testMultiStringColumn()
  {
    ColumnCapabilities caps = INC_INDEX.getColumnCapabilities("d2");
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertTrue(caps.hasNulls().isUnknown());

    caps = MMAP_INDEX.getColumnHolder("d2").getCapabilities();
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertFalse(caps.hasNulls().isMaybeTrue());
  }


  @Test
  public void testMultiStringColumnWithNulls()
  {
    ColumnCapabilities caps = INC_INDEX_WITH_NULLS.getColumnCapabilities("d2");
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertTrue(caps.hasNulls().isTrue());

    caps = MMAP_INDEX_WITH_NULLS.getColumnHolder("d2").getCapabilities();
    Assert.assertEquals(ValueType.STRING, caps.getType());
    Assert.assertTrue(caps.hasBitmapIndexes());
    Assert.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertTrue(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertTrue(caps.hasMultipleValues().isTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertTrue(caps.hasNulls().isTrue());
  }

  @Test
  public void testComplexColumn()
  {
    assertComplexColumnCapabilites(INC_INDEX.getColumnCapabilities("m4"));
    assertComplexColumnCapabilites(MMAP_INDEX.getColumnHolder("m4").getCapabilities());
    // results for this complex aren't different, we only know that nullability is unknown
    assertComplexColumnCapabilites(INC_INDEX_WITH_NULLS.getColumnCapabilities("m4"));
    assertComplexColumnCapabilites(MMAP_INDEX_WITH_NULLS.getColumnHolder("m4").getCapabilities());
  }

  private void assertComplexColumnCapabilites(ColumnCapabilities caps)
  {
    Assert.assertEquals(HyperUniquesAggregatorFactory.TYPE, caps.toColumnType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertFalse(caps.hasMultipleValues().isUnknown());
    Assert.assertTrue(caps.hasNulls().isTrue());
  }

  private void assertNonStringColumnCapabilities(ColumnCapabilities caps, ColumnType valueType)
  {
    Assert.assertEquals(valueType, caps.toColumnType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    Assert.assertFalse(caps.hasNulls().isTrue());
  }

  private void assertNonStringColumnCapabilitiesWithNulls(ColumnCapabilities caps, ColumnType valueType)
  {
    Assert.assertEquals(valueType, caps.toColumnType());
    Assert.assertFalse(caps.hasBitmapIndexes());
    Assert.assertFalse(caps.isDictionaryEncoded().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesSorted().isTrue());
    Assert.assertFalse(caps.areDictionaryValuesUnique().isTrue());
    Assert.assertFalse(caps.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(caps.hasSpatialIndexes());
    // check isMaybeTrue because incremental index uses Unknown
    Assert.assertEquals(NullHandling.sqlCompatible(), caps.hasNulls().isMaybeTrue());
  }
}
