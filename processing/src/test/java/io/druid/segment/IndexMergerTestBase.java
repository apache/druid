/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.SimpleDictionaryEncodedColumn;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.BitmapValues;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexMergerTestBase
{

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected IndexMerger indexMerger;

  @Parameterized.Parameters(name = "{index}: metric compression={0}, dimension compression={1}, long encoding={2}, segment write-out medium={3}")
  public static Collection<Object[]> data()
  {
    return Collections2.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
                EnumSet.allOf(CompressionStrategy.class),
                ImmutableSet.copyOf(CompressionStrategy.noNoneValues()),
                EnumSet.allOf(CompressionFactory.LongEncodingStrategy.class),
                SegmentWriteOutMediumFactory.builtInFactories()
            )
        ), new Function<List<?>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  static IndexSpec makeIndexSpec(
      BitmapSerdeFactory bitmapSerdeFactory,
      CompressionStrategy compressionStrategy,
      CompressionStrategy dimCompressionStrategy,
      CompressionFactory.LongEncodingStrategy longEncodingStrategy
  )
  {
    if (bitmapSerdeFactory != null || compressionStrategy != null) {
      return new IndexSpec(
          bitmapSerdeFactory,
          dimCompressionStrategy,
          compressionStrategy,
          longEncodingStrategy
      );
    } else {
      return new IndexSpec();
    }
  }

  private final IndexSpec indexSpec;
  private final IndexIO indexIO;
  private final boolean useBitmapIndexes;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  protected IndexMergerTestBase(
      @Nullable BitmapSerdeFactory bitmapSerdeFactory,
      CompressionStrategy compressionStrategy,
      CompressionStrategy dimCompressionStrategy,
      CompressionFactory.LongEncodingStrategy longEncodingStrategy,
      SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  )
  {
    this.indexSpec = makeIndexSpec(
        bitmapSerdeFactory != null ? bitmapSerdeFactory : new ConciseBitmapSerdeFactory(),
        compressionStrategy,
        dimCompressionStrategy,
        longEncodingStrategy
    );
    this.indexIO = TestHelper.getTestIndexIO(segmentWriteOutMediumFactory);
    this.useBitmapIndexes = bitmapSerdeFactory != null;
  }

  @Test
  public void testPersist() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec, null))
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    assertDimCompression(index, indexSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        IncrementalIndexTest.getDefaultCombiningAggregatorFactories(),
        index.getMetadata().getAggregators()
    );

    Assert.assertEquals(
        Granularities.NONE,
        index.getMetadata().getQueryGranularity()
    );
  }

  @Test
  public void testPersistWithDifferentDims() throws Exception
  {
    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    toPersist.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2")
        )
    );
    toPersist.add(
        new MapBasedInputRow(
            1,
            Collections.singletonList("dim1"),
            ImmutableMap.of("dim1", "3")
        )
    );

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec, null))
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());
    assertDimCompression(index, indexSpec.getDimensionCompression());

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(2, rowList.size());
    Assert.assertEquals(ImmutableList.of("1", "2"), rowList.get(0).dimensionValues());
    Assert.assertEquals(Arrays.asList("3", null), rowList.get(1).dimensionValues());

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dim1", ""));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim1", "1"));
    checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("dim1", "3"));

    checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("dim2", ""));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim2", "2"));
  }

  @Test
  public void testPersistWithSegmentMetadata() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    Map<String, Object> metadataElems = ImmutableMap.of("key", "value");
    toPersist.getMetadata().putAll(metadataElems);

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec, null))
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    assertDimCompression(index, indexSpec.getDimensionCompression());

    Assert.assertEquals(
        new Metadata(
            metadataElems,
            IncrementalIndexTest.getDefaultCombiningAggregatorFactories(),
            null,
            Granularities.NONE,
            Boolean.TRUE
        ),
        index.getMetadata()
    );
  }

  @Test
  public void testPersistMerge() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    IncrementalIndex toPersist2 = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .buildOnheap();

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2")
        )
    );

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "5", "dim2", "6")
        )
    );

    final File tempDir1 = temporaryFolder.newFolder();
    final File tempDir2 = temporaryFolder.newFolder();
    final File mergedDir = temporaryFolder.newFolder();

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tempDir1, indexSpec, null))
    );

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());

    QueryableIndex index2 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist2, tempDir2, indexSpec, null))
    );

    Assert.assertEquals(2, index2.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index2.getAvailableDimensions()));
    Assert.assertEquals(3, index2.getColumnNames().size());

    AggregatorFactory[] mergedAggregators = new AggregatorFactory[]{
        new CountAggregatorFactory("count")
    };
    QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                true,
                mergedAggregators,
                mergedDir,
                indexSpec,
                null
            )
        )
    );

    Assert.assertEquals(3, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());
    assertDimCompression(index2, indexSpec.getDimensionCompression());
    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(merged, indexSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        getCombiningAggregators(mergedAggregators),
        merged.getMetadata().getAggregators()
    );
  }

  @Test
  public void testPersistEmptyColumn() throws Exception
  {
    final IncrementalIndex toPersist1 = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(/* empty */)
        .setMaxRowCount(10)
        .buildOnheap();

    final IncrementalIndex toPersist2 = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(/* empty */)
        .setMaxRowCount(10)
        .buildOnheap();

    final File tmpDir1 = temporaryFolder.newFolder();
    final File tmpDir2 = temporaryFolder.newFolder();
    final File tmpDir3 = temporaryFolder.newFolder();

    toPersist1.add(
        new MapBasedInputRow(
            1L,
            ImmutableList.of("dim1", "dim2"),
            ImmutableMap.of("dim1", ImmutableList.of(), "dim2", "foo")
        )
    );

    toPersist2.add(
        new MapBasedInputRow(
            1L,
            ImmutableList.of("dim1", "dim2"),
            ImmutableMap.of("dim1", ImmutableList.of(), "dim2", "bar")
        )
    );

    final QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tmpDir1, indexSpec, null))
    );
    final QueryableIndex index2 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist2, tmpDir2, indexSpec, null))
    );
    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                true,
                new AggregatorFactory[]{},
                tmpDir3,
                indexSpec,
                null
            )
        )
    );

    Assert.assertEquals(1, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index1.getAvailableDimensions()));

    Assert.assertEquals(1, index2.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index2.getAvailableDimensions()));

    Assert.assertEquals(2, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(merged.getAvailableDimensions()));

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(index2, indexSpec.getDimensionCompression());
    assertDimCompression(merged, indexSpec.getDimensionCompression());
  }

  @Test
  public void testMergeRetainsValues() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final File tempDir1 = temporaryFolder.newFolder();
    final File mergedDir = temporaryFolder.newFolder();
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        indexSpec.getBitmapSerdeFactory()
                 .getBitmapFactory()
    );

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tempDir1, indexSpec, null))
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    indexIO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());


    QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                ImmutableList.of(index1),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                mergedDir,
                indexSpec,
                null
            )
        )
    );

    Assert.assertEquals(2, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());

    indexIO.validateTwoSegments(tempDir1, mergedDir);

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(merged, indexSpec.getDimensionCompression());
  }

  @Test
  public void testAppendRetainsValues() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final File tempDir1 = temporaryFolder.newFolder();
    final File mergedDir = temporaryFolder.newFolder();
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        indexSpec.getBitmapSerdeFactory()
                 .getBitmapFactory()
    );

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.append(ImmutableList.of(incrementalAdapter), null, tempDir1, indexSpec, null))
    );
    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    indexIO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());

    Assert.assertArrayEquals(
        IncrementalIndexTest.getDefaultCombiningAggregatorFactories(),
        index1.getMetadata().getAggregators()
    );

    AggregatorFactory[] mergedAggregators = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                ImmutableList.of(index1),
                true,
                mergedAggregators,
                mergedDir,
                indexSpec,
                null
            )
        )
    );

    Assert.assertEquals(2, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());

    indexIO.validateTwoSegments(tempDir1, mergedDir);

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(merged, indexSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        getCombiningAggregators(mergedAggregators),
        merged.getMetadata().getAggregators()
    );
  }

  @Test
  public void testMergeSpecChange() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final File tempDir1 = temporaryFolder.newFolder();
    final File mergedDir = temporaryFolder.newFolder();
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        indexSpec.getBitmapSerdeFactory()
                 .getBitmapFactory()
    );

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tempDir1, indexSpec, null))
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    indexIO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());

    IndexSpec newSpec = new IndexSpec(
        indexSpec.getBitmapSerdeFactory(),
        CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressionStrategy.LZF :
        CompressionStrategy.LZ4,
        CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressionStrategy.LZF :
        CompressionStrategy.LZ4,
        CompressionFactory.LongEncodingStrategy.LONGS.equals(indexSpec.getLongEncoding()) ?
        CompressionFactory.LongEncodingStrategy.AUTO :
        CompressionFactory.LongEncodingStrategy.LONGS
    );

    AggregatorFactory[] mergedAggregators = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                ImmutableList.of(index1),
                true,
                mergedAggregators,
                mergedDir,
                newSpec,
                null
            )
        )
    );

    Assert.assertEquals(2, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());

    indexIO.validateTwoSegments(tempDir1, mergedDir);

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(merged, newSpec.getDimensionCompression());
  }


  @Test
  public void testConvertSame() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    final AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new LongSumAggregatorFactory(
            "longSum1",
            "dim1"
        ),
        new LongSumAggregatorFactory("longSum2", "dim2")
    };

    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(aggregators);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final File tempDir1 = temporaryFolder.newFolder();
    final File convertDir = temporaryFolder.newFolder();
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        indexSpec.getBitmapSerdeFactory()
                 .getBitmapFactory()
    );

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tempDir1, indexSpec, null))
    );

    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    indexIO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(4, index1.getColumnNames().size());


    QueryableIndex converted = closer.closeLater(
        indexIO.loadIndex(indexMerger.convert(tempDir1, convertDir, indexSpec))
    );

    Assert.assertEquals(2, converted.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(converted.getAvailableDimensions()));
    Assert.assertEquals(4, converted.getColumnNames().size());

    indexIO.validateTwoSegments(tempDir1, convertDir);

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(converted, indexSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        getCombiningAggregators(aggregators),
        converted.getMetadata().getAggregators()
    );
  }


  @Test
  public void testConvertDifferent() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    final AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new LongSumAggregatorFactory(
            "longSum1",
            "dim1"
        ),
        new LongSumAggregatorFactory("longSum2", "dim2")
    };

    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(aggregators);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final File tempDir1 = temporaryFolder.newFolder();
    final File convertDir = temporaryFolder.newFolder();
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        indexSpec.getBitmapSerdeFactory()
                 .getBitmapFactory()
    );

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tempDir1, indexSpec, null))
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    indexIO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(4, index1.getColumnNames().size());


    IndexSpec newSpec = new IndexSpec(
        indexSpec.getBitmapSerdeFactory(),
        CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressionStrategy.LZF :
        CompressionStrategy.LZ4,
        CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressionStrategy.LZF :
        CompressionStrategy.LZ4,
        CompressionFactory.LongEncodingStrategy.LONGS.equals(indexSpec.getLongEncoding()) ?
        CompressionFactory.LongEncodingStrategy.AUTO :
        CompressionFactory.LongEncodingStrategy.LONGS
    );

    QueryableIndex converted = closer.closeLater(
        indexIO.loadIndex(indexMerger.convert(tempDir1, convertDir, newSpec))
    );

    Assert.assertEquals(2, converted.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(converted.getAvailableDimensions()));
    Assert.assertEquals(4, converted.getColumnNames().size());

    indexIO.validateTwoSegments(tempDir1, convertDir);

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(converted, newSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        getCombiningAggregators(aggregators),
        converted.getMetadata().getAggregators()
    );
  }

  private void assertDimCompression(QueryableIndex index, CompressionStrategy expectedStrategy)
      throws Exception
  {
    // Java voodoo
    if (expectedStrategy == null || expectedStrategy == CompressionStrategy.UNCOMPRESSED) {
      return;
    }

    DictionaryEncodedColumn encodedColumn = index.getColumn("dim2").getDictionaryEncoding();
    Object obj;
    if (encodedColumn.hasMultipleValues()) {
      Field field = SimpleDictionaryEncodedColumn.class.getDeclaredField("multiValueColumn");
      field.setAccessible(true);

      obj = field.get(encodedColumn);
    } else {
      Field field = SimpleDictionaryEncodedColumn.class.getDeclaredField("column");
      field.setAccessible(true);

      obj = field.get(encodedColumn);
    }
    // CompressedVSizeColumnarIntsSupplier$CompressedByteSizeColumnarInts
    // CompressedVSizeColumnarMultiIntsSupplier$CompressedVSizeColumnarMultiInts
    Field compressedSupplierField = obj.getClass().getDeclaredField("this$0");
    compressedSupplierField.setAccessible(true);

    Object supplier = compressedSupplierField.get(obj);

    Field compressionField = supplier.getClass().getDeclaredField("compression");
    compressionField.setAccessible(true);

    Object strategy = compressionField.get(supplier);

    Assert.assertEquals(expectedStrategy, strategy);
  }


  @Test
  public void testNonLexicographicDimOrderMerge() throws Exception
  {
    IncrementalIndex toPersist1 = getIndexD3();
    IncrementalIndex toPersist2 = getIndexD3();
    IncrementalIndex toPersist3 = getIndexD3();

    final File tmpDir = temporaryFolder.newFolder();
    final File tmpDir2 = temporaryFolder.newFolder();
    final File tmpDir3 = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tmpDir, indexSpec, null))
    );

    QueryableIndex index2 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist2, tmpDir2, indexSpec, null))
    );

    QueryableIndex index3 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist3, tmpDir3, indexSpec, null))
    );


    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2, index3),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec,
                null
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(Arrays.asList("d3", "d1", "d2"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(3, rowList.size());

    Assert.assertEquals(Arrays.asList("30000", "100", "4000"), rowList.get(0).dimensionValues());
    Assert.assertEquals(Collections.singletonList(3L), rowList.get(0).metricValues());

    Assert.assertEquals(Arrays.asList("40000", "300", "2000"), rowList.get(1).dimensionValues());
    Assert.assertEquals(Collections.singletonList(3L), rowList.get(1).metricValues());

    Assert.assertEquals(Arrays.asList("50000", "200", "3000"), rowList.get(2).dimensionValues());
    Assert.assertEquals(Collections.singletonList(3L), rowList.get(2).metricValues());

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("d3", ""));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("d3", "30000"));
    checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d3", "40000"));
    checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d3", "50000"));

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("d1", ""));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("d1", "100"));
    checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d1", "200"));
    checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d1", "300"));

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("d2", ""));
    checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d2", "2000"));
    checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d2", "3000"));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("d2", "4000"));

  }

  @Test
  public void testMergeWithDimensionsList() throws Exception
  {
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(
            makeDimensionSchemas(Arrays.asList("dimA", "dimB", "dimC")),
            null,
            null
        ))
        .withMetrics(new CountAggregatorFactory("count"))
        .build();


    IncrementalIndex toPersist1 = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .buildOnheap();
    IncrementalIndex toPersist2 = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .buildOnheap();
    IncrementalIndex toPersist3 = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .buildOnheap();

    addDimValuesToIndex(toPersist1, "dimA", Arrays.asList("1", "2"));
    addDimValuesToIndex(toPersist2, "dimA", Arrays.asList("1", "2"));
    addDimValuesToIndex(toPersist3, "dimC", Arrays.asList("1", "2"));


    final File tmpDir = temporaryFolder.newFolder();
    final File tmpDir2 = temporaryFolder.newFolder();
    final File tmpDir3 = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tmpDir, indexSpec, null))
    );

    QueryableIndex index2 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist2, tmpDir2, indexSpec, null))
    );

    QueryableIndex index3 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist3, tmpDir3, indexSpec, null))
    );

    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2, index3),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec,
                null
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(ImmutableList.of("dimA", "dimC"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(4, rowList.size());

    Assert.assertEquals(Arrays.asList(null, "1"), rowList.get(0).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList.get(0).metricValues());

    Assert.assertEquals(Arrays.asList(null, "2"), rowList.get(1).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList.get(1).metricValues());

    Assert.assertEquals(Arrays.asList("1", null), rowList.get(2).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(2).metricValues());

    Assert.assertEquals(Arrays.asList("2", null), rowList.get(3).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(3).metricValues());

    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dimA").hasBitmapIndexes());
    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dimC").hasBitmapIndexes());

    if (useBitmapIndexes) {
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dimA", ""));
      checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("dimA", "1"));
      checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("dimA", "2"));

      checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dimB", ""));

      checkBitmapIndex(Arrays.asList(2, 3), adapter.getBitmapIndex("dimC", ""));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dimC", "1"));
      checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("dimC", "2"));
    }

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dimB", ""));
  }


  @Test
  public void testDisjointDimMerge() throws Exception
  {
    IncrementalIndex toPersistA = getSingleDimIndex("dimA", Arrays.asList("1", "2"));
    IncrementalIndex toPersistB1 = getSingleDimIndex("dimB", Arrays.asList("1", "2", "3"));
    IncrementalIndex toPersistB2 = getIndexWithDims(Arrays.asList("dimA", "dimB"));
    addDimValuesToIndex(toPersistB2, "dimB", Arrays.asList("1", "2", "3"));

    for (IncrementalIndex toPersistB : Arrays.asList(toPersistB1, toPersistB2)) {

      final File tmpDirA = temporaryFolder.newFolder();
      final File tmpDirB = temporaryFolder.newFolder();
      final File tmpDirMerged = temporaryFolder.newFolder();

      QueryableIndex indexA = closer.closeLater(
          indexIO.loadIndex(indexMerger.persist(toPersistA, tmpDirA, indexSpec, null))
      );

      QueryableIndex indexB = closer.closeLater(
          indexIO.loadIndex(indexMerger.persist(toPersistB, tmpDirB, indexSpec, null))
      );

      final QueryableIndex merged = closer.closeLater(
          indexIO.loadIndex(
              indexMerger.mergeQueryableIndex(
                  Arrays.asList(indexA, indexB),
                  true,
                  new AggregatorFactory[]{new CountAggregatorFactory("count")},
                  tmpDirMerged,
                  indexSpec,
                  null
              )
          )
      );

      final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
      final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

      Assert.assertEquals(ImmutableList.of("dimA", "dimB"), ImmutableList.copyOf(adapter.getDimensionNames()));
      Assert.assertEquals(5, rowList.size());

      Assert.assertEquals(Arrays.asList(null, "1"), rowList.get(0).dimensionValues());
      Assert.assertEquals(Collections.singletonList(1L), rowList.get(0).metricValues());

      Assert.assertEquals(Arrays.asList(null, "2"), rowList.get(1).dimensionValues());
      Assert.assertEquals(Collections.singletonList(1L), rowList.get(1).metricValues());

      Assert.assertEquals(Arrays.asList(null, "3"), rowList.get(2).dimensionValues());
      Assert.assertEquals(Collections.singletonList(1L), rowList.get(2).metricValues());

      Assert.assertEquals(Arrays.asList("1", null), rowList.get(3).dimensionValues());
      Assert.assertEquals(Collections.singletonList(1L), rowList.get(3).metricValues());

      Assert.assertEquals(Arrays.asList("2", null), rowList.get(4).dimensionValues());
      Assert.assertEquals(Collections.singletonList(1L), rowList.get(4).metricValues());

      // dimA always has bitmap indexes, since it has them in indexA (it comes in through discovery).
      Assert.assertTrue(adapter.getCapabilities("dimA").hasBitmapIndexes());
      checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("dimA", ""));
      checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("dimA", "1"));
      checkBitmapIndex(Collections.singletonList(4), adapter.getBitmapIndex("dimA", "2"));


      // dimB may or may not have bitmap indexes, since it comes in through explicit definition in toPersistB2.
      //noinspection ObjectEquality
      if (toPersistB == toPersistB2) {
        Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dimB").hasBitmapIndexes());
      }
      //noinspection ObjectEquality
      if (toPersistB != toPersistB2 || useBitmapIndexes) {
        checkBitmapIndex(Arrays.asList(3, 4), adapter.getBitmapIndex("dimB", ""));
        checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dimB", "1"));
        checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("dimB", "2"));
        checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("dimB", "3"));
      }
    }
  }

  @Test
  public void testJointDimMerge() throws Exception
  {
    // (d1, d2, d3) from only one index, and their dim values are ('empty', 'has null', 'no null')
    // (d4, d5, d6, d7, d8, d9) are from both indexes
    // d4: 'empty' join 'empty'
    // d5: 'empty' join 'has null'
    // d6: 'empty' join 'no null'
    // d7: 'has null' join 'has null'
    // d8: 'has null' join 'no null'
    // d9: 'no null' join 'no null'
    IncrementalIndexSchema rollupIndexSchema = new IncrementalIndexSchema.Builder()
        .withMetrics(new CountAggregatorFactory("count"))
        .build();

    IncrementalIndexSchema noRollupIndexSchema = new IncrementalIndexSchema.Builder()
        .withMetrics(new CountAggregatorFactory("count"))
        .withRollup(false)
        .build();

    for (IncrementalIndexSchema indexSchema : Arrays.asList(rollupIndexSchema, noRollupIndexSchema)) {

      IncrementalIndex toPersistA = new IncrementalIndex.Builder()
          .setIndexSchema(indexSchema)
          .setMaxRowCount(1000)
          .buildOnheap();

      toPersistA.add(
          new MapBasedInputRow(
              1,
              Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
              ImmutableMap.of("d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910")
          )
      );
      toPersistA.add(
          new MapBasedInputRow(
              2,
              Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
              ImmutableMap.of("d2", "210", "d3", "311", "d7", "710", "d8", "810", "d9", "911")
          )
      );

      IncrementalIndex toPersistB = new IncrementalIndex.Builder()
          .setIndexSchema(indexSchema)
          .setMaxRowCount(1000)
          .buildOnheap();

      toPersistB.add(
          new MapBasedInputRow(
              3,
              Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
              ImmutableMap.of("d5", "520", "d6", "620", "d7", "720", "d8", "820", "d9", "920")
          )
      );
      toPersistB.add(
          new MapBasedInputRow(
              4,
              Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
              ImmutableMap.of("d5", "", "d6", "621", "d7", "", "d8", "821", "d9", "921")
          )
      );
      final File tmpDirA = temporaryFolder.newFolder();
      final File tmpDirB = temporaryFolder.newFolder();
      final File tmpDirMerged = temporaryFolder.newFolder();

      QueryableIndex indexA = closer.closeLater(
          indexIO.loadIndex(indexMerger.persist(toPersistA, tmpDirA, indexSpec, null))
      );

      QueryableIndex indexB = closer.closeLater(
          indexIO.loadIndex(indexMerger.persist(toPersistB, tmpDirB, indexSpec, null))
      );

      final QueryableIndex merged = closer.closeLater(
          indexIO.loadIndex(
              indexMerger.mergeQueryableIndex(
                  Arrays.asList(indexA, indexB),
                  true,
                  new AggregatorFactory[]{new CountAggregatorFactory("count")},
                  tmpDirMerged,
                  indexSpec,
                  null
              )
          )
      );

      final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
      final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

      Assert.assertEquals(
          ImmutableList.of("d2", "d3", "d5", "d6", "d7", "d8", "d9"),
          ImmutableList.copyOf(adapter.getDimensionNames())
      );
      Assert.assertEquals(4, rowList.size());
      Assert.assertEquals(
          Arrays.asList(null, "310", null, null, null, null, "910"),
          rowList.get(0).dimensionValues()
      );
      Assert.assertEquals(
          Arrays.asList("210", "311", null, null, "710", "810", "911"),
          rowList.get(1).dimensionValues()
      );
      Assert.assertEquals(
          Arrays.asList(null, null, "520", "620", "720", "820", "920"),
          rowList.get(2).dimensionValues()
      );
      Assert.assertEquals(
          Arrays.asList(null, null, null, "621", null, "821", "921"),
          rowList.get(3).dimensionValues()
      );

      checkBitmapIndex(Arrays.asList(0, 2, 3), adapter.getBitmapIndex("d2", ""));
      checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d2", "210"));

      checkBitmapIndex(Arrays.asList(2, 3), adapter.getBitmapIndex("d3", ""));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("d3", "310"));
      checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d3", "311"));

      checkBitmapIndex(Arrays.asList(0, 1, 3), adapter.getBitmapIndex("d5", ""));
      checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d5", "520"));

      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("d6", ""));
      checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d6", "620"));
      checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d6", "621"));

      checkBitmapIndex(Arrays.asList(0, 3), adapter.getBitmapIndex("d7", ""));
      checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d7", "710"));
      checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d7", "720"));

      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("d8", ""));
      checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d8", "810"));
      checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d8", "820"));
      checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d8", "821"));

      checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("d9", ""));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("d9", "910"));
      checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("d9", "911"));
      checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("d9", "920"));
      checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d9", "921"));
    }
  }

  @Test
  public void testNoRollupMergeWithDuplicateRow() throws Exception
  {
    // (d3, d6, d8, d9) as actually data from index1 and index2
    // index1 has two duplicate rows
    // index2 has 1 row which is same as index1 row and another different row
    // then we can test
    // 1. incrementalIndex with duplicate rows
    // 2. incrementalIndex without duplicate rows
    // 3. merge 2 indexes with duplicate rows

    IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMetrics(new CountAggregatorFactory("count"))
        .withRollup(false)
        .build();
    IncrementalIndex toPersistA = new IncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(1000)
        .buildOnheap();

    toPersistA.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistA.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );

    IncrementalIndex toPersistB = new IncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(1000)
        .buildOnheap();

    toPersistB.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistB.add(
        new MapBasedInputRow(
            4,
            Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d5", "", "d6", "621", "d7", "", "d8", "821", "d9", "921"
            )
        )
    );
    final File tmpDirA = temporaryFolder.newFolder();
    final File tmpDirB = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex indexA = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistA, tmpDirA, indexSpec, null))
    );

    QueryableIndex indexB = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistB, tmpDirB, indexSpec, null))
    );

    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                false,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec,
                null
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(
        ImmutableList.of("d3", "d6", "d8", "d9"),
        ImmutableList.copyOf(adapter.getDimensionNames())
    );
    Assert.assertEquals(4, rowList.size());
    Assert.assertEquals(Arrays.asList("310", null, null, "910"), rowList.get(0).dimensionValues());
    Assert.assertEquals(Arrays.asList("310", null, null, "910"), rowList.get(1).dimensionValues());
    Assert.assertEquals(Arrays.asList("310", null, null, "910"), rowList.get(2).dimensionValues());
    Assert.assertEquals(Arrays.asList(null, "621", "821", "921"), rowList.get(3).dimensionValues());

    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d3", ""));
    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d3", "310"));

    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d6", ""));
    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d6", "621"));

    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d8", ""));
    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d8", "821"));

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("d9", ""));
    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d9", "910"));
    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d9", "921"));
  }

  private void checkBitmapIndex(List<Integer> expected, BitmapValues real)
  {
    Assert.assertEquals("bitmap size", expected.size(), real.size());
    int i = 0;
    for (IntIterator iterator = real.iterator(); iterator.hasNext(); ) {
      int index = iterator.nextInt();
      Assert.assertEquals(expected.get(i++), (Integer) index);
    }
  }

  @Test
  public void testMergeWithSupersetOrdering() throws Exception
  {
    IncrementalIndex toPersistA = getSingleDimIndex("dimA", Arrays.asList("1", "2"));
    IncrementalIndex toPersistB = getSingleDimIndex("dimB", Arrays.asList("1", "2", "3"));

    IncrementalIndex toPersistBA = getSingleDimIndex("dimB", Arrays.asList("1", "2", "3"));
    addDimValuesToIndex(toPersistBA, "dimA", Arrays.asList("1", "2"));

    IncrementalIndex toPersistBA2 = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .buildOnheap();

    toPersistBA2.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimB", "dimA"),
            ImmutableMap.of("dimB", "1", "dimA", "")
        )
    );

    toPersistBA2.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimB", "dimA"),
            ImmutableMap.of("dimB", "", "dimA", "1")
        )
    );


    IncrementalIndex toPersistC = getSingleDimIndex("dimA", Arrays.asList("1", "2"));
    addDimValuesToIndex(toPersistC, "dimC", Arrays.asList("1", "2", "3"));

    final File tmpDirA = temporaryFolder.newFolder();
    final File tmpDirB = temporaryFolder.newFolder();
    final File tmpDirBA = temporaryFolder.newFolder();
    final File tmpDirBA2 = temporaryFolder.newFolder();
    final File tmpDirC = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();
    final File tmpDirMerged2 = temporaryFolder.newFolder();

    QueryableIndex indexA = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistA, tmpDirA, indexSpec, null))
    );

    QueryableIndex indexB = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistB, tmpDirB, indexSpec, null))
    );

    QueryableIndex indexBA = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistBA, tmpDirBA, indexSpec, null))
    );

    QueryableIndex indexBA2 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistBA2, tmpDirBA2, indexSpec, null))
    );

    QueryableIndex indexC = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistC, tmpDirC, indexSpec, null))
    );

    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB, indexBA, indexBA2),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec,
                null
            )
        )
    );

    final QueryableIndex merged2 = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB, indexBA, indexC),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged2,
                indexSpec,
                null
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    final QueryableIndexIndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(merged2);
    final List<DebugRow> rowList2 = RowIteratorHelper.toList(adapter2.getRows());

    Assert.assertEquals(ImmutableList.of("dimB", "dimA"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(5, rowList.size());

    Assert.assertEquals(Arrays.asList(null, "1"), rowList.get(0).dimensionValues());
    Assert.assertEquals(Collections.singletonList(3L), rowList.get(0).metricValues());

    Assert.assertEquals(Arrays.asList(null, "2"), rowList.get(1).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(1).metricValues());

    Assert.assertEquals(Arrays.asList("1", null), rowList.get(2).dimensionValues());
    Assert.assertEquals(Collections.singletonList(3L), rowList.get(2).metricValues());

    Assert.assertEquals(Arrays.asList("2", null), rowList.get(3).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(3).metricValues());

    Assert.assertEquals(Arrays.asList("3", null), rowList.get(4).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(4).metricValues());

    checkBitmapIndex(Arrays.asList(2, 3, 4), adapter.getBitmapIndex("dimA", ""));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dimA", "1"));
    checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("dimA", "2"));

    checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dimB", ""));
    checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("dimB", "1"));
    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("dimB", "2"));
    checkBitmapIndex(Collections.singletonList(4), adapter.getBitmapIndex("dimB", "3"));


    Assert.assertEquals(ImmutableList.of("dimA", "dimB", "dimC"), ImmutableList.copyOf(adapter2.getDimensionNames()));
    Assert.assertEquals(12, rowList2.size());
    Assert.assertEquals(Arrays.asList(null, null, "1"), rowList2.get(0).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(0).metricValues());
    Assert.assertEquals(Arrays.asList(null, null, "2"), rowList2.get(1).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(1).metricValues());

    Assert.assertEquals(Arrays.asList(null, null, "3"), rowList2.get(2).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(2).metricValues());
    Assert.assertEquals(Arrays.asList(null, "1", null), rowList2.get(3).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(3).metricValues());

    Assert.assertEquals(Arrays.asList(null, "2", null), rowList2.get(4).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(4).metricValues());
    Assert.assertEquals(Arrays.asList(null, "3", null), rowList2.get(5).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(5).metricValues());

    Assert.assertEquals(Arrays.asList("1", null, null), rowList2.get(6).dimensionValues());
    Assert.assertEquals(Collections.singletonList(3L), rowList2.get(6).metricValues());
    Assert.assertEquals(Arrays.asList("2", null, null), rowList2.get(7).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(7).metricValues());

    Assert.assertEquals(Arrays.asList(null, "1", null), rowList2.get(8).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(8).metricValues());
    Assert.assertEquals(Arrays.asList(null, "2", null), rowList2.get(9).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(9).metricValues());

    Assert.assertEquals(Arrays.asList(null, "3", null), rowList2.get(10).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(10).metricValues());
    Assert.assertEquals(Arrays.asList("2", null, null), rowList2.get(11).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList2.get(11).metricValues());

    checkBitmapIndex(Arrays.asList(0, 1, 2, 3, 4, 5, 8, 9, 10), adapter2.getBitmapIndex("dimA", ""));
    checkBitmapIndex(Collections.singletonList(6), adapter2.getBitmapIndex("dimA", "1"));
    checkBitmapIndex(Arrays.asList(7, 11), adapter2.getBitmapIndex("dimA", "2"));

    checkBitmapIndex(Arrays.asList(0, 1, 2, 6, 7, 11), adapter2.getBitmapIndex("dimB", ""));
    checkBitmapIndex(Arrays.asList(3, 8), adapter2.getBitmapIndex("dimB", "1"));
    checkBitmapIndex(Arrays.asList(4, 9), adapter2.getBitmapIndex("dimB", "2"));
    checkBitmapIndex(Arrays.asList(5, 10), adapter2.getBitmapIndex("dimB", "3"));

    checkBitmapIndex(Arrays.asList(3, 4, 5, 6, 7, 8, 9, 10, 11), adapter2.getBitmapIndex("dimC", ""));
    checkBitmapIndex(Collections.singletonList(0), adapter2.getBitmapIndex("dimC", "1"));
    checkBitmapIndex(Collections.singletonList(1), adapter2.getBitmapIndex("dimC", "2"));
    checkBitmapIndex(Collections.singletonList(2), adapter2.getBitmapIndex("dimC", "3"));

  }

  @Test
  public void testMismatchedDimensions() throws IOException
  {
    IncrementalIndex index1 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });
    index1.add(
        new MapBasedInputRow(
            1L,
            Arrays.asList("d1", "d2"),
            ImmutableMap.of("d1", "a", "d2", "z", "A", 1)
        )
    );
    closer.closeLater(index1);

    IncrementalIndex index2 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("C", "C")
    });
    index2.add(new MapBasedInputRow(
        1L,
        Arrays.asList("d1", "d2"),
        ImmutableMap.of("d1", "a", "d2", "z", "A", 2, "C", 100)
    ));
    closer.closeLater(index2);

    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    List<IndexableAdapter> toMerge = Arrays.asList(
        new IncrementalIndexAdapter(interval, index1, factory),
        new IncrementalIndexAdapter(interval, index2, factory)
    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    indexMerger.merge(
        toMerge,
        true,
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("A", "A"),
            new LongSumAggregatorFactory("C", "C"),
            },
        tmpDirMerged,
        indexSpec
    );
  }

  @Test
  public void testAddMetrics() throws IOException
  {
    IncrementalIndex index1 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });
    closer.closeLater(index1);
    long timestamp = System.currentTimeMillis();
    index1.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2", "A", 5)
        )
    );

    IncrementalIndex index2 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("C", "C")
    });

    index2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2", "A", 5, "C", 6)
        )
    );
    closer.closeLater(index2);

    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    List<IndexableAdapter> toMerge = Arrays.asList(
        new IncrementalIndexAdapter(interval, index1, factory),
        new IncrementalIndexAdapter(interval, index2, factory)

    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    File merged = indexMerger.merge(
        toMerge,
        true,
        new AggregatorFactory[]{new LongSumAggregatorFactory("A", "A"), new LongSumAggregatorFactory("C", "C")},
        tmpDirMerged,
        indexSpec
    );
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(indexIO.loadIndex(
        merged)));
    Assert.assertEquals(ImmutableSet.of("A", "C"), ImmutableSet.copyOf(adapter.getAvailableMetrics()));

  }

  @Test
  public void testAddMetricsBothSidesNull() throws IOException
  {
    IncrementalIndex index1 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });
    closer.closeLater(index1);
    long timestamp = System.currentTimeMillis();
    index1.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2", "A", 5)
        )
    );

    IncrementalIndex index2 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("C", "C")
    });

    index2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2", "A", 5, "C", 6)
        )
    );
    closer.closeLater(index2);


    IncrementalIndex index3 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });

    index3.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of("dim1", "1", "dim2", "2", "A", 5)
        )
    );


    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    List<IndexableAdapter> toMerge = Arrays.asList(
        new IncrementalIndexAdapter(interval, index1, factory),
        new IncrementalIndexAdapter(interval, index2, factory),
        new IncrementalIndexAdapter(interval, index3, factory)
    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    File merged = indexMerger.merge(
        toMerge,
        true,
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("A", "A"),
            new LongSumAggregatorFactory("C", "C")
        },
        tmpDirMerged,
        indexSpec
    );
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(indexIO.loadIndex(
        merged)));
    Assert.assertEquals(ImmutableSet.of("A", "C"), ImmutableSet.copyOf(adapter.getAvailableMetrics()));

  }

  @Test
  public void testMismatchedMetrics() throws IOException
  {
    IncrementalIndex index1 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });
    closer.closeLater(index1);

    IncrementalIndex index2 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("C", "C")
    });
    closer.closeLater(index2);

    IncrementalIndex index3 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("B", "B")
    });
    closer.closeLater(index3);

    IncrementalIndex index4 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("C", "C"),
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("B", "B")
    });
    closer.closeLater(index4);

    IncrementalIndex index5 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("C", "C"),
        new LongSumAggregatorFactory("B", "B")
    });
    closer.closeLater(index5);


    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    List<IndexableAdapter> toMerge = Arrays.asList(
        new IncrementalIndexAdapter(interval, index1, factory),
        new IncrementalIndexAdapter(interval, index2, factory),
        new IncrementalIndexAdapter(interval, index3, factory),
        new IncrementalIndexAdapter(interval, index4, factory),
        new IncrementalIndexAdapter(interval, index5, factory)
    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    File merged = indexMerger.merge(
        toMerge,
        true,
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("A", "A"),
            new LongSumAggregatorFactory("B", "B"),
            new LongSumAggregatorFactory("C", "C"),
            new LongSumAggregatorFactory("D", "D")
        },
        tmpDirMerged,
        indexSpec
    );

    // Since D was not present in any of the indices, it is not present in the output
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(indexIO.loadIndex(
        merged)));
    Assert.assertEquals(ImmutableSet.of("A", "B", "C"), ImmutableSet.copyOf(adapter.getAvailableMetrics()));

  }

  @Test(expected = IAE.class)
  public void testMismatchedMetricsVarying() throws IOException
  {

    IncrementalIndex index2 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("C", "C")
    });
    closer.closeLater(index2);

    IncrementalIndex index5 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("C", "C"),
        new LongSumAggregatorFactory("B", "B")
    });
    closer.closeLater(index5);


    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    List<IndexableAdapter> toMerge = Collections.singletonList(
        new IncrementalIndexAdapter(interval, index2, factory)
    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    final File merged = indexMerger.merge(
        toMerge,
        true,
        new AggregatorFactory[]{
            new LongSumAggregatorFactory("B", "B"),
            new LongSumAggregatorFactory("A", "A"),
            new LongSumAggregatorFactory("D", "D")
        },
        tmpDirMerged,
        indexSpec
    );
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(
        closer.closeLater(indexIO.loadIndex(merged))
    );
    Assert.assertEquals(ImmutableSet.of("A", "B", "C"), ImmutableSet.copyOf(adapter.getAvailableMetrics()));
  }

  @Test
  public void testMergeNumericDims() throws Exception
  {
    IncrementalIndex toPersist1 = getIndexWithNumericDims();
    IncrementalIndex toPersist2 = getIndexWithNumericDims();

    final File tmpDir = temporaryFolder.newFolder();
    final File tmpDir2 = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tmpDir, indexSpec, null))
    );

    QueryableIndex index2 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist2, tmpDir2, indexSpec, null))
    );

    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec,
                null
            )
        )
    );

    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(ImmutableList.of("dimA", "dimB", "dimC"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(4, rowList.size());

    Assert.assertEquals(Arrays.asList(0L, 0.0f, "Nully Row"), rowList.get(0).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(0).metricValues());

    Assert.assertEquals(Arrays.asList(72L, 60000.789f, "World"), rowList.get(1).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(0).metricValues());

    Assert.assertEquals(Arrays.asList(100L, 4000.567f, "Hello"), rowList.get(2).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(1).metricValues());

    Assert.assertEquals(Arrays.asList(3001L, 1.2345f, "Foobar"), rowList.get(3).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(2).metricValues());
  }

  private IncrementalIndex getIndexWithNumericDims() throws Exception
  {
    IncrementalIndex index = getIndexWithDimsFromSchemata(
        Arrays.asList(
            new LongDimensionSchema("dimA"),
            new FloatDimensionSchema("dimB"),
            new StringDimensionSchema("dimC", MultiValueHandling.SORTED_ARRAY, useBitmapIndexes)
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.of("dimA", 100L, "dimB", 4000.567, "dimC", "Hello")
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.of("dimA", 72L, "dimB", 60000.789, "dimC", "World")
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.of("dimA", 3001L, "dimB", 1.2345, "dimC", "Foobar")
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.of("dimC", "Nully Row")
        )
    );

    return index;
  }

  private IncrementalIndex getIndexWithDimsFromSchemata(List<DimensionSchema> dims)
  {
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(dims, null, null))
        .withMetrics(new CountAggregatorFactory("count"))
        .build();

    return new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .buildOnheap();
  }


  @Test
  public void testPersistNullColumnSkipping() throws Exception
  {
    //check that column d2 is skipped because it only has null values
    IncrementalIndex index1 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });
    index1.add(new MapBasedInputRow(
        1L,
        Arrays.asList("d1", "d2"),
        ImmutableMap.of("d1", "a", "d2", "", "A", 1)
    ));

    index1.add(new MapBasedInputRow(
        1L,
        Arrays.asList("d1", "d2"),
        ImmutableMap.of("d1", "b", "d2", "", "A", 1)
    ));

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(index1, tempDir, indexSpec, null))
    );
    List<String> expectedColumnNames = Arrays.asList("A", "d1");
    List<String> actualColumnNames = Lists.newArrayList(index.getColumnNames());
    Collections.sort(expectedColumnNames);
    Collections.sort(actualColumnNames);
    Assert.assertEquals(expectedColumnNames, actualColumnNames);

    SmooshedFileMapper sfm = closer.closeLater(SmooshedFileMapper.load(tempDir));
    List<String> expectedFilenames = Arrays.asList("A", "__time", "d1", "index.drd", "metadata.drd");
    List<String> actualFilenames = new ArrayList<>(sfm.getInternalFilenames());
    Collections.sort(expectedFilenames);
    Collections.sort(actualFilenames);
    Assert.assertEquals(expectedFilenames, actualFilenames);
  }


  private IncrementalIndex getIndexD3() throws Exception
  {
    IncrementalIndex toPersist1 = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .buildOnheap();

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "4000", "d3", "30000")
        )
    );

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "300", "d2", "2000", "d3", "40000")
        )
    );

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "200", "d2", "3000", "d3", "50000")
        )
    );

    return toPersist1;
  }

  private IncrementalIndex getSingleDimIndex(String dimName, List<String> values) throws Exception
  {
    IncrementalIndex toPersist1 = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .buildOnheap();

    addDimValuesToIndex(toPersist1, dimName, values);
    return toPersist1;
  }

  private void addDimValuesToIndex(IncrementalIndex index, String dimName, List<String> values) throws Exception
  {
    for (String val : values) {
      index.add(new MapBasedInputRow(1, Collections.singletonList(dimName), ImmutableMap.of(dimName, val)));
    }
  }

  private IncrementalIndex getIndexWithDims(List<String> dims)
  {
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(makeDimensionSchemas(dims), null, null))
        .withMetrics(new CountAggregatorFactory("count"))
        .build();

    return new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .buildOnheap();
  }

  private AggregatorFactory[] getCombiningAggregators(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiningAggregators = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiningAggregators[i] = aggregators[i].getCombiningFactory();
    }
    return combiningAggregators;
  }

  @Test
  public void testDictIdSeeker()
  {
    IntBuffer dimConversions = ByteBuffer.allocateDirect(3 * Integer.BYTES).asIntBuffer();
    dimConversions.put(0);
    dimConversions.put(2);
    dimConversions.put(4);
    IndexMerger.IndexSeeker dictIdSeeker = new IndexMerger.IndexSeekerWithConversion(
        (IntBuffer) dimConversions.asReadOnlyBuffer().rewind()
    );
    Assert.assertEquals(0, dictIdSeeker.seek(0));
    Assert.assertEquals(-1, dictIdSeeker.seek(1));
    Assert.assertEquals(1, dictIdSeeker.seek(2));
    try {
      dictIdSeeker.seek(5);
      Assert.fail("Only support access in order");
    }
    catch (ISE ise) {
      Assert.assertTrue("Only support access in order", true);
    }
    Assert.assertEquals(-1, dictIdSeeker.seek(3));
    Assert.assertEquals(2, dictIdSeeker.seek(4));
    Assert.assertEquals(-1, dictIdSeeker.seek(5));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCloser() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);
    ColumnCapabilitiesImpl capabilities = (ColumnCapabilitiesImpl) toPersist.getCapabilities("dim1");
    capabilities.setHasSpatialIndexes(true);

    final File tempDir = temporaryFolder.newFolder();
    final File v8TmpDir = new File(tempDir, "v8-tmp");
    final File v9TmpDir = new File(tempDir, "v9-tmp");

    try {
      indexMerger.persist(toPersist, tempDir, indexSpec, null);
    }
    finally {
      if (v8TmpDir.exists()) {
        Assert.fail("v8-tmp dir not clean.");
      }
      if (v9TmpDir.exists()) {
        Assert.fail("v9-tmp dir not clean.");
      }
    }
  }

  @Test
  public void testMultiValueHandling() throws Exception
  {
    InputRow[] rows = new InputRow[]{
        new MapBasedInputRow(
            1,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of(
                "dim1", Arrays.asList("x", "a", "a", "b"),
                "dim2", Arrays.asList("a", "x", "b", "x")
            )
        ),
        new MapBasedInputRow(
            1,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.of(
                "dim1", Arrays.asList("a", "b", "x"),
                "dim2", Arrays.asList("x", "a", "b")
            )
        )
    };

    List<DimensionSchema> schema;
    QueryableIndex index;
    QueryableIndexIndexableAdapter adapter;
    List<DebugRow> rowList;

    // xaab-axbx + abx-xab --> aabx-abxx + abx-abx --> abx-abx + aabx-abxx
    schema = makeDimensionSchemas(Arrays.asList("dim1", "dim2"), MultiValueHandling.SORTED_ARRAY);
    index = persistAndLoad(schema, rows);
    adapter = new QueryableIndexIndexableAdapter(index);
    rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    Assert.assertEquals(2, rowList.size());
    Assert.assertEquals(
        Arrays.asList(Arrays.asList("a", "b", "x"), Arrays.asList("a", "b", "x")),
        rowList.get(0).dimensionValues()
    );
    Assert.assertEquals(
        Arrays.asList(Arrays.asList("a", "a", "b", "x"), Arrays.asList("a", "b", "x", "x")),
        rowList.get(1).dimensionValues()
    );

    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dim1").hasBitmapIndexes());
    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dim2").hasBitmapIndexes());

    if (useBitmapIndexes) {
      checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dim1", ""));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim1", "a"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim1", "b"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim1", "x"));

      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim2", "a"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim2", "b"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim2", "x"));
    }

    // xaab-axbx + abx-xab --> abx-abx + abx-abx --> abx-abx
    schema = makeDimensionSchemas(Arrays.asList("dim1", "dim2"), MultiValueHandling.SORTED_SET);
    index = persistAndLoad(schema, rows);

    Assert.assertEquals(1, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    adapter = new QueryableIndexIndexableAdapter(index);
    rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(1, rowList.size());
    Assert.assertEquals(
        Arrays.asList(Arrays.asList("a", "b", "x"), Arrays.asList("a", "b", "x")),
        rowList.get(0).dimensionValues()
    );

    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dim1").hasBitmapIndexes());
    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dim2").hasBitmapIndexes());

    if (useBitmapIndexes) {
      checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dim1", ""));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim1", "a"));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim1", "b"));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim1", "x"));

      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim2", "a"));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim2", "b"));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dim2", "x"));
    }

    // xaab-axbx + abx-xab --> abx-xab + xaab-axbx
    schema = makeDimensionSchemas(Arrays.asList("dim1", "dim2"), MultiValueHandling.ARRAY);
    index = persistAndLoad(schema, rows);

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    adapter = new QueryableIndexIndexableAdapter(index);
    rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(2, rowList.size());
    Assert.assertEquals(
        Arrays.asList(Arrays.asList("a", "b", "x"), Arrays.asList("x", "a", "b")),
        rowList.get(0).dimensionValues()
    );
    Assert.assertEquals(
        Arrays.asList(Arrays.asList("x", "a", "a", "b"), Arrays.asList("a", "x", "b", "x")),
        rowList.get(1).dimensionValues()
    );

    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dim1").hasBitmapIndexes());
    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dim2").hasBitmapIndexes());

    if (useBitmapIndexes) {
      checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dim1", ""));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim1", "a"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim1", "b"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim1", "x"));

      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim2", "a"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim2", "b"));
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dim2", "x"));
    }
  }

  @Test
  public void testDimensionWithEmptyName() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);
    toPersist.add(new MapBasedInputRow(
        timestamp,
        Arrays.asList("", "dim2"),
        ImmutableMap.<String, Object>of("", "1", "dim2", "2")
    ));

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.persist(
                toPersist,
                tempDir,
                indexSpec,
                null
            )
        )
    );

    Assert.assertEquals(3, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(
        Arrays.asList("dim1", "dim2"),
        Lists.newArrayList(index.getAvailableDimensions())
    );
    Assert.assertEquals(3, index.getColumnNames().size());

    assertDimCompression(index, indexSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        IncrementalIndexTest.getDefaultCombiningAggregatorFactories(),
        index.getMetadata().getAggregators()
    );

    Assert.assertEquals(
        Granularities.NONE,
        index.getMetadata().getQueryGranularity()
    );
  }

  private QueryableIndex persistAndLoad(List<DimensionSchema> schema, InputRow... rows) throws IOException
  {
    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null, new DimensionsSpec(schema, null, null));
    for (InputRow row : rows) {
      toPersist.add(row);
    }

    final File tempDir = temporaryFolder.newFolder();
    return closer.closeLater(indexIO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec, null)));
  }

  private List<DimensionSchema> makeDimensionSchemas(final List<String> dimensions)
  {
    return makeDimensionSchemas(dimensions, MultiValueHandling.SORTED_ARRAY);
  }

  private List<DimensionSchema> makeDimensionSchemas(
      final List<String> dimensions,
      final MultiValueHandling multiValueHandling
  )
  {
    return dimensions.stream()
                     .map(
                         dimension -> new StringDimensionSchema(
                             dimension,
                             multiValueHandling,
                             useBitmapIndexes
                         )
                     )
                     .collect(Collectors.toList());
  }
}
