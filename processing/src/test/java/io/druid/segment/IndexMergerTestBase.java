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
import com.google.common.primitives.Ints;
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
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
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

public class IndexMergerTestBase
{
  private final static IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected IndexMerger indexMerger;

  @Parameterized.Parameters(name = "{index}: metric compression={0}, dimension compression={1}, long encoding={2}")
  public static Collection<Object[]> data()
  {
    return Collections2.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
                EnumSet.allOf(CompressedObjectStrategy.CompressionStrategy.class),
                ImmutableSet.copyOf(CompressedObjectStrategy.CompressionStrategy.noNoneValues()),
                EnumSet.allOf(CompressionFactory.LongEncodingStrategy.class)
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
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      CompressedObjectStrategy.CompressionStrategy dimCompressionStrategy,
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
  @Rule
  public final CloserRule closer = new CloserRule(false);

  protected IndexMergerTestBase(
      BitmapSerdeFactory bitmapSerdeFactory,
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      CompressedObjectStrategy.CompressionStrategy dimCompressionStrategy,
      CompressionFactory.LongEncodingStrategy longEncodingStrategy
  )
  {
    this.indexSpec = makeIndexSpec(bitmapSerdeFactory, compressionStrategy, dimCompressionStrategy, longEncodingStrategy);
  }

  @Test
  public void testPersist() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist,
                tempDir,
                indexSpec
            )
        )
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
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2")
        )
    );
    toPersist.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dim1"),
            ImmutableMap.<String, Object>of("dim1", "3")
        )
    );

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist,
                tempDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());
    assertDimCompression(index, indexSpec.getDimensionCompression());

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(2, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0}, {1}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new int[][]{{1}, {0}}, boatList.get(1).getDims());

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("dim1", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim1", "1"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("dim1", "3"));

    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("dim2", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim2", "2"));
  }

  @Test
  public void testPersistWithSegmentMetadata() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    Map<String, Object> metadataElems = ImmutableMap.<String, Object>of("key", "value");
    toPersist.getMetadata().putAll(metadataElems);

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist,
                tempDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    assertDimCompression(index, indexSpec.getDimensionCompression());

    Assert.assertEquals(
        new Metadata()
            .setAggregators(
                IncrementalIndexTest.getDefaultCombiningAggregatorFactories()
            )
            .setQueryGranularity(Granularities.NONE)
            .setRollup(Boolean.TRUE)
            .putAll(metadataElems),
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
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2")
        )
    );

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.<String, Object>of("dim1", "5", "dim2", "6")
        )
    );

    final File tempDir1 = temporaryFolder.newFolder();
    final File tempDir2 = temporaryFolder.newFolder();
    final File mergedDir = temporaryFolder.newFolder();

    QueryableIndex index1 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tempDir1,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());

    QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist2,
                tempDir2,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index2.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index2.getAvailableDimensions()));
    Assert.assertEquals(3, index2.getColumnNames().size());

    AggregatorFactory[] mergedAggregators = new AggregatorFactory[]{
        new CountAggregatorFactory("count")
    };
    QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                true,
                mergedAggregators,
                mergedDir,
                indexSpec
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
            ImmutableMap.<String, Object>of("dim1", ImmutableList.of(), "dim2", "foo")
        )
    );

    toPersist2.add(
        new MapBasedInputRow(
            1L,
            ImmutableList.of("dim1", "dim2"),
            ImmutableMap.<String, Object>of("dim1", ImmutableList.of(), "dim2", "bar")
        )
    );

    final QueryableIndex index1 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tmpDir1,
                indexSpec
            )
        )
    );
    final QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist2,
                tmpDir2,
                indexSpec
            )
        )
    );
    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                true,
                new AggregatorFactory[]{},
                tmpDir3,
                indexSpec
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
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tempDir1,
                indexSpec
            )
        )
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    INDEX_IO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());


    QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                ImmutableList.of(index1),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                mergedDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());

    INDEX_IO.validateTwoSegments(tempDir1, mergedDir);

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
        INDEX_IO.loadIndex(
            indexMerger.append(
                ImmutableList.<IndexableAdapter>of(incrementalAdapter), null, tempDir1, indexSpec
            )
        )
    );
    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    INDEX_IO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());

    Assert.assertArrayEquals(
        IncrementalIndexTest.getDefaultCombiningAggregatorFactories(),
        index1.getMetadata().getAggregators()
    );

    AggregatorFactory[] mergedAggregators = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                ImmutableList.of(index1),
                true,
                mergedAggregators,
                mergedDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());

    INDEX_IO.validateTwoSegments(tempDir1, mergedDir);

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
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tempDir1,
                indexSpec
            )
        )
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    INDEX_IO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());

    IndexSpec newSpec = new IndexSpec(
        indexSpec.getBitmapSerdeFactory(),
        CompressedObjectStrategy.CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressedObjectStrategy.CompressionStrategy.LZF :
        CompressedObjectStrategy.CompressionStrategy.LZ4,
        CompressedObjectStrategy.CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressedObjectStrategy.CompressionStrategy.LZF :
        CompressedObjectStrategy.CompressionStrategy.LZ4,
        CompressionFactory.LongEncodingStrategy.LONGS.equals(indexSpec.getLongEncoding()) ?
        CompressionFactory.LongEncodingStrategy.AUTO :
        CompressionFactory.LongEncodingStrategy.LONGS
    );

    AggregatorFactory[] mergedAggregators = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                ImmutableList.of(index1),
                true,
                mergedAggregators,
                mergedDir,
                newSpec
            )
        )
    );

    Assert.assertEquals(2, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());

    INDEX_IO.validateTwoSegments(tempDir1, mergedDir);

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
        INDEX_IO.loadIndex(indexMerger.persist(toPersist1, tempDir1, indexSpec))
    );

    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    INDEX_IO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(4, index1.getColumnNames().size());


    QueryableIndex converted = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.convert(
                tempDir1,
                convertDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, converted.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(converted.getAvailableDimensions()));
    Assert.assertEquals(4, converted.getColumnNames().size());

    INDEX_IO.validateTwoSegments(tempDir1, convertDir);

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
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tempDir1,
                indexSpec
            )
        )
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    INDEX_IO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(4, index1.getColumnNames().size());


    IndexSpec newSpec = new IndexSpec(
        indexSpec.getBitmapSerdeFactory(),
        CompressedObjectStrategy.CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressedObjectStrategy.CompressionStrategy.LZF :
        CompressedObjectStrategy.CompressionStrategy.LZ4,
        CompressedObjectStrategy.CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressedObjectStrategy.CompressionStrategy.LZF :
        CompressedObjectStrategy.CompressionStrategy.LZ4,
        CompressionFactory.LongEncodingStrategy.LONGS.equals(indexSpec.getLongEncoding()) ?
        CompressionFactory.LongEncodingStrategy.AUTO :
        CompressionFactory.LongEncodingStrategy.LONGS
    );

    QueryableIndex converted = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.convert(
                tempDir1,
                convertDir,
                newSpec
            )
        )
    );

    Assert.assertEquals(2, converted.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(converted.getAvailableDimensions()));
    Assert.assertEquals(4, converted.getColumnNames().size());

    INDEX_IO.validateTwoSegments(tempDir1, convertDir);

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(converted, newSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        getCombiningAggregators(aggregators),
        converted.getMetadata().getAggregators()
    );
  }

  private void assertDimCompression(QueryableIndex index, CompressedObjectStrategy.CompressionStrategy expectedStrategy)
      throws Exception
  {
    // Java voodoo
    if (expectedStrategy == null || expectedStrategy == CompressedObjectStrategy.CompressionStrategy.UNCOMPRESSED) {
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
    // CompressedVSizeIntsIndexedSupplier$CompressedByteSizeIndexedInts
    // CompressedVSizeIndexedSupplier$CompressedVSizeIndexed
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
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tmpDir,
                indexSpec
            )
        )
    );

    QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist2,
                tmpDir2,
                indexSpec
            )
        )
    );

    QueryableIndex index3 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist3,
                tmpDir3,
                indexSpec
            )
        )
    );


    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2, index3),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(ImmutableList.of("d3", "d1", "d2"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(3, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {2}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new Object[]{3L}, boatList.get(0).getMetrics());
    Assert.assertArrayEquals(new int[][]{{1}, {2}, {0}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new Object[]{3L}, boatList.get(1).getMetrics());
    Assert.assertArrayEquals(new int[][]{{2}, {1}, {1}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new Object[]{3L}, boatList.get(2).getMetrics());

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("d3", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d3", "30000"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d3", "40000"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d3", "50000"));

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("d1", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d1", "100"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d1", "200"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d1", "300"));

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("d2", ""));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d2", "2000"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d2", "3000"));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d2", "4000"));

  }

  @Test
  public void testMergeWithDimensionsList() throws Exception
  {
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("dimA", "dimB", "dimC")),
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
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tmpDir,
                indexSpec
            )
        )
    );

    QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist2,
                tmpDir2,
                indexSpec
            )
        )
    );

    QueryableIndex index3 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist3,
                tmpDir3,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2, index3),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(ImmutableList.of("dimA", "dimC"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(4, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0}, {1}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList.get(0).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {2}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList.get(1).getMetrics());
    Assert.assertArrayEquals(new int[][]{{1}, {0}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(2).getMetrics());
    Assert.assertArrayEquals(new int[][]{{2}, {0}}, boatList.get(3).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(3).getMetrics());

    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dimA", ""));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("dimA", "1"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("dimA", "2"));

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("dimB", ""));

    checkBitmapIndex(Lists.newArrayList(2, 3), adapter.getBitmapIndex("dimC", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dimC", "1"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("dimC", "2"));
  }


  @Test
  public void testDisjointDimMerge() throws Exception
  {
    IncrementalIndex toPersistA = getSingleDimIndex("dimA", Arrays.asList("1", "2"));
    IncrementalIndex toPersistB = getSingleDimIndex("dimB", Arrays.asList("1", "2", "3"));
    IncrementalIndex toPersistB2 = getIndexWithDims(Arrays.asList("dimA", "dimB"));
    addDimValuesToIndex(toPersistB2, "dimB", Arrays.asList("1", "2", "3"));

    final File tmpDirA = temporaryFolder.newFolder();
    final File tmpDirB = temporaryFolder.newFolder();
    final File tmpDirB2 = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex indexA = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    QueryableIndex indexB2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistB2,
                tmpDirB2,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndex merged2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB2),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    final QueryableIndexIndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(merged2);
    final List<Rowboat> boatList2 = ImmutableList.copyOf(adapter2.getRows());

    Assert.assertEquals(ImmutableList.of("dimA", "dimB"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(5, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0}, {1}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList.get(0).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {2}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList.get(1).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {3}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList.get(2).getMetrics());
    Assert.assertArrayEquals(new int[][]{{1}, {0}}, boatList.get(3).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList.get(3).getMetrics());
    Assert.assertArrayEquals(new int[][]{{2}, {0}}, boatList.get(4).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList.get(4).getMetrics());

    checkBitmapIndex(Lists.newArrayList(0, 1, 2), adapter.getBitmapIndex("dimA", ""));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("dimA", "1"));
    checkBitmapIndex(Lists.newArrayList(4), adapter.getBitmapIndex("dimA", "2"));

    checkBitmapIndex(Lists.newArrayList(3, 4), adapter.getBitmapIndex("dimB", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dimB", "1"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("dimB", "2"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("dimB", "3"));

    Assert.assertEquals(ImmutableList.of("dimA", "dimB"), ImmutableList.copyOf(adapter2.getDimensionNames()));
    Assert.assertEquals(5, boatList2.size());
    Assert.assertArrayEquals(new int[][]{{0}, {1}}, boatList2.get(0).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(0).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {2}}, boatList2.get(1).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(1).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {3}}, boatList2.get(2).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(2).getMetrics());
    Assert.assertArrayEquals(new int[][]{{1}, {0}}, boatList2.get(3).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(3).getMetrics());
    Assert.assertArrayEquals(new int[][]{{2}, {0}}, boatList2.get(4).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(4).getMetrics());

    checkBitmapIndex(Lists.newArrayList(0, 1, 2), adapter2.getBitmapIndex("dimA", ""));
    checkBitmapIndex(Lists.newArrayList(3), adapter2.getBitmapIndex("dimA", "1"));
    checkBitmapIndex(Lists.newArrayList(4), adapter2.getBitmapIndex("dimA", "2"));

    checkBitmapIndex(Lists.newArrayList(3, 4), adapter2.getBitmapIndex("dimB", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter2.getBitmapIndex("dimB", "1"));
    checkBitmapIndex(Lists.newArrayList(1), adapter2.getBitmapIndex("dimB", "2"));
    checkBitmapIndex(Lists.newArrayList(2), adapter2.getBitmapIndex("dimB", "3"));
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

    IncrementalIndex toPersistA = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .buildOnheap();

    toPersistA.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistA.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d2", "210", "d3", "311", "d7", "710", "d8", "810", "d9", "911"
            )
        )
    );

    IncrementalIndex toPersistB = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .buildOnheap();

    toPersistB.add(
        new MapBasedInputRow(
            3,
            Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d5", "520", "d6", "620", "d7", "720", "d8", "820", "d9", "920"
            )
        )
    );
    toPersistB.add(
        new MapBasedInputRow(
            4,
            Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d5", "", "d6", "621", "d7", "", "d8", "821", "d9", "921"
            )
        )
    );
    final File tmpDirA = temporaryFolder.newFolder();
    final File tmpDirB = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex indexA = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(
        ImmutableList.of("d2", "d3", "d5", "d6", "d7", "d8", "d9"),
        ImmutableList.copyOf(adapter.getDimensionNames())
    );
    Assert.assertEquals(4, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0}, {1}, {0}, {0}, {0}, {0}, {0}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new int[][]{{1}, {2}, {0}, {0}, {1}, {1}, {1}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {1}, {1}, {2}, {2}, {2}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {0}, {2}, {0}, {3}, {3}}, boatList.get(3).getDims());

    checkBitmapIndex(Lists.newArrayList(0, 2, 3), adapter.getBitmapIndex("d2", ""));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d2", "210"));

    checkBitmapIndex(Lists.newArrayList(2, 3), adapter.getBitmapIndex("d3", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d3", "310"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d3", "311"));

    checkBitmapIndex(Lists.newArrayList(0, 1, 3), adapter.getBitmapIndex("d5", ""));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d5", "520"));

    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("d6", ""));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d6", "620"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d6", "621"));

    checkBitmapIndex(Lists.newArrayList(0, 3), adapter.getBitmapIndex("d7", ""));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d7", "710"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d7", "720"));

    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d8", ""));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d8", "810"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d8", "820"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d8", "821"));

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("d9", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d9", "910"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d9", "911"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d9", "920"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d9", "921"));
  }

  @Test
  public void testNoRollupMergeWithoutDuplicateRow() throws Exception
  {
    // (d1, d2, d3) from only one index, and their dim values are ('empty', 'has null', 'no null')
    // (d4, d5, d6, d7, d8, d9) are from both indexes
    // d4: 'empty' join 'empty'
    // d5: 'empty' join 'has null'
    // d6: 'empty' join 'no null'
    // d7: 'has null' join 'has null'
    // d8: 'has null' join 'no null'
    // d9: 'no null' join 'no null'

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
            ImmutableMap.<String, Object>of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistA.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d2", "210", "d3", "311", "d7", "710", "d8", "810", "d9", "911"
            )
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
            ImmutableMap.<String, Object>of(
                "d5", "520", "d6", "620", "d7", "720", "d8", "820", "d9", "920"
            )
        )
    );
    toPersistB.add(
        new MapBasedInputRow(
            4,
            Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d5", "", "d6", "621", "d7", "", "d8", "821", "d9", "921"
            )
        )
    );
    final File tmpDirA = temporaryFolder.newFolder();
    final File tmpDirB = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex indexA = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(
        ImmutableList.of("d2", "d3", "d5", "d6", "d7", "d8", "d9"),
        ImmutableList.copyOf(adapter.getDimensionNames())
    );
    Assert.assertEquals(4, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0}, {1}, {0}, {0}, {0}, {0}, {0}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new int[][]{{1}, {2}, {0}, {0}, {1}, {1}, {1}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {1}, {1}, {2}, {2}, {2}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {0}, {2}, {0}, {3}, {3}}, boatList.get(3).getDims());

    checkBitmapIndex(Lists.newArrayList(0, 2, 3), adapter.getBitmapIndex("d2", ""));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d2", "210"));

    checkBitmapIndex(Lists.newArrayList(2, 3), adapter.getBitmapIndex("d3", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d3", "310"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d3", "311"));

    checkBitmapIndex(Lists.newArrayList(0, 1, 3), adapter.getBitmapIndex("d5", ""));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d5", "520"));

    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("d6", ""));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d6", "620"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d6", "621"));

    checkBitmapIndex(Lists.newArrayList(0, 3), adapter.getBitmapIndex("d7", ""));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d7", "710"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d7", "720"));

    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d8", ""));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d8", "810"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d8", "820"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d8", "821"));

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("d9", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("d9", "910"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("d9", "911"));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("d9", "920"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d9", "921"));
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
            ImmutableMap.<String, Object>of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistA.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
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
            ImmutableMap.<String, Object>of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistB.add(
        new MapBasedInputRow(
            4,
            Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d5", "", "d6", "621", "d7", "", "d8", "821", "d9", "921"
            )
        )
    );
    final File tmpDirA = temporaryFolder.newFolder();
    final File tmpDirB = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex indexA = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                false,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(
        ImmutableList.of("d3", "d6", "d8", "d9"),
        ImmutableList.copyOf(adapter.getDimensionNames())
    );
    Assert.assertEquals(4, boatList.size());
    Assert.assertArrayEquals(new int[][]{{1}, {0}, {0}, {0}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new int[][]{{1}, {0}, {0}, {0}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new int[][]{{1}, {0}, {0}, {0}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new int[][]{{0}, {1}, {1}, {1}}, boatList.get(3).getDims());

    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d3", ""));
    checkBitmapIndex(Lists.newArrayList(0, 1, 2), adapter.getBitmapIndex("d3", "310"));

    checkBitmapIndex(Lists.newArrayList(0, 1, 2), adapter.getBitmapIndex("d6", ""));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d6", "621"));

    checkBitmapIndex(Lists.newArrayList(0, 1, 2), adapter.getBitmapIndex("d8", ""));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d8", "821"));

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("d9", ""));
    checkBitmapIndex(Lists.newArrayList(0, 1, 2), adapter.getBitmapIndex("d9", "910"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("d9", "921"));
  }

  private void checkBitmapIndex(ArrayList<Integer> expected, BitmapValues real)
  {
    Assert.assertEquals(expected.size(), real.size());
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
            ImmutableMap.<String, Object>of("dimB", "1", "dimA", "")
        )
    );

    toPersistBA2.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimB", "dimA"),
            ImmutableMap.<String, Object>of("dimB", "", "dimA", "1")
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
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    QueryableIndex indexBA = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistBA,
                tmpDirBA,
                indexSpec
            )
        )
    );

    QueryableIndex indexBA2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistBA2,
                tmpDirBA2,
                indexSpec
            )
        )
    );

    QueryableIndex indexC = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersistC,
                tmpDirC,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB, indexBA, indexBA2),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndex merged2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB, indexBA, indexC),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged2,
                indexSpec
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<Rowboat> boatList = ImmutableList.copyOf(adapter.getRows());

    final QueryableIndexIndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(merged2);
    final List<Rowboat> boatList2 = ImmutableList.copyOf(adapter2.getRows());

    Assert.assertEquals(ImmutableList.of("dimB", "dimA"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(5, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0}, {1}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new Object[]{3L}, boatList.get(0).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {2}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(1).getMetrics());
    Assert.assertArrayEquals(new int[][]{{1}, {0}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new Object[]{3L}, boatList.get(2).getMetrics());
    Assert.assertArrayEquals(new int[][]{{2}, {0}}, boatList.get(3).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(3).getMetrics());
    Assert.assertArrayEquals(new int[][]{{3}, {0}}, boatList.get(4).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(4).getMetrics());

    checkBitmapIndex(Lists.newArrayList(2, 3, 4), adapter.getBitmapIndex("dimA", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dimA", "1"));
    checkBitmapIndex(Lists.newArrayList(1), adapter.getBitmapIndex("dimA", "2"));

    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dimB", ""));
    checkBitmapIndex(Lists.newArrayList(2), adapter.getBitmapIndex("dimB", "1"));
    checkBitmapIndex(Lists.newArrayList(3), adapter.getBitmapIndex("dimB", "2"));
    checkBitmapIndex(Lists.newArrayList(4), adapter.getBitmapIndex("dimB", "3"));


    Assert.assertEquals(ImmutableList.of("dimA", "dimB", "dimC"), ImmutableList.copyOf(adapter2.getDimensionNames()));
    Assert.assertEquals(12, boatList2.size());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {1}}, boatList2.get(0).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(0).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {2}}, boatList2.get(1).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(1).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {0}, {3}}, boatList2.get(2).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(2).getMetrics());

    Assert.assertArrayEquals(new int[][]{{0}, {1}, {0}}, boatList2.get(3).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(3).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {2}, {0}}, boatList2.get(4).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(4).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {3}, {0}}, boatList2.get(5).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(5).getMetrics());

    Assert.assertArrayEquals(new int[][]{{1}, {0}, {0}}, boatList2.get(6).getDims());
    Assert.assertArrayEquals(new Object[]{3L}, boatList2.get(6).getMetrics());
    Assert.assertArrayEquals(new int[][]{{2}, {0}, {0}}, boatList2.get(7).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(7).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {1}, {0}}, boatList2.get(8).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(8).getMetrics());

    Assert.assertArrayEquals(new int[][]{{0}, {2}, {0}}, boatList2.get(9).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(9).getMetrics());
    Assert.assertArrayEquals(new int[][]{{0}, {3}, {0}}, boatList2.get(10).getDims());
    Assert.assertArrayEquals(new Object[]{1L}, boatList2.get(10).getMetrics());
    Assert.assertArrayEquals(new int[][]{{2}, {0}, {0}}, boatList2.get(11).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList2.get(11).getMetrics());

    checkBitmapIndex(Lists.newArrayList(0, 1, 2, 3, 4, 5, 8, 9, 10), adapter2.getBitmapIndex("dimA", ""));
    checkBitmapIndex(Lists.newArrayList(6), adapter2.getBitmapIndex("dimA", "1"));
    checkBitmapIndex(Lists.newArrayList(7, 11), adapter2.getBitmapIndex("dimA", "2"));

    checkBitmapIndex(Lists.newArrayList(0, 1, 2, 6, 7, 11), adapter2.getBitmapIndex("dimB", ""));
    checkBitmapIndex(Lists.newArrayList(3, 8), adapter2.getBitmapIndex("dimB", "1"));
    checkBitmapIndex(Lists.newArrayList(4, 9), adapter2.getBitmapIndex("dimB", "2"));
    checkBitmapIndex(Lists.newArrayList(5, 10), adapter2.getBitmapIndex("dimB", "3"));

    checkBitmapIndex(Lists.newArrayList(3, 4, 5, 6, 7, 8, 9, 10, 11), adapter2.getBitmapIndex("dimC", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter2.getBitmapIndex("dimC", "1"));
    checkBitmapIndex(Lists.newArrayList(1), adapter2.getBitmapIndex("dimC", "2"));
    checkBitmapIndex(Lists.newArrayList(2), adapter2.getBitmapIndex("dimC", "3"));

  }

  @Test
  public void testMismatchedDimensions() throws IOException, IndexSizeExceededException
  {
    IncrementalIndex index1 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });
    index1.add(
        new MapBasedInputRow(
            1L,
            Lists.newArrayList("d1", "d2"),
            ImmutableMap.<String, Object>of("d1", "a", "d2", "z", "A", 1)
        )
    );
    closer.closeLater(index1);

    IncrementalIndex index2 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("C", "C")
    });
    index2.add(new MapBasedInputRow(
        1L,
        Lists.newArrayList("d1", "d2"),
        ImmutableMap.<String, Object>of("d1", "a", "d2", "z", "A", 2, "C", 100)
    ));
    closer.closeLater(index2);

    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
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
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "A", 5)
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
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "A", 5, "C", 6)
        )
    );
    closer.closeLater(index2);

    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
        new IncrementalIndexAdapter(interval, index1, factory),
        new IncrementalIndexAdapter(interval, index2, factory)

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
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(INDEX_IO.loadIndex(
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
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "A", 5)
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
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "A", 5, "C", 6)
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
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "A", 5)
        )
    );


    Interval interval = new Interval(DateTimes.EPOCH, DateTimes.nowUtc());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
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
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(INDEX_IO.loadIndex(
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
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
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
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(INDEX_IO.loadIndex(
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
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
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
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(INDEX_IO.loadIndex(
        merged)));
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
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist1,
                tmpDir,
                indexSpec
            )
        )
    );

    QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                toPersist2,
                tmpDir2,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    Iterable<Rowboat> boats = adapter.getRows();
    List<Rowboat> boatList = Lists.newArrayList(boats);

    Assert.assertEquals(ImmutableList.of("dimA", "dimB", "dimC"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(4, boatList.size());

    Assert.assertArrayEquals(new Object[]{0L, 0.0f, new int[]{2}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(0).getMetrics());

    Assert.assertArrayEquals(new Object[]{72L, 60000.789f, new int[]{3}}, boatList.get(1).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(0).getMetrics());

    Assert.assertArrayEquals(new Object[]{100L, 4000.567f, new int[]{1}}, boatList.get(2).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(1).getMetrics());

    Assert.assertArrayEquals(new Object[]{3001L, 1.2345f, new int[]{0}}, boatList.get(3).getDims());
    Assert.assertArrayEquals(new Object[]{2L}, boatList.get(2).getMetrics());
  }

  private IncrementalIndex getIndexWithNumericDims() throws Exception
  {
    IncrementalIndex index = getIndexWithDimsFromSchemata(
        Arrays.asList(
            new LongDimensionSchema("dimA"),
            new FloatDimensionSchema("dimB"),
            new StringDimensionSchema("dimC")
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.<String, Object>of("dimA", 100L, "dimB", 4000.567, "dimC", "Hello")
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.<String, Object>of("dimA", 72L, "dimB", 60000.789, "dimC", "World")
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.<String, Object>of("dimA", 3001L, "dimB", 1.2345, "dimC", "Foobar")
        )
    );

    index.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("dimA", "dimB", "dimC"),
            ImmutableMap.<String, Object>of("dimC", "Nully Row")
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
        Lists.newArrayList("d1", "d2"),
        ImmutableMap.<String, Object>of("d1", "a", "d2", "", "A", 1)
    ));

    index1.add(new MapBasedInputRow(
        1L,
        Lists.newArrayList("d1", "d2"),
        ImmutableMap.<String, Object>of("d1", "b", "d2", "", "A", 1)
    ));

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        INDEX_IO.loadIndex(
            indexMerger.persist(
                index1,
                tempDir,
                indexSpec
            )
        )
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
            ImmutableMap.<String, Object>of("d1", "100", "d2", "4000", "d3", "30000")
        )
    );

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.<String, Object>of("d1", "300", "d2", "2000", "d3", "40000")
        )
    );

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.<String, Object>of("d1", "200", "d2", "3000", "d3", "50000")
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
      index.add(
          new MapBasedInputRow(
              1,
              Arrays.asList(dimName),
              ImmutableMap.<String, Object>of(dimName, val)
          )
      );
    }
  }

  private IncrementalIndex getIndexWithDims(List<String> dims)
  {
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dims), null, null))
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
  public void testDictIdSeeker() throws Exception
  {
    IntBuffer dimConversions = ByteBuffer.allocateDirect(3 * Ints.BYTES).asIntBuffer();
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
      indexMerger.persist(
          toPersist,
          tempDir,
          indexSpec
      );
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
            ImmutableMap.<String, Object>of(
                "dim1", Arrays.asList("x", "a", "a", "b"),
                "dim2", Arrays.asList("a", "x", "b", "x")
            )
        ),
        new MapBasedInputRow(
            1,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.<String, Object>of(
                "dim1", Arrays.asList("a", "b", "x"),
                "dim2", Arrays.asList("x", "a", "b")
            )
        )
    };

    List<DimensionSchema> schema;
    QueryableIndex index;
    QueryableIndexIndexableAdapter adapter;
    List<Rowboat> boatList;

    // xaab-axbx + abx-xab --> aabx-abxx + abx-abx --> abx-abx + aabx-abxx
    schema = DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2"), MultiValueHandling.SORTED_ARRAY);
    index = persistAndLoad(schema, rows);
    adapter = new QueryableIndexIndexableAdapter(index);
    boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    Assert.assertEquals(2, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0, 1, 2}, {0, 1, 2}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new int[][]{{0, 0, 1, 2}, {0, 1, 2, 2}}, boatList.get(1).getDims());

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("dim1", ""));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim1", "a"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim1", "b"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim1", "x"));

    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim2", "a"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim2", "b"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim2", "x"));

    // xaab-axbx + abx-xab --> abx-abx + abx-abx --> abx-abx
    schema = DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2"), MultiValueHandling.SORTED_SET);
    index = persistAndLoad(schema, rows);

    Assert.assertEquals(1, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    adapter = new QueryableIndexIndexableAdapter(index);
    boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(1, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0, 1, 2}, {0, 1, 2}}, boatList.get(0).getDims());

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("dim1", ""));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim1", "a"));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim1", "b"));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim1", "x"));

    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim2", "a"));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim2", "b"));
    checkBitmapIndex(Lists.newArrayList(0), adapter.getBitmapIndex("dim2", "x"));

    // xaab-axbx + abx-xab --> abx-xab + xaab-axbx
    schema = DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "dim2"), MultiValueHandling.ARRAY);
    index = persistAndLoad(schema, rows);

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    adapter = new QueryableIndexIndexableAdapter(index);
    boatList = ImmutableList.copyOf(adapter.getRows());

    Assert.assertEquals(2, boatList.size());
    Assert.assertArrayEquals(new int[][]{{0, 1, 2}, {2, 0, 1}}, boatList.get(0).getDims());
    Assert.assertArrayEquals(new int[][]{{2, 0, 0, 1}, {0, 2, 1, 2}}, boatList.get(1).getDims());

    checkBitmapIndex(new ArrayList<Integer>(), adapter.getBitmapIndex("dim1", ""));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim1", "a"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim1", "b"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim1", "x"));

    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim2", "a"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim2", "b"));
    checkBitmapIndex(Lists.newArrayList(0, 1), adapter.getBitmapIndex("dim2", "x"));
  }

  private QueryableIndex persistAndLoad(List<DimensionSchema> schema, InputRow... rows) throws IOException
  {
    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null, new DimensionsSpec(schema, null, null));
    for (InputRow row : rows) {
      toPersist.add(row);
    }

    final File tempDir = temporaryFolder.newFolder();
    return closer.closeLater(INDEX_IO.loadIndex(indexMerger.persist(toPersist, tempDir, indexSpec)));
  }
}
