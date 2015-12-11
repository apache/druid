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
import com.metamx.collections.bitmap.RoaringBitmapFactory;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.SimpleDictionaryEncodedColumn;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class IndexMergerTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected IndexMerger INDEX_MERGER;
  private final static IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  @Parameterized.Parameters(name = "{index}: useV9={0}, bitmap={1}, metric compression={2}, dimension compression={3}")
  public static Collection<Object[]> data()
  {
    return Collections2.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
                ImmutableSet.of(
                    true,
                    false
                ),
                ImmutableSet.of(
                    new RoaringBitmapSerdeFactory(),
                    new ConciseBitmapSerdeFactory()
                ),
                ImmutableSet.of(
                    CompressedObjectStrategy.CompressionStrategy.UNCOMPRESSED,
                    CompressedObjectStrategy.CompressionStrategy.LZ4,
                    CompressedObjectStrategy.CompressionStrategy.LZF
                ),
                ImmutableSet.of(
                    CompressedObjectStrategy.CompressionStrategy.UNCOMPRESSED,
                    CompressedObjectStrategy.CompressionStrategy.LZ4,
                    CompressedObjectStrategy.CompressionStrategy.LZF
                )
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
      CompressedObjectStrategy.CompressionStrategy dimCompressionStrategy
  )
  {
    if (bitmapSerdeFactory != null || compressionStrategy != null) {
      return new IndexSpec(
          bitmapSerdeFactory,
          compressionStrategy.name().toLowerCase(),
          dimCompressionStrategy.name().toLowerCase()
      );
    } else {
      return new IndexSpec();
    }
  }

  private final IndexSpec indexSpec;
  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IndexMergerTest(
      boolean useV9,
      BitmapSerdeFactory bitmapSerdeFactory,
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      CompressedObjectStrategy.CompressionStrategy dimCompressionStrategy
  )
  {
    this.indexSpec = makeIndexSpec(bitmapSerdeFactory, compressionStrategy, dimCompressionStrategy);
    if (useV9) {
      INDEX_MERGER = TestHelper.getTestIndexMergerV9();
    } else {
      INDEX_MERGER = TestHelper.getTestIndexMerger();
    }
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
            INDEX_MERGER.persist(
                toPersist,
                tempDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    assertDimCompression(index, indexSpec.getDimensionCompressionStrategy());

    Assert.assertArrayEquals(
        IncrementalIndexTest.getDefaultCombiningAggregatorFactories(),
        index.getMetadata().getAggregators()
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
            INDEX_MERGER.persist(
                toPersist,
                tempDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());
    assertDimCompression(index, indexSpec.getDimensionCompressionStrategy());

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
            INDEX_MERGER.persist(
                toPersist,
                tempDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    assertDimCompression(index, indexSpec.getDimensionCompressionStrategy());

    Assert.assertEquals(
        new Metadata()
            .setAggregators(
                IncrementalIndexTest.getDefaultCombiningAggregatorFactories()
            )
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

    IncrementalIndex toPersist2 = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        1000
    );

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
            INDEX_MERGER.persist(
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
            INDEX_MERGER.persist(
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
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                mergedAggregators,
                mergedDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(3, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
    Assert.assertEquals(3, merged.getColumnNames().size());
    assertDimCompression(index2, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(merged, indexSpec.getDimensionCompressionStrategy());

    Assert.assertArrayEquals(
        getCombiningAggregators(mergedAggregators),
        merged.getMetadata().getAggregators()
    );
  }

  @Test
  public void testPersistEmptyColumn() throws Exception
  {
    final IncrementalIndex toPersist1 = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{},
        10
    );
    final IncrementalIndex toPersist2 = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{},
        10
    );
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
            INDEX_MERGER.persist(
                toPersist1,
                tmpDir1,
                indexSpec
            )
        )
    );
    final QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersist2,
                tmpDir2,
                indexSpec
            )
        )
    );
    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(index1, index2),
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

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(index2, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(merged, indexSpec.getDimensionCompressionStrategy());
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
            INDEX_MERGER.persist(
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
            INDEX_MERGER.mergeQueryableIndex(
                ImmutableList.of(index1),
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

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(merged, indexSpec.getDimensionCompressionStrategy());
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
            INDEX_MERGER.append(
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
            INDEX_MERGER.mergeQueryableIndex(
                ImmutableList.of(index1),
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

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(merged, indexSpec.getDimensionCompressionStrategy());

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
            INDEX_MERGER.persist(
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
        "lz4".equals(indexSpec.getDimensionCompression()) ? "lzf" : "lz4",
        "lz4".equals(indexSpec.getMetricCompression()) ? "lzf" : "lz4"
    );

    AggregatorFactory[] mergedAggregators = new AggregatorFactory[]{new CountAggregatorFactory("count")};
    QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                ImmutableList.of(index1),
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

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(merged, newSpec.getDimensionCompressionStrategy());
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
        INDEX_IO.loadIndex(INDEX_MERGER.persist(toPersist1, tempDir1, indexSpec))
    );

    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    INDEX_IO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(4, index1.getColumnNames().size());


    QueryableIndex converted = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.convert(
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

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(converted, indexSpec.getDimensionCompressionStrategy());

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
            INDEX_MERGER.persist(
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
        "lz4".equals(indexSpec.getDimensionCompression()) ? "lzf" : "lz4",
        "lz4".equals(indexSpec.getMetricCompression()) ? "lzf" : "lz4"
    );

    QueryableIndex converted = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.convert(
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

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(converted, newSpec.getDimensionCompressionStrategy());

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

    Object encodedColumn = index.getColumn("dim2").getDictionaryEncoding();
    Field field = SimpleDictionaryEncodedColumn.class.getDeclaredField("column");
    field.setAccessible(true);

    Object obj = field.get(encodedColumn);
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
            INDEX_MERGER.persist(
                toPersist1,
                tmpDir,
                indexSpec
            )
        )
    );

    QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersist2,
                tmpDir2,
                indexSpec
            )
        )
    );

    QueryableIndex index3 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersist3,
                tmpDir3,
                indexSpec
            )
        )
    );


    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(index1, index2, index3),
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
        .withDimensionsSpec(new DimensionsSpec(Arrays.asList("dimA", "dimB", "dimC"), null, null))
        .withMinTimestamp(0L)
        .withQueryGranularity(QueryGranularity.NONE)
        .withMetrics(new AggregatorFactory[]{new CountAggregatorFactory("count")})
        .build();


    IncrementalIndex toPersist1 = new OnheapIncrementalIndex(schema, 1000);
    IncrementalIndex toPersist2 = new OnheapIncrementalIndex(schema, 1000);
    IncrementalIndex toPersist3 = new OnheapIncrementalIndex(schema, 1000);

    addDimValuesToIndex(toPersist1, "dimA", Arrays.asList("1", "2"));
    addDimValuesToIndex(toPersist2, "dimA", Arrays.asList("1", "2"));
    addDimValuesToIndex(toPersist3, "dimC", Arrays.asList("1", "2"));


    final File tmpDir = temporaryFolder.newFolder();
    final File tmpDir2 = temporaryFolder.newFolder();
    final File tmpDir3 = temporaryFolder.newFolder();
    final File tmpDirMerged = temporaryFolder.newFolder();

    QueryableIndex index1 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersist1,
                tmpDir,
                indexSpec
            )
        )
    );

    QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersist2,
                tmpDir2,
                indexSpec
            )
        )
    );

    QueryableIndex index3 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersist3,
                tmpDir3,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(index1, index2, index3),
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
            INDEX_MERGER.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    QueryableIndex indexB2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersistB2,
                tmpDirB2,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndex merged2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(indexA, indexB2),
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

    IncrementalIndex toPersistA = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        1000
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
    toPersistA.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.<String, Object>of(
                "d2", "210", "d3", "311", "d7", "710", "d8", "810", "d9", "911"
            )
        )
    );

    IncrementalIndex toPersistB = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        1000
    );
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
            INDEX_MERGER.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
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

  private void checkBitmapIndex(ArrayList<Integer> expected, IndexedInts real)
  {
    Assert.assertEquals(expected.size(), real.size());
    int i = 0;
    for (Object index : real) {
      Assert.assertEquals(expected.get(i++), index);
    }
  }

  @Test
  public void testMergeWithSupersetOrdering() throws Exception
  {
    IncrementalIndex toPersistA = getSingleDimIndex("dimA", Arrays.asList("1", "2"));
    IncrementalIndex toPersistB = getSingleDimIndex("dimB", Arrays.asList("1", "2", "3"));

    IncrementalIndex toPersistBA = getSingleDimIndex("dimB", Arrays.asList("1", "2", "3"));
    addDimValuesToIndex(toPersistBA, "dimA", Arrays.asList("1", "2"));

    IncrementalIndex toPersistBA2 = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        1000
    );

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
            INDEX_MERGER.persist(
                toPersistA,
                tmpDirA,
                indexSpec
            )
        )
    );

    QueryableIndex indexB = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersistB,
                tmpDirB,
                indexSpec
            )
        )
    );

    QueryableIndex indexBA = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersistBA,
                tmpDirBA,
                indexSpec
            )
        )
    );

    QueryableIndex indexBA2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersistBA2,
                tmpDirBA2,
                indexSpec
            )
        )
    );

    QueryableIndex indexC = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersistC,
                tmpDirC,
                indexSpec
            )
        )
    );

    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(indexA, indexB, indexBA, indexBA2),
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec
            )
        )
    );

    final QueryableIndex merged2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(indexA, indexB, indexBA, indexC),
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

  public void testMismatchedDimensions() throws IOException, IndexSizeExceededException
  {
    IncrementalIndex index1 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A")
    });
    index1.add(new MapBasedInputRow(1L, Lists.newArrayList("d1", "d2"), ImmutableMap.<String, Object>of("d1", "a", "d2", "z", "A", 1)));
    closer.closeLater(index1);

    IncrementalIndex index2 = IncrementalIndexTest.createIndex(new AggregatorFactory[]{
        new LongSumAggregatorFactory("A", "A"),
        new LongSumAggregatorFactory("C", "C")
    });
    index2.add(new MapBasedInputRow(1L, Lists.newArrayList("d2"), ImmutableMap.<String, Object>of("d2", "z", "A", 2, "C", 100)));
    closer.closeLater(index2);

    Interval interval = new Interval(0, new DateTime().getMillis());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
        new IncrementalIndexAdapter(interval, index1, factory),
        new IncrementalIndexAdapter(interval, index2, factory)
    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    INDEX_MERGER.merge(
        toMerge,
        new AggregatorFactory[] {
            new LongSumAggregatorFactory("A", "A"),
            new LongSumAggregatorFactory("C", "C"),
        },
        tmpDirMerged,
        indexSpec
    );
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


    Interval interval = new Interval(0, new DateTime().getMillis());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
        new IncrementalIndexAdapter(interval, index1, factory),
        new IncrementalIndexAdapter(interval, index2, factory),
        new IncrementalIndexAdapter(interval, index3, factory),
        new IncrementalIndexAdapter(interval, index4, factory),
        new IncrementalIndexAdapter(interval, index5, factory)
    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    File merged = INDEX_MERGER.merge(
        toMerge,
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
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(INDEX_IO.loadIndex(merged)));
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


    Interval interval = new Interval(0, new DateTime().getMillis());
    RoaringBitmapFactory factory = new RoaringBitmapFactory();
    ArrayList<IndexableAdapter> toMerge = Lists.<IndexableAdapter>newArrayList(
        new IncrementalIndexAdapter(interval, index2, factory)
    );

    final File tmpDirMerged = temporaryFolder.newFolder();

    final File merged = INDEX_MERGER.merge(
        toMerge,
        new AggregatorFactory[] {
            new LongSumAggregatorFactory("B", "B"),
            new LongSumAggregatorFactory("A", "A"),
            new LongSumAggregatorFactory("D", "D")
        },
        tmpDirMerged,
        indexSpec
    );
    final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(closer.closeLater(INDEX_IO.loadIndex(merged)));
    Assert.assertEquals(ImmutableSet.of("A", "B", "C"), ImmutableSet.copyOf(adapter.getAvailableMetrics()));
  }

  private IncrementalIndex getIndexD3() throws Exception
  {
    IncrementalIndex toPersist1 = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        1000
    );

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
    IncrementalIndex toPersist1 = new OnheapIncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        1000
    );

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
    IncrementalIndexSchema schema = new IncrementalIndexSchema(
        0L,
        QueryGranularity.NONE,
        new DimensionsSpec(dims, null, null),
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );

    return new OnheapIncrementalIndex(
        schema,
        1000
    );
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
}
