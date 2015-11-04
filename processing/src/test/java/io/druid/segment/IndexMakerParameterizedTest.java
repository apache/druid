/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.SimpleDictionaryEncodedColumn;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Ignore
/*
* IndexMaker is not yet ready for production. Enable this test when IndexMaker is ready.
*/

@RunWith(Parameterized.class)
public class IndexMakerParameterizedTest
{

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public final CloserRule closer = new CloserRule(false);

  private final static IndexMaker INDEX_MAKER = TestHelper.getTestIndexMaker();
  private final static IndexIO INDEX_IO = TestHelper.getTestIndexIO();


  @Parameterized.Parameters(name = "{index}: bitmap={0}, metric compression={1}, dimension compression={2}")
  public static Collection<Object[]> data()
  {
    return Collections2.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
                ImmutableSet.of(new RoaringBitmapSerdeFactory(), new ConciseBitmapSerdeFactory()),
                ImmutableSet.of(
                    CompressedObjectStrategy.CompressionStrategy.LZ4,
                    CompressedObjectStrategy.CompressionStrategy.LZF
                ),
                ImmutableSet.of(
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

  public IndexMakerParameterizedTest(
      BitmapSerdeFactory bitmapSerdeFactory,
      CompressedObjectStrategy.CompressionStrategy compressionStrategy,
      CompressedObjectStrategy.CompressionStrategy dimCompressionStrategy
  )
  {
    this.indexSpec = makeIndexSpec(bitmapSerdeFactory, compressionStrategy, dimCompressionStrategy);
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
            INDEX_MAKER.persist(
                toPersist,
                tempDir,
                null,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
    Assert.assertEquals(3, index.getColumnNames().size());

    assertDimCompression(index, indexSpec.getDimensionCompressionStrategy());
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
            INDEX_MAKER.persist(
                toPersist1,
                tempDir1,
                null,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());

    QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MAKER.persist(
                toPersist2,
                tempDir2,
                null,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, index2.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index2.getAvailableDimensions()));
    Assert.assertEquals(3, index2.getColumnNames().size());

    QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MAKER.mergeQueryableIndex(
                Arrays.asList(index1, index2),
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
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
            INDEX_MAKER.persist(
                toPersist1,
                tmpDir1,
                null,
                indexSpec
            )
        )
    );
    final QueryableIndex index2 = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MAKER.persist(
                toPersist1,
                tmpDir2,
                null,
                indexSpec
            )
        )
    );
    final QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MAKER.mergeQueryableIndex(Arrays.asList(index1, index2), new AggregatorFactory[]{}, tmpDir3, indexSpec)
        )
    );

    Assert.assertEquals(1, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index1.getAvailableDimensions()));

    Assert.assertEquals(1, index2.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index2.getAvailableDimensions()));

    Assert.assertEquals(1, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
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
            INDEX_MAKER.persist(
                toPersist1,
                tempDir1,
                null,
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
            INDEX_MAKER.mergeQueryableIndex(
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
            INDEX_MAKER.append(
                ImmutableList.<IndexableAdapter>of(incrementalAdapter), tempDir1, indexSpec
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
            INDEX_MAKER.mergeQueryableIndex(
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
            INDEX_MAKER.persist(
                toPersist1,
                tempDir1,
                null,
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


    QueryableIndex merged = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MAKER.mergeQueryableIndex(
                ImmutableList.of(index1),
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
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
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
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
            INDEX_MAKER.persist(
                toPersist1,
                tempDir1,
                null,
                indexSpec
            )
        )
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    INDEX_IO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(3, index1.getColumnNames().size());


    QueryableIndex converted = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MAKER.convert(
                tempDir1,
                convertDir,
                indexSpec
            )
        )
    );

    Assert.assertEquals(2, converted.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(converted.getAvailableDimensions()));
    Assert.assertEquals(3, converted.getColumnNames().size());

    INDEX_IO.validateTwoSegments(tempDir1, convertDir);

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(converted, indexSpec.getDimensionCompressionStrategy());
  }


  @Test
  public void testConvertDifferent() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
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
            INDEX_MAKER.persist(
                toPersist1,
                tempDir1,
                null,
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

    QueryableIndex converted = closer.closeLater(INDEX_IO.loadIndex(INDEX_MAKER.convert(tempDir1, convertDir, newSpec)));

    Assert.assertEquals(2, converted.getColumn(Column.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(converted.getAvailableDimensions()));
    Assert.assertEquals(3, converted.getColumnNames().size());

    INDEX_IO.validateTwoSegments(tempDir1, convertDir);

    assertDimCompression(index1, indexSpec.getDimensionCompressionStrategy());
    assertDimCompression(converted, newSpec.getDimensionCompressionStrategy());
  }

  private void assertDimCompression(QueryableIndex index, CompressedObjectStrategy.CompressionStrategy expectedStrategy)
      throws Exception
  {
    // Java voodoo

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
}
