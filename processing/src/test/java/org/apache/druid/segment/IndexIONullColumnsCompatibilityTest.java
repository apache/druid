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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.IndexIO.IndexLoader;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IndexIONullColumnsCompatibilityTest extends InitializedNullHandlingTest
{
  private static final Interval INTERVAL = Intervals.of("2022-01/P1D");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File segmentDir;

  @Before
  public void setup() throws IOException
  {
    final IndexMerger indexMerger = TestHelper.getTestIndexMergerV9(
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    final IncrementalIndex incrementalIndex = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(INTERVAL.getStart().getMillis())
                .withMetrics(new CountAggregatorFactory("count"))
                .withDimensionsSpec(
                    new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("dim1", "unknownDim", "dim2")))
                )
                .build()
        )
        .setMaxRowCount(1000000)
        .build();

    incrementalIndex.add(
        new MapBasedInputRow(
            INTERVAL.getStart(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableMap.of("dim1", "val1", "dim2", "val2")
        )
    );
    segmentDir = indexMerger.persist(
        incrementalIndex,
        temporaryFolder.newFolder(),
        new IndexSpec(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
  }

  @Test
  public void testV9LoaderThatReadsEmptyColumns() throws IOException
  {
    QueryableIndex queryableIndex = TestHelper.getTestIndexIO().loadIndex(segmentDir);
    Assert.assertEquals(
        ImmutableList.of("dim1", "unknownDim", "dim2"),
        Lists.newArrayList(queryableIndex.getAvailableDimensions().iterator())
    );
  }

  @Test
  public void testV9LoaderThatIgnoresmptyColumns() throws IOException
  {
    QueryableIndex queryableIndex = new V9IndexLoaderExceptEmptyColumns(TestHelper.NO_CACHE_COLUMN_CONFIG).load(
        segmentDir,
        TestHelper.makeJsonMapper(),
        false,
        SegmentLazyLoadFailCallback.NOOP
    );
    Assert.assertEquals(
        ImmutableList.of("dim1", "dim2"),
        Lists.newArrayList(queryableIndex.getAvailableDimensions().iterator())
    );
  }

  /**
   * A duplicate of V9IndexLoader except for reading null-only columns.
   * This class is copied from the version of {@link IndexIO}
   * corresponding to the commit {@code fc76b014d15caf5662800706237eb336aef426e4}
   * and adjusted a bit to fix some method signature changes.
   */
  private static class V9IndexLoaderExceptEmptyColumns implements IndexLoader
  {
    private final ColumnConfig columnConfig;

    private V9IndexLoaderExceptEmptyColumns(ColumnConfig columnConfig)
    {
      this.columnConfig = columnConfig;
    }

    @Override
    public QueryableIndex load(File inDir, ObjectMapper mapper, boolean lazy, SegmentLazyLoadFailCallback loadFailed) throws IOException
    {
      final int theVersion = Ints.fromByteArray(Files.toByteArray(new File(inDir, "version.bin")));
      if (theVersion != IndexIO.V9_VERSION) {
        throw new IAE("Expected version[9], got[%d]", theVersion);
      }

      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);

      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
      /**
       * Index.drd should consist of the segment version, the columns and dimensions of the segment as generic
       * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
       */
      final GenericIndexed<String> cols = GenericIndexed.read(
          indexBuffer,
          GenericIndexed.STRING_STRATEGY,
          smooshedFiles
      );
      final GenericIndexed<String> dims = GenericIndexed.read(
          indexBuffer,
          GenericIndexed.STRING_STRATEGY,
          smooshedFiles
      );
      final Interval dataInterval = Intervals.utc(indexBuffer.getLong(), indexBuffer.getLong());
      final BitmapSerdeFactory segmentBitmapSerdeFactory;

      /**
       * This is a workaround for the fact that in v8 segments, we have no information about the type of bitmap
       * index to use. Since we cannot very cleanly build v9 segments directly, we are using a workaround where
       * this information is appended to the end of index.drd.
       */
      if (indexBuffer.hasRemaining()) {
        segmentBitmapSerdeFactory = mapper.readValue(IndexIO.SERIALIZER_UTILS.readString(indexBuffer), BitmapSerdeFactory.class);
      } else {
        segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
      }

      Metadata metadata = null;
      ByteBuffer metadataBB = smooshedFiles.mapFile("metadata.drd");
      if (metadataBB != null) {
        try {
          metadata = mapper.readValue(
              IndexIO.SERIALIZER_UTILS.readBytes(metadataBB, metadataBB.remaining()),
              Metadata.class
          );
        }
        catch (IOException ex) {
          throw new IOException("Failed to read metadata", ex);
        }
      }

      Map<String, Supplier<ColumnHolder>> columns = new HashMap<>();

      for (String columnName : cols) {
        if (Strings.isNullOrEmpty(columnName)) {
          continue;
        }

        ByteBuffer colBuffer = smooshedFiles.mapFile(columnName);

        if (lazy) {
          columns.put(columnName, Suppliers.memoize(
              () -> {
                try {
                  return deserializeColumn(mapper, colBuffer, smooshedFiles);
                }
                catch (IOException | RuntimeException e) {
                  loadFailed.execute();
                  throw Throwables.propagate(e);
                }
              }
          ));
        } else {
          ColumnHolder columnHolder = deserializeColumn(mapper, colBuffer, smooshedFiles);
          columns.put(columnName, () -> columnHolder);
        }

      }

      ByteBuffer timeBuffer = smooshedFiles.mapFile("__time");

      if (lazy) {
        columns.put(ColumnHolder.TIME_COLUMN_NAME, Suppliers.memoize(
            () -> {
              try {
                return deserializeColumn(mapper, timeBuffer, smooshedFiles);
              }
              catch (IOException | RuntimeException e) {
                loadFailed.execute();
                throw Throwables.propagate(e);
              }
            }
        ));
      } else {
        ColumnHolder columnHolder = deserializeColumn(mapper, timeBuffer, smooshedFiles);
        columns.put(ColumnHolder.TIME_COLUMN_NAME, () -> columnHolder);
      }

      final QueryableIndex index = new SimpleQueryableIndex(
          dataInterval,
          dims,
          segmentBitmapSerdeFactory.getBitmapFactory(),
          columns,
          smooshedFiles,
          metadata,
          lazy
      );

      return index;
    }

    private ColumnHolder deserializeColumn(
        ObjectMapper mapper,
        ByteBuffer byteBuffer,
        SmooshedFileMapper smooshedFiles
    ) throws IOException
    {
      ColumnDescriptor serde = mapper.readValue(
          IndexIO.SERIALIZER_UTILS.readString(byteBuffer), ColumnDescriptor.class
      );
      return serde.read(byteBuffer, columnConfig, smooshedFiles);
    }
  }
}
