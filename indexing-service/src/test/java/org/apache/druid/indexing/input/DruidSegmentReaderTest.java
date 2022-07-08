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

package org.apache.druid.indexing.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.druid.common.config.NullHandlingTest;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntity.CleanableFile;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.hll.HyperLogLogHash;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertThrows;

public class DruidSegmentReaderTest extends NullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File segmentDirectory;

  private final IndexIO indexIO = TestHelper.getTestIndexIO();

  @Before
  public void setUp() throws IOException
  {
    // Write a segment with two rows in it, with columns: s (string), d (double), cnt (long), met_s (complex).
    final IncrementalIndex incrementalIndex =
        IndexBuilder.create()
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withDimensionsSpec(
                                new DimensionsSpec(
                                    ImmutableList.of(
                                        StringDimensionSchema.create("s"),
                                        new DoubleDimensionSchema("d")
                                    )
                                )
                            )
                            .withMetrics(
                                new CountAggregatorFactory("cnt"),
                                new HyperUniquesAggregatorFactory("met_s", "s")
                            )
                            .withRollup(false)
                            .build()
                    )
                    .rows(
                        ImmutableList.of(
                            new MapBasedInputRow(
                                DateTimes.of("2000"),
                                ImmutableList.of("s", "d"),
                                ImmutableMap.<String, Object>builder()
                                    .put("s", "foo")
                                    .put("d", 1.23)
                                    .build()
                            ),
                            new MapBasedInputRow(
                                DateTimes.of("2000T01"),
                                ImmutableList.of("s", "d"),
                                ImmutableMap.<String, Object>builder()
                                    .put("s", "bar")
                                    .put("d", 4.56)
                                    .build()
                            )
                        )
                    )
                    .buildIncrementalIndex();

    segmentDirectory = temporaryFolder.newFolder();

    try {
      TestHelper.getTestIndexMergerV9(
          OnHeapMemorySegmentWriteOutMediumFactory.instance()
      ).persist(
          incrementalIndex,
          segmentDirectory,
          new IndexSpec(),
          null
      );
    }
    finally {
      incrementalIndex.close();
    }
  }

  @Test
  public void testReader() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("__time", "millis", DateTimes.of("1971")),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.all(),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("2000"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("foo"))
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("2000T01"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T01").getMillis())
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("bar"))
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testDruidTombstoneSegmentReader() throws IOException
  {
    final DruidTombstoneSegmentReader reader = new DruidTombstoneSegmentReader(
        makeTombstoneInputEntity(Intervals.of("2000/P1D"))
    );

    Assert.assertFalse(reader.intermediateRowIterator().hasNext());
    Assert.assertEquals(
        Collections.emptyList(),
        readRows(reader)
    );
  }

  @Test
  public void testDruidTombstoneSegmentReaderBadEntity()
  {
    assertThrows(ClassCastException.class, () -> {
      new DruidTombstoneSegmentReader(
          new FileEntity(null));
    });
  }

  @Test
  public void testDruidTombstoneSegmentReaderNotCreatedFromTombstone()
  {
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      new DruidTombstoneSegmentReader(makeInputEntity(Intervals.of("2000/P1D")));
    });
    String expectedMessage =
        "DruidSegmentInputEntity must be created from a tombstone but is not.";
    String actualMessage = exception.getMessage();
    Assert.assertEquals(expectedMessage, actualMessage);

  }

  @Test
  public void testReaderAutoTimestampFormat() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("__time", "auto", DateTimes.of("1971")),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.all(),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("2000"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("foo"))
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("2000T01"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T01").getMillis())
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("bar"))
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testReaderWithDimensionExclusions() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("__time", "millis", DateTimes.of("1971")),
        DimensionsSpec.builder().setDimensionExclusions(ImmutableList.of("__time", "s", "cnt", "met_s")).build(),
        ColumnsFilter.all(),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("2000"),
                ImmutableList.of("d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("foo"))
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("2000T01"),
                ImmutableList.of("d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T01").getMillis())
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("bar"))
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testReaderWithInclusiveColumnsFilter() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("__time", "millis", DateTimes.of("1971")),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.inclusionBased(ImmutableSet.of("__time", "s", "d")),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("2000"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("2000T01"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T01").getMillis())
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testReaderWithInclusiveColumnsFilterNoTimestamp() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("__time", "millis", DateTimes.of("1971")),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.inclusionBased(ImmutableSet.of("s", "d")),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("1971"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("1971"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testReaderWithFilter() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("__time", "millis", DateTimes.of("1971")),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.all(),
        new SelectorDimFilter("d", "1.23", null),
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("2000"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("foo"))
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testReaderTimestampFromDouble() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("d", "posix", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.all(),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("1970-01-01T00:00:01.000Z"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("foo"))
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("1970-01-01T00:00:04.000Z"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T01").getMillis())
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("bar"))
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testReaderTimestampAsPosixIncorrectly() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec("__time", "posix", null),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.all(),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("31969-04-01T00:00:00.000Z"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("foo"))
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("31969-05-12T16:00:00.000Z"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T01").getMillis())
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("bar"))
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testReaderTimestampSpecDefault() throws IOException
  {
    final DruidSegmentReader reader = new DruidSegmentReader(
        makeInputEntity(Intervals.of("2000/P1D")),
        indexIO,
        new TimestampSpec(null, null, DateTimes.of("1971")),
        new DimensionsSpec(
            ImmutableList.of(
                StringDimensionSchema.create("s"),
                new DoubleDimensionSchema("d")
            )
        ),
        ColumnsFilter.all(),
        null,
        temporaryFolder.newFolder()
    );

    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedInputRow(
                DateTimes.of("1971"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T").getMillis())
                    .put("s", "foo")
                    .put("d", 1.23d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("foo"))
                    .build()
            ),
            new MapBasedInputRow(
                DateTimes.of("1971"),
                ImmutableList.of("s", "d"),
                ImmutableMap.<String, Object>builder()
                    .put("__time", DateTimes.of("2000T01").getMillis())
                    .put("s", "bar")
                    .put("d", 4.56d)
                    .put("cnt", 1L)
                    .put("met_s", makeHLLC("bar"))
                    .build()
            )
        ),
        readRows(reader)
    );
  }

  @Test
  public void testMakeCloseableIteratorFromSequenceAndSegmentFileCloseYielderOnClose() throws IOException
  {
    MutableBoolean isSequenceClosed = new MutableBoolean(false);
    MutableBoolean isFileClosed = new MutableBoolean(false);
    Sequence<Map<String, Object>> sequence = new BaseSequence<>(
        new IteratorMaker<Map<String, Object>, Iterator<Map<String, Object>>>()
        {
          @Override
          public Iterator<Map<String, Object>> make()
          {
            return Collections.emptyIterator();
          }

          @Override
          public void cleanup(Iterator<Map<String, Object>> iterFromMake)
          {
            isSequenceClosed.setValue(true);
          }
        }
    );
    CleanableFile cleanableFile = new CleanableFile()
    {
      @Override
      public File file()
      {
        return null;
      }

      @Override
      public void close()
      {
        isFileClosed.setValue(true);
      }
    };
    try (CloseableIterator<Map<String, Object>> iterator =
             DruidSegmentReader.makeCloseableIteratorFromSequenceAndSegmentFile(sequence, cleanableFile)) {
      while (iterator.hasNext()) {
        iterator.next();
      }
    }
    Assert.assertTrue("File is not closed", isFileClosed.booleanValue());
    Assert.assertTrue("Sequence is not closed", isSequenceClosed.booleanValue());
  }

  private DruidSegmentInputEntity makeInputEntity(final Interval interval)
  {
    return makeInputEntity(interval, segmentDirectory);
  }

  public static DruidSegmentInputEntity makeInputEntity(final Interval interval, final File segmentDirectory)
  {
    return new DruidSegmentInputEntity(
        new SegmentCacheManager()
        {
          @Override
          public boolean isSegmentCached(DataSegment segment)
          {
            throw new UnsupportedOperationException("unused");
          }

          @Override
          public File getSegmentFiles(DataSegment segment)
          {
            return segmentDirectory;
          }

          @Override
          public void cleanup(DataSegment segment)
          {
            throw new UnsupportedOperationException("unused");
          }

          @Override
          public boolean reserve(DataSegment segment)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean release(DataSegment segment)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void loadSegmentIntoPageCache(DataSegment segment, ExecutorService exec)
          {
            throw new UnsupportedOperationException();
          }
        },
        DataSegment.builder()
                   .dataSource("ds")
                   .dimensions(ImmutableList.of("s", "d"))
                   .metrics(ImmutableList.of("cnt", "met_s"))
                   .interval(Intervals.of("2000/P1D"))
                   .version("1")
                   .size(0)
                   .build(),
        interval
    );
  }

  public static DruidSegmentInputEntity makeTombstoneInputEntity(final Interval interval)
  {
    return new DruidSegmentInputEntity(
        new SegmentCacheManager()
        {
          @Override
          public boolean isSegmentCached(DataSegment segment)
          {
            throw new UnsupportedOperationException("unused");
          }

          @Override
          public File getSegmentFiles(DataSegment segment)
          {
            throw new UnsupportedOperationException("unused");
          }

          @Override
          public void cleanup(DataSegment segment)
          {
            throw new UnsupportedOperationException("unused");
          }

          @Override
          public boolean reserve(DataSegment segment)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean release(DataSegment segment)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void loadSegmentIntoPageCache(DataSegment segment, ExecutorService exec)
          {
            throw new UnsupportedOperationException();

          }
        },
        DataSegment.builder()
                   .dataSource("ds")
                   .interval(Intervals.of("2000/P1D"))
                   .version("1")
                   .shardSpec(new TombstoneShardSpec())
                   .loadSpec(ImmutableMap.of("type", "tombstone"))
                   .size(1)
                   .build(),
        interval
    );
  }


  private List<InputRow> readRows(DruidSegmentReader reader) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    try (final CloseableIterator<Map<String, Object>> iterator = reader.intermediateRowIterator()) {
      while (iterator.hasNext()) {
        rows.addAll(reader.parseInputRows(iterator.next()));
      }
    }
    return rows;
  }

  private List<InputRow> readRows(DruidTombstoneSegmentReader reader) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    try (final CloseableIterator<Map<String, Object>> iterator = reader.intermediateRowIterator()) {
      while (iterator.hasNext()) {
        rows.addAll(reader.parseInputRows(iterator.next()));
      }
    }
    return rows;
  }


  private static HyperLogLogCollector makeHLLC(final String... values)
  {
    final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    for (String value : values) {
      collector.add(HyperLogLogHash.getDefault().hash(value));
    }
    return collector;
  }
}
