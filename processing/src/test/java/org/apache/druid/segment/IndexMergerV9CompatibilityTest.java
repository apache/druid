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
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class IndexMergerV9CompatibilityTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  private static final long TIMESTAMP = DateTimes.of("2014-01-01").getMillis();
  private static final AggregatorFactory[] DEFAULT_AGG_FACTORIES = new AggregatorFactory[]{
      new CountAggregatorFactory(
          "count"
      )
  };

  private static final IndexSpec INDEX_SPEC = IndexMergerTestBase.makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressionStrategy.LZ4,
      CompressionStrategy.LZ4,
      CompressionFactory.LongEncodingStrategy.LONGS
  );
  private static final List<String> DIMS = ImmutableList.of("dim0", "dim1");

  private final Collection<InputRow> events;
  @Rule
  public final CloserRule closer = new CloserRule(false);
  private final IndexMerger indexMerger;
  private final IndexIO indexIO;

  public IndexMergerV9CompatibilityTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
    indexIO = TestHelper.getTestIndexIO();
    events = new ArrayList<>();

    final Map<String, Object> map1 = ImmutableMap.of(
        DIMS.get(0), ImmutableList.of("dim00", "dim01"),
        DIMS.get(1), "dim10"
    );

    final List<String> nullList = Collections.singletonList(null);

    final Map<String, Object> map2 = ImmutableMap.of(
        DIMS.get(0), nullList,
        DIMS.get(1), "dim10"
    );

    final Map<String, Object> map3 = ImmutableMap.of(
        DIMS.get(0),
        ImmutableList.of("dim00", "dim01")
    );

    final Map<String, Object> map4 = ImmutableMap.of();

    final Map<String, Object> map5 = ImmutableMap.of(DIMS.get(1), "dim10");

    final Map<String, Object> map6 = new HashMap<>();
    map6.put(DIMS.get(1), null); // ImmutableMap cannot take null

    int i = 0;
    for (final Map<String, Object> map : Arrays.asList(map1, map2, map3, map4, map5, map6)) {
      events.add(new MapBasedInputRow(TIMESTAMP + i++, DIMS, map));
    }
  }

  IncrementalIndex toPersist;
  File tmpDir;
  File persistTmpDir;

  @Before
  public void setUp() throws IOException
  {
    toPersist = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMinTimestamp(JodaUtils.MIN_INSTANT)
                .withMetrics(DEFAULT_AGG_FACTORIES)
                .build()
        )
    .setMaxRowCount(1000000)
    .build();

    toPersist.getMetadata().put("key", "value");
    for (InputRow event : events) {
      toPersist.add(event);
    }
    tmpDir = FileUtils.createTempDir();
    persistTmpDir = new File(tmpDir, "persistDir");
    org.apache.commons.io.FileUtils.forceMkdir(persistTmpDir);
    String[] files = new String[] {"00000.smoosh", "meta.smoosh", "version.bin"};
    for (String file : files) {
      new ByteSource()
      {
        @Override
        public InputStream openStream()
        {
          return IndexMergerV9CompatibilityTest.class.getResourceAsStream("/v8SegmentPersistDir/" + file);
        }
      }.copyTo(Files.asByteSink(new File(persistTmpDir, file)));
    }
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testPersistWithSegmentMetadata() throws IOException
  {
    File outDir = FileUtils.createTempDir();
    QueryableIndex index = null;
    try {
      outDir = FileUtils.createTempDir();
      index = indexIO.loadIndex(indexMerger.persist(toPersist, outDir, INDEX_SPEC, null));

      Assert.assertEquals("value", index.getMetadata().get("key"));
    }
    finally {
      if (index != null) {
        index.close();
      }

      if (outDir != null) {
        FileUtils.deleteDirectory(outDir);
      }
    }
  }

  @Test
  public void testSimpleReprocess() throws IOException
  {
    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(
        closer.closeLater(
            indexIO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    reprocessAndValidate(persistTmpDir, new File(tmpDir, "reprocessed"));
  }

  private File reprocessAndValidate(File inDir, File tmpDir) throws IOException
  {
    final File outDir = indexMerger.convert(
        inDir,
        tmpDir,
        INDEX_SPEC
    );
    indexIO.validateTwoSegments(persistTmpDir, outDir);
    return outDir;
  }

  @Test
  public void testIdempotentReprocess() throws IOException
  {
    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(
        closer.closeLater(
            indexIO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    final File tmpDir1 = new File(tmpDir, "reprocessed1");
    reprocessAndValidate(persistTmpDir, tmpDir1);

    final File tmpDir2 = new File(tmpDir, "reprocessed2");
    final IndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(closer.closeLater(indexIO.loadIndex(tmpDir1)));
    Assert.assertEquals(events.size(), adapter2.getNumRows());
    reprocessAndValidate(tmpDir1, tmpDir2);

    final File tmpDir3 = new File(tmpDir, "reprocessed3");
    final IndexableAdapter adapter3 = new QueryableIndexIndexableAdapter(closer.closeLater(indexIO.loadIndex(tmpDir2)));
    Assert.assertEquals(events.size(), adapter3.getNumRows());
    reprocessAndValidate(tmpDir2, tmpDir3);
  }
}
