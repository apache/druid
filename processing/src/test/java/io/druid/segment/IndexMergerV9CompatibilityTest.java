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
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
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
  @Rule
  public final CloserRule closer = new CloserRule(false);
  private static final long TIMESTAMP = DateTime.parse("2014-01-01").getMillis();
  private static final AggregatorFactory[] DEFAULT_AGG_FACTORIES = new AggregatorFactory[]{
      new CountAggregatorFactory(
          "count"
      )
  };

  private static final IndexMergerV9 INDEX_MERGER_V9 = TestHelper.getTestIndexMergerV9();
  private static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();
  private static final IndexMerger INDEX_MERGER = TestHelper.getTestIndexMerger();

  private static final IndexSpec INDEX_SPEC = IndexMergerTest.makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressedObjectStrategy.CompressionStrategy.LZ4,
      CompressedObjectStrategy.CompressionStrategy.LZ4
  );
  private static final List<String> DIMS = ImmutableList.of("dim0", "dim1");

  private static final Function<Collection<Map<String, Object>>, Object[]> OBJECT_MAKER = new Function<Collection<Map<String, Object>>, Object[]>()
  {
    @Nullable
    @Override
    public Object[] apply(Collection<Map<String, Object>> input)
    {
      final ArrayList<InputRow> list = new ArrayList<>();
      int i = 0;
      for (final Map<String, Object> map : input) {
        list.add(new MapBasedInputRow(TIMESTAMP + i++, DIMS, map));
      }
      return new Object[]{list};
    }
  };

  @SafeVarargs
  public static Collection<Object[]> permute(Map<String, Object>... maps)
  {
    if (maps == null) {
      return ImmutableList.<Object[]>of();
    }
    return Collections2.transform(
        Collections2.permutations(
            Arrays.asList(maps)
        ),
        OBJECT_MAKER
    );
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> paramFeeder()
  {
    final Map<String, Object> map1 = ImmutableMap.<String, Object>of(
        DIMS.get(0), ImmutableList.<String>of("dim00", "dim01"),
        DIMS.get(1), "dim10"
    );

    final List<String> nullList = Collections.singletonList(null);

    final Map<String, Object> map2 = ImmutableMap.<String, Object>of(
        DIMS.get(0), nullList,
        DIMS.get(1), "dim10"
    );


    final Map<String, Object> map3 = ImmutableMap.<String, Object>of(
        DIMS.get(0),
        ImmutableList.<String>of("dim00", "dim01")
    );

    final Map<String, Object> map4 = ImmutableMap.<String, Object>of();

    final Map<String, Object> map5 = ImmutableMap.<String, Object>of(DIMS.get(1), "dim10");

    final Map<String, Object> map6 = new HashMap<>();
    map6.put(DIMS.get(1), null); // ImmutableMap cannot take null


    return Iterables.<Object[]>concat(
        permute(map1)
        , permute(map1, map4)
        , permute(map1, map5)
        , permute(map5, map6)
        , permute(map4, map5)
        , Iterables.transform(ImmutableList.of(Arrays.asList(map1, map2, map3, map4, map5, map6)), OBJECT_MAKER)
    );

  }

  private final Collection<InputRow> events;

  public IndexMergerV9CompatibilityTest(
      final Collection<InputRow> events
  )
  {
    this.events = events;
  }

  IncrementalIndex toPersist;
  File tmpDir;
  File persistTmpDir;

  @Before
  public void setUp() throws IOException
  {
    toPersist = new OnheapIncrementalIndex(
        JodaUtils.MIN_INSTANT,
        QueryGranularity.NONE,
        DEFAULT_AGG_FACTORIES,
        1000000
    );
    toPersist.getMetadata().put("key", "value");
    for (InputRow event : events) {
      toPersist.add(event);
    }
    tmpDir = Files.createTempDir();
    persistTmpDir = new File(tmpDir, "persistDir");
    INDEX_MERGER.persist(toPersist, persistTmpDir, INDEX_SPEC);
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testPersistWithSegmentMetadata() throws IOException
  {
    File outDir = Files.createTempDir();
    QueryableIndex index = null;
    try {
      outDir = Files.createTempDir();
      index = INDEX_IO.loadIndex(INDEX_MERGER_V9.persist(toPersist, outDir, INDEX_SPEC));

      Assert.assertEquals("value", index.getMetadata().get("key"));
    }
    finally {
      if (index != null) {
        index.close();
        ;
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
            INDEX_IO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    reprocessAndValidate(persistTmpDir, new File(tmpDir, "reprocessed"));
  }

  private File reprocessAndValidate(File inDir, File tmpDir) throws IOException
  {
    final File outDir = INDEX_MERGER_V9.convert(
        inDir,
        tmpDir,
        INDEX_SPEC
    );
    INDEX_IO.validateTwoSegments(persistTmpDir, outDir);
    return outDir;
  }

  private File appendAndValidate(File inDir, File tmpDir) throws IOException
  {
    final File outDir = INDEX_MERGER.append(
        ImmutableList.<IndexableAdapter>of(new QueryableIndexIndexableAdapter(closer.closeLater(INDEX_IO.loadIndex(inDir)))),
        null,
        tmpDir,
        INDEX_SPEC
    );
    INDEX_IO.validateTwoSegments(persistTmpDir, outDir);
    return outDir;
  }

  @Test
  public void testIdempotentReprocess() throws IOException
  {
    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(
        closer.closeLater(
            INDEX_IO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    final File tmpDir1 = new File(tmpDir, "reprocessed1");
    reprocessAndValidate(persistTmpDir, tmpDir1);

    final File tmpDir2 = new File(tmpDir, "reprocessed2");
    final IndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(closer.closeLater(INDEX_IO.loadIndex(tmpDir1)));
    Assert.assertEquals(events.size(), adapter2.getNumRows());
    reprocessAndValidate(tmpDir1, tmpDir2);

    final File tmpDir3 = new File(tmpDir, "reprocessed3");
    final IndexableAdapter adapter3 = new QueryableIndexIndexableAdapter(closer.closeLater(INDEX_IO.loadIndex(tmpDir2)));
    Assert.assertEquals(events.size(), adapter3.getNumRows());
    reprocessAndValidate(tmpDir2, tmpDir3);
  }

  @Test
  public void testSimpleAppend() throws IOException
  {
    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(
        closer.closeLater(
            INDEX_IO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    appendAndValidate(persistTmpDir, new File(tmpDir, "reprocessed"));
  }

  @Test
  public void testIdempotentAppend() throws IOException
  {
    final IndexableAdapter adapter = new QueryableIndexIndexableAdapter(
        closer.closeLater(
            INDEX_IO.loadIndex(
                persistTmpDir
            )
        )
    );
    Assert.assertEquals(events.size(), adapter.getNumRows());
    final File tmpDir1 = new File(tmpDir, "reprocessed1");
    appendAndValidate(persistTmpDir, tmpDir1);

    final File tmpDir2 = new File(tmpDir, "reprocessed2");
    final IndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(closer.closeLater(INDEX_IO.loadIndex(tmpDir1)));
    Assert.assertEquals(events.size(), adapter2.getNumRows());
    appendAndValidate(tmpDir1, tmpDir2);

    final File tmpDir3 = new File(tmpDir, "reprocessed3");
    final IndexableAdapter adapter3 = new QueryableIndexIndexableAdapter(closer.closeLater(INDEX_IO.loadIndex(tmpDir2)));
    Assert.assertEquals(events.size(), adapter3.getNumRows());
    appendAndValidate(tmpDir2, tmpDir3);
  }
}
