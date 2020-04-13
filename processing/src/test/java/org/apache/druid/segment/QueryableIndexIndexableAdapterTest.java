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
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.IncrementalIndexTest;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Collection;

@RunWith(Parameterized.class)
public class QueryableIndexIndexableAdapterTest
{
  private static final IndexSpec INDEX_SPEC = IndexMergerTestBase.makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressionStrategy.LZ4,
      CompressionStrategy.LZ4,
      CompressionFactory.LongEncodingStrategy.LONGS
  );


  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public final CloserRule closer = new CloserRule(false);

  private final IndexMerger indexMerger;
  private final IndexIO indexIO;

  public QueryableIndexIndexableAdapterTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
    indexIO = TestHelper.getTestIndexIO();
  }

  @Test
  public void testGetBitmapIndex() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    final File tempDir = temporaryFolder.newFolder();
    QueryableIndex index = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.persist(
                toPersist,
                tempDir,
                INDEX_SPEC,
                null
            )
        )
    );

    IndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
    String dimension = "dim1";
    @SuppressWarnings("UnusedAssignment") //null is added to all dimensions with value
    BitmapValues bitmapValues = adapter.getBitmapValues(dimension, 0);
    try (CloseableIndexed<String> dimValueLookup = adapter.getDimValueLookup(dimension)) {
      for (int i = 0; i < dimValueLookup.size(); i++) {
        bitmapValues = adapter.getBitmapValues(dimension, i);
        Assert.assertEquals(1, bitmapValues.size());
      }
    }
  }
}
