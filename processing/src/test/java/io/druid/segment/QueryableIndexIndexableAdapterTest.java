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

import java.io.File;

import io.druid.segment.column.BitmapIndexSeeker;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.metamx.common.ISE;

public class QueryableIndexIndexableAdapterTest {
  private final static IndexMerger INDEX_MERGER = TestHelper.getTestIndexMerger();
  private final static IndexIO INDEX_IO = TestHelper.getTestIndexIO();
  private static final IndexSpec INDEX_SPEC = IndexMergerTest.makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressedObjectStrategy.CompressionStrategy.LZ4,
      CompressedObjectStrategy.CompressionStrategy.LZ4
  );

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public final CloserRule closer = new CloserRule(false);
	   
  @Test
  public void testGetBitmapIndexSeeker() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    final File tempDir = temporaryFolder.newFolder();
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist.getInterval(),
        toPersist,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );

    QueryableIndex index = closer.closeLater(
        INDEX_IO.loadIndex(
            INDEX_MERGER.persist(
                toPersist,
                tempDir,
                null,
                INDEX_SPEC
            )
        )
    );

    IndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
    BitmapIndexSeeker bitmapIndexSeeker = adapter.getBitmapIndexSeeker("dim1");
    IndexedInts indexedInts0 = bitmapIndexSeeker.seek("0");
    Assert.assertEquals(0, indexedInts0.size());
    IndexedInts indexedInts1 = bitmapIndexSeeker.seek("1");
    Assert.assertEquals(1, indexedInts1.size());
    try {
      bitmapIndexSeeker.seek("4");
      Assert.assertFalse("Only support access in order", true);
    } catch(ISE ise) {
      Assert.assertTrue("Only support access in order", true);
    }
    IndexedInts indexedInts2 = bitmapIndexSeeker.seek("2");
    Assert.assertEquals(0, indexedInts2.size());
    IndexedInts indexedInts3 = bitmapIndexSeeker.seek("3");
    Assert.assertEquals(1, indexedInts3.size());
    IndexedInts indexedInts4 = bitmapIndexSeeker.seek("4");
    Assert.assertEquals(0, indexedInts4.size());
  }
}
