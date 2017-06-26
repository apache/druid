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

import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.incremental.IncrementalIndex;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class QueryableIndexIndexableAdapterTest
{
  private final static IndexMerger INDEX_MERGER = TestHelper.getTestIndexMergerV9();
  private final static IndexIO INDEX_IO = TestHelper.getTestIndexIO();
  private static final IndexSpec INDEX_SPEC = IndexMergerTestBase.makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressedObjectStrategy.CompressionStrategy.LZ4,
      CompressedObjectStrategy.CompressionStrategy.LZ4,
      CompressionFactory.LongEncodingStrategy.LONGS
  );

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public final CloserRule closer = new CloserRule(false);

  @Test
  public void testGetBitmapIndex() throws Exception
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
                INDEX_SPEC
            )
        )
    );

    IndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
    String dimension = "dim1";
    //null is added to all dimensions with value
    IndexedInts indexedInts = adapter.getBitmapIndex(dimension, 0);
    for (int i = 0; i < adapter.getDimValueLookup(dimension).size(); i++) {
      indexedInts = adapter.getBitmapIndex(dimension, i);
      Assert.assertEquals(1, indexedInts.size());
    }
  }
}
