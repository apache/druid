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

package io.druid.segment.incremental;

import io.druid.segment.IndexSpec;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.RowIterator;
import io.druid.segment.data.BitmapValues;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.CompressionStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class IncrementalIndexAdapterTest
{
  private static final IndexSpec INDEX_SPEC = new IndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressionStrategy.LZ4,
      CompressionStrategy.LZ4,
      CompressionFactory.LongEncodingStrategy.LONGS
  );

  @Test
  public void testGetBitmapIndex() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex incrementalIndex = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, incrementalIndex);
    IndexableAdapter adapter = new IncrementalIndexAdapter(
        incrementalIndex.getInterval(),
        incrementalIndex,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );
    String dimension = "dim1";
    for (int i = 0; i < adapter.getDimValueLookup(dimension).size(); i++) {
      BitmapValues bitmapValues = adapter.getBitmapValues(dimension, i);
      Assert.assertEquals(1, bitmapValues.size());
    }
  }

  @Test
  public void testGetRowsIterable() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        INDEX_SPEC.getBitmapSerdeFactory()
                  .getBitmapFactory()
    );

    RowIterator rows = incrementalAdapter.getRows();
    List<Integer> rowNums = new ArrayList<>();
    while (rows.moveToNext()) {
      rowNums.add(rows.getPointer().getRowNum());
    }
    Assert.assertEquals(2, rowNums.size());
    Assert.assertEquals(0, (long) rowNums.get(0));
    Assert.assertEquals(1, (long) rowNums.get(1));
  }
}
