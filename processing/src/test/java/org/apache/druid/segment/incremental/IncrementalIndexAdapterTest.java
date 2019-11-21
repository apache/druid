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

package org.apache.druid.segment.incremental;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.RowIterator;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.IncrementalIndexTest;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class IncrementalIndexAdapterTest extends InitializedNullHandlingTest
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
    try (CloseableIndexed<String> dimValueLookup = adapter.getDimValueLookup(dimension)) {
      for (int i = 0; i < dimValueLookup.size(); i++) {
        BitmapValues bitmapValues = adapter.getBitmapValues(dimension, i);
        Assert.assertEquals(1, bitmapValues.size());
      }
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

  @Test
  public void testGetRowsIterableNoRollup() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createNoRollupIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);


    ArrayList<Integer> dim1Vals = new ArrayList<>();
    for (IncrementalIndexRow row : toPersist1.getFacts().keySet()) {
      dim1Vals.add(((int[]) row.getDims()[0])[0]);
    }
    ArrayList<Integer> dim2Vals = new ArrayList<>();
    for (IncrementalIndexRow row : toPersist1.getFacts().keySet()) {
      dim2Vals.add(((int[]) row.getDims()[1])[0]);
    }

    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        INDEX_SPEC.getBitmapSerdeFactory()
                  .getBitmapFactory()
    );

    RowIterator rows = incrementalAdapter.getRows();
    List<String> rowStrings = new ArrayList<>();
    while (rows.moveToNext()) {
      rowStrings.add(rows.getPointer().toString());
    }

    Function<Integer, String> getExpected = (rowNumber) -> {
      if (rowNumber < 3) {
        return StringUtils.format(
            "RowPointer{indexNum=0, rowNumber=%s, timestamp=%s, dimensions={dim1=1, dim2=2}, metrics={count=1}}",
            rowNumber,
            timestamp
        );
      } else {
        return StringUtils.format(
            "RowPointer{indexNum=0, rowNumber=%s, timestamp=%s, dimensions={dim1=3, dim2=4}, metrics={count=1}}",
            rowNumber,
            timestamp
        );
      }
    };


    // without sorting, output would be
    //    RowPointer{indexNum=0, rowNumber=0, timestamp=1533347274588, dimensions={dim1=1, dim2=2}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=1, timestamp=1533347274588, dimensions={dim1=3, dim2=4}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=2, timestamp=1533347274588, dimensions={dim1=1, dim2=2}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=3, timestamp=1533347274588, dimensions={dim1=3, dim2=4}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=4, timestamp=1533347274588, dimensions={dim1=1, dim2=2}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=5, timestamp=1533347274588, dimensions={dim1=3, dim2=4}, metrics={count=1}}
    // but with sorting, output should be
    //    RowPointer{indexNum=0, rowNumber=0, timestamp=1533347361396, dimensions={dim1=1, dim2=2}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=1, timestamp=1533347361396, dimensions={dim1=1, dim2=2}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=2, timestamp=1533347361396, dimensions={dim1=1, dim2=2}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=3, timestamp=1533347361396, dimensions={dim1=3, dim2=4}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=4, timestamp=1533347361396, dimensions={dim1=3, dim2=4}, metrics={count=1}}
    //    RowPointer{indexNum=0, rowNumber=5, timestamp=1533347361396, dimensions={dim1=3, dim2=4}, metrics={count=1}}

    Assert.assertEquals(6, rowStrings.size());
    for (int i = 0; i < 6; i++) {
      if (i % 2 == 0) {
        Assert.assertEquals(0, (long) dim1Vals.get(i));
        Assert.assertEquals(0, (long) dim2Vals.get(i));
      } else {
        Assert.assertEquals(1, (long) dim1Vals.get(i));
        Assert.assertEquals(1, (long) dim2Vals.get(i));
      }
      Assert.assertEquals(getExpected.apply(i), rowStrings.get(i));
    }
  }
}
