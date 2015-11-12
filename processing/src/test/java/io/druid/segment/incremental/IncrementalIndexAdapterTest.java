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
import io.druid.segment.Rowboat;
import io.druid.segment.column.BitmapIndexSeeker;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.data.IndexedInts;

import org.junit.Assert;
import org.junit.Test;

import com.metamx.common.ISE;

import java.util.ArrayList;
import java.util.List;

public class IncrementalIndexAdapterTest
{
  private static final IndexSpec INDEX_SPEC = new IndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressedObjectStrategy.CompressionStrategy.LZ4.name().toLowerCase(),
      CompressedObjectStrategy.CompressionStrategy.LZ4.name().toLowerCase()
  );

  @Test
  public void testGetBitmapIndexSeeker() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex incrementalIndex = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, incrementalIndex);
    IndexableAdapter adapter = new IncrementalIndexAdapter(
        incrementalIndex.getInterval(),
        incrementalIndex,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );
    BitmapIndexSeeker bitmapIndexSeeker = adapter.getBitmapIndexSeeker("dim1");
    IndexedInts indexedInts0 = bitmapIndexSeeker.seek("0");
    Assert.assertEquals(0, indexedInts0.size());
    IndexedInts indexedInts1 = bitmapIndexSeeker.seek("1");
    Assert.assertEquals(1, indexedInts1.size());
    try {
      bitmapIndexSeeker.seek("01");
      Assert.assertFalse("Only support access in order", true);
    }
    catch (ISE ise) {
      Assert.assertTrue("Only support access in order", true);
    }
    IndexedInts indexedInts2 = bitmapIndexSeeker.seek("2");
    Assert.assertEquals(0, indexedInts2.size());
    IndexedInts indexedInts3 = bitmapIndexSeeker.seek("3");
    Assert.assertEquals(1, indexedInts3.size());
    IndexedInts indexedInts4 = bitmapIndexSeeker.seek("4");
    Assert.assertEquals(0, indexedInts4.size());
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

    Iterable<Rowboat> boats = incrementalAdapter.getRows();
    List<Rowboat> boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    Assert.assertEquals(2, boatList.size());
    Assert.assertEquals(0, boatList.get(0).getRowNum());
    Assert.assertEquals(1, boatList.get(1).getRowNum());

    /* Iterate through the Iterable a few times, check that boat row numbers are correct afterwards */
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }

    Assert.assertEquals(2, boatList.size());
    Assert.assertEquals(0, boatList.get(0).getRowNum());
    Assert.assertEquals(1, boatList.get(1).getRowNum());

  }
}
