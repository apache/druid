/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment.incremental;

import io.druid.segment.IndexSpec;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class IncrementalIndexAdapterTest
{
  @Test
  public void testGetRowsIterable() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(true, null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    IndexSpec indexSpec = new IndexSpec(
        new RoaringBitmapSerdeFactory(),
        CompressedObjectStrategy.CompressionStrategy.LZ4.name().toLowerCase(),
        CompressedObjectStrategy.CompressionStrategy.LZ4.name().toLowerCase()
    );

    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        indexSpec.getBitmapSerdeFactory()
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
