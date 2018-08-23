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

package io.druid.segment.incremental;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

/**
 */
public class IncrementalIndexRowCompTest
{
  private static final Logger log = new Logger(IncrementalIndexRowCompTest.class);

  @Test
  public void testBasic() throws IndexSizeExceededException
  {
    IncrementalIndex index = new IncrementalIndex.Builder()
            .setSimpleTestingIndexSchema(new CountAggregatorFactory("cnt"))
            .setMaxRowCount(1000)
            .buildOnheap();

    long time = System.currentTimeMillis();
    IncrementalIndexRow td1 = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    IncrementalIndexRow td2 = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "A")).getIncrementalIndexRow();
    IncrementalIndexRow td3 = index.toIncrementalIndexRow(toMapRow(time, "billy", "A")).getIncrementalIndexRow();

    IncrementalIndexRow td4 = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    IncrementalIndexRow td5 = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", Arrays.asList("A", "B"))).getIncrementalIndexRow();
    IncrementalIndexRow td6 = index.toIncrementalIndexRow(toMapRow(time + 1)).getIncrementalIndexRow();

    Comparator<IncrementalIndexRow> comparator = index.dimsComparator();

    Assert.assertEquals(0, comparator.compare(td1, td1));
    Assert.assertEquals(0, comparator.compare(td2, td2));
    Assert.assertEquals(0, comparator.compare(td3, td3));

    Assert.assertTrue(comparator.compare(td1, td2) > 0);
    Assert.assertTrue(comparator.compare(td2, td1) < 0);
    Assert.assertTrue(comparator.compare(td2, td3) > 0);
    Assert.assertTrue(comparator.compare(td3, td2) < 0);
    Assert.assertTrue(comparator.compare(td1, td3) > 0);
    Assert.assertTrue(comparator.compare(td3, td1) < 0);

    Assert.assertTrue(comparator.compare(td6, td1) > 0);
    Assert.assertTrue(comparator.compare(td6, td2) > 0);
    Assert.assertTrue(comparator.compare(td6, td3) > 0);

    Assert.assertTrue(comparator.compare(td4, td6) > 0);
    Assert.assertTrue(comparator.compare(td5, td6) > 0);
    Assert.assertTrue(comparator.compare(td4, td5) < 0);
    Assert.assertTrue(comparator.compare(td5, td4) > 0);
  }

  @Test
  public void testTimeAndDimsSerialization() throws IndexSizeExceededException
  {
    IncrementalIndex index = new IncrementalIndex.Builder()
            .setSimpleTestingIndexSchema(true, new CountAggregatorFactory("cnt"))
            .setMaxRowCount(1000)
            .buildOffheapOak();

    OakIncrementalIndex oakIndex = (OakIncrementalIndex) index;

    long time = System.currentTimeMillis();
    IncrementalIndexRow[] origIncrementalIndexRow = new IncrementalIndexRow[6];
    ByteBuffer[] serializedIncrementalIndexRow = new ByteBuffer[6];
    IncrementalIndexRow[] deserializedIncrementalIndexRow = new IncrementalIndexRow[6];

    Comparator<IncrementalIndexRow> comparator = index.dimsComparator();

    origIncrementalIndexRow[0] = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    origIncrementalIndexRow[1] = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "A")).getIncrementalIndexRow();
    origIncrementalIndexRow[2] = index.toIncrementalIndexRow(toMapRow(time, "billy", "A")).getIncrementalIndexRow();
    origIncrementalIndexRow[3] = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    origIncrementalIndexRow[4] = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", "A,B")).getIncrementalIndexRow();
    origIncrementalIndexRow[5] = index.toIncrementalIndexRow(toMapRow(time + 1)).getIncrementalIndexRow();

    OakKeySerializer serializer = new OakKeySerializer(oakIndex.dimensionDescsList);

    for (int i = 0; i < 6; i++) {
      serializedIncrementalIndexRow[i] = ByteBuffer.allocate(serializer.calculateSize(origIncrementalIndexRow[i]));
      serializer.serialize(origIncrementalIndexRow[i], serializedIncrementalIndexRow[i]);
      deserializedIncrementalIndexRow[i] = serializer.deserialize(serializedIncrementalIndexRow[i]);
      Assert.assertEquals(0, comparator.compare(origIncrementalIndexRow[i], deserializedIncrementalIndexRow[i]));
    }
  }

  @Test
  public void testIncrementalIndexRowByteBufferComparator() throws IndexSizeExceededException
  {
    IncrementalIndex index = new IncrementalIndex.Builder()
            .setSimpleTestingIndexSchema(true, new CountAggregatorFactory("cnt"))
            .setMaxRowCount(1000)
            .buildOffheapOak();

    OakIncrementalIndex oakIndex = (OakIncrementalIndex) index;

    long time = System.currentTimeMillis();

    IncrementalIndexRow[] incrementalIndexRowArray = new IncrementalIndexRow[6];
    incrementalIndexRowArray[0] = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    incrementalIndexRowArray[1] = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "A")).getIncrementalIndexRow();
    incrementalIndexRowArray[2] = index.toIncrementalIndexRow(toMapRow(time, "billy", "A")).getIncrementalIndexRow();
    incrementalIndexRowArray[3] = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    incrementalIndexRowArray[4] = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", "B,A")).getIncrementalIndexRow();
    incrementalIndexRowArray[5] = index.toIncrementalIndexRow(toMapRow(time + 1)).getIncrementalIndexRow();

    OakKeySerializer serializer = new OakKeySerializer(oakIndex.dimensionDescsList);

    ByteBuffer[] incrementalIndexRowByteBufferArray = new ByteBuffer[6];
    for (int i = 0; i < 6; i++) {
      incrementalIndexRowByteBufferArray[i] = ByteBuffer.allocate(serializer.calculateSize(incrementalIndexRowArray[i]));
      serializer.serialize(incrementalIndexRowArray[i], incrementalIndexRowByteBufferArray[i]);
    }

    OakKeysComparator comparator = new OakKeysComparator(oakIndex.dimensionDescsList, true);

    Assert.assertEquals(0, comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[0], incrementalIndexRowByteBufferArray[0]));
    Assert.assertEquals(0, comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[1], incrementalIndexRowByteBufferArray[1]));
    Assert.assertEquals(0, comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[2], incrementalIndexRowByteBufferArray[2]));

    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[0], incrementalIndexRowByteBufferArray[1]) > 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[1], incrementalIndexRowByteBufferArray[0]) < 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[1], incrementalIndexRowByteBufferArray[2]) > 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[2], incrementalIndexRowByteBufferArray[1]) < 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[0], incrementalIndexRowByteBufferArray[2]) > 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[2], incrementalIndexRowByteBufferArray[0]) < 0);

    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[5], incrementalIndexRowByteBufferArray[0]) > 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[5], incrementalIndexRowByteBufferArray[1]) > 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[5], incrementalIndexRowByteBufferArray[2]) > 0);

    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[3], incrementalIndexRowByteBufferArray[5]) > 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[4], incrementalIndexRowByteBufferArray[5]) > 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[3], incrementalIndexRowByteBufferArray[4]) < 0);
    Assert.assertTrue(comparator.compareSerializedKeys(incrementalIndexRowByteBufferArray[4], incrementalIndexRowByteBufferArray[3]) > 0);
  }

  private MapBasedInputRow toMapRow(long time, Object... dimAndVal)
  {
    Map<String, Object> data = Maps.newHashMap();
    for (int i = 0; i < dimAndVal.length; i += 2) {
      data.put((String) dimAndVal[i], dimAndVal[i + 1]);
    }
    return new MapBasedInputRow(time, Lists.newArrayList(data.keySet()), data);
  }
}
