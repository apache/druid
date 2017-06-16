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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import static io.druid.segment.incremental.IncrementalIndex.TimeAndDims;

/**
 */
public class TimeAndDimsCompTest
{
  @Test
  public void testBasic() throws IndexSizeExceededException
  {
    IncrementalIndex index = new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("cnt"))
        .setMaxRowCount(1000)
        .buildOnheap();

    long time = System.currentTimeMillis();
    TimeAndDims td1 = index.toTimeAndDims(toMapRow(time, "billy", "A", "joe", "B"));
    TimeAndDims td2 = index.toTimeAndDims(toMapRow(time, "billy", "A", "joe", "A"));
    TimeAndDims td3 = index.toTimeAndDims(toMapRow(time, "billy", "A"));

    TimeAndDims td4 = index.toTimeAndDims(toMapRow(time + 1, "billy", "A", "joe", "B"));
    TimeAndDims td5 = index.toTimeAndDims(toMapRow(time + 1, "billy", "A", "joe", Arrays.asList("A", "B")));
    TimeAndDims td6 = index.toTimeAndDims(toMapRow(time + 1));

    Comparator<IncrementalIndex.TimeAndDims> comparator = index.dimsComparator();

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

  private MapBasedInputRow toMapRow(long time, Object... dimAndVal)
  {
    Map<String, Object> data = Maps.newHashMap();
    for (int i = 0; i < dimAndVal.length; i += 2) {
      data.put((String) dimAndVal[i], dimAndVal[i + 1]);
    }
    return new MapBasedInputRow(time, Lists.newArrayList(data.keySet()), data);
  }
}
