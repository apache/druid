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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class IncrementalIndexRowCompTest extends InitializedNullHandlingTest
{
  public final IncrementalIndexCreator indexCreator;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public IncrementalIndexRowCompTest(String indexType) throws JsonProcessingException
  {
    indexCreator = closer.closeLater(
        new IncrementalIndexCreator(indexType, (builder, args) -> builder
            .setSimpleTestingIndexSchema(new CountAggregatorFactory("cnt"))
            .setMaxRowCount(1_000)
            .build())
    );
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.getAppendableIndexTypes();
  }

  @Test
  public void testBasic()
  {
    IncrementalIndex<?> index = indexCreator.createIndex();

    long time = System.currentTimeMillis();
    IncrementalIndexRow ir1 = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    IncrementalIndexRow ir2 = index.toIncrementalIndexRow(toMapRow(time, "billy", "A", "joe", "A")).getIncrementalIndexRow();
    IncrementalIndexRow ir3 = index.toIncrementalIndexRow(toMapRow(time, "billy", "A")).getIncrementalIndexRow();

    IncrementalIndexRow ir4 = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", "B")).getIncrementalIndexRow();
    IncrementalIndexRow ir5 = index.toIncrementalIndexRow(toMapRow(time + 1, "billy", "A", "joe", Arrays.asList("A", "B"))).getIncrementalIndexRow();
    IncrementalIndexRow ir6 = index.toIncrementalIndexRow(toMapRow(time + 1)).getIncrementalIndexRow();

    Comparator<IncrementalIndexRow> comparator = index.dimsComparator();

    Assert.assertEquals(0, comparator.compare(ir1, ir1));
    Assert.assertEquals(0, comparator.compare(ir2, ir2));
    Assert.assertEquals(0, comparator.compare(ir3, ir3));

    Assert.assertTrue(comparator.compare(ir1, ir2) > 0);
    Assert.assertTrue(comparator.compare(ir2, ir1) < 0);
    Assert.assertTrue(comparator.compare(ir2, ir3) > 0);
    Assert.assertTrue(comparator.compare(ir3, ir2) < 0);
    Assert.assertTrue(comparator.compare(ir1, ir3) > 0);
    Assert.assertTrue(comparator.compare(ir3, ir1) < 0);

    Assert.assertTrue(comparator.compare(ir6, ir1) > 0);
    Assert.assertTrue(comparator.compare(ir6, ir2) > 0);
    Assert.assertTrue(comparator.compare(ir6, ir3) > 0);

    Assert.assertTrue(comparator.compare(ir4, ir6) > 0);
    Assert.assertTrue(comparator.compare(ir5, ir6) > 0);
    Assert.assertTrue(comparator.compare(ir5, ir4) < 0);
    Assert.assertTrue(comparator.compare(ir4, ir5) > 0);
  }

  private MapBasedInputRow toMapRow(long time, Object... dimAndVal)
  {
    Map<String, Object> data = new HashMap<>();
    for (int i = 0; i < dimAndVal.length; i += 2) {
      data.put((String) dimAndVal[i], dimAndVal[i + 1]);
    }
    return new MapBasedInputRow(time, Lists.newArrayList(data.keySet()), data);
  }
}
