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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 */
public class OnHeapDimDimTest
{
  @Test
  public void testCachedCompares() throws IndexSizeExceededException
  {
    IncrementalIndex index1 = createIndexWithCache(-1);
    IncrementalIndex index2 = createIndexWithCache(64);
    IncrementalIndex index3 = createIndexWithCache(4096);

    Random r = new Random();
    StringBuilder builder = new StringBuilder();

    List<String> testStrings = Lists.newArrayList();
    testStrings.add("");
    for (int i = 0; i < 4096; i++) {
      int max = r.nextBoolean() ? 0x7f : Character.MAX_VALUE;
      for (int x = r.nextInt(17) + 3; x >= 0; x--) {
        builder.append(Character.toChars(r.nextInt(max)));
      }
      String value = builder.toString();
      for (int x = r.nextInt(512); x >= 0; x--) {
        testStrings.add(value);
      }
      builder.setLength(0);
    }
    Collections.shuffle(testStrings);

    final List<String> dimensions = Arrays.asList("string");
    List<InputRow> rows = Lists.newArrayList(
        Iterables.transform(
            testStrings, new Function<String, InputRow>()
            {
              @Override
              public InputRow apply(String input)
              {
                return new MapBasedInputRow(
                    10000, dimensions, ImmutableMap.<String, Object>of("string", input)
                );
              }
            }
        )
    );
    long prev = System.currentTimeMillis();
    for (InputRow row : rows) {
      index1.add(row);
    }
    System.out.println(" No cache. Took " + (System.currentTimeMillis() - prev) + " msec");

    prev = System.currentTimeMillis();
    for (InputRow row : rows) {
      index2.add(row);
    }
    System.out.println(" 64 entry cache. Took " + (System.currentTimeMillis() - prev) + " msec");

    prev = System.currentTimeMillis();
    for (InputRow row : rows) {
      index3.add(row);
    }
    System.out.println(" 4096 entry cache. Took " + (System.currentTimeMillis() - prev) + " msec");

    Assert.assertTrue(Iterables.elementsEqual(index1, index2));
    Assert.assertTrue(Iterables.elementsEqual(index1, index3));
  }

  private IncrementalIndex createIndexWithCache(int cacheSize)
  {
    IncrementalIndexSchema schema1 = new IncrementalIndexSchema.Builder().withDimensionsSpec(
        new DimensionsSpec(Arrays.<DimensionSchema>asList(new StringDimensionSchema("string", cacheSize)), null, null)
    ).build();

    return new OnheapIncrementalIndex(schema1, true, 65536);
  }
}
