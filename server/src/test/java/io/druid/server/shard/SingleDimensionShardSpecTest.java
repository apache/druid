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

package io.druid.server.shard;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class SingleDimensionShardSpecTest
{
  @Test
  public void testIsInChunk() throws Exception
  {
    Map<SingleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>> tests = ImmutableMap.<SingleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>>builder()
        .put(
            makeSpec(null, null),
            makeList(
                true, null,
                true, "a",
                true, "h",
                true, "p",
                true, "y"
            )
        )
        .put(
            makeSpec(null, "m"),
            makeList(
                true, null,
                true, "a",
                true, "h",
                false, "p",
                false, "y"
            )
        )
        .put(
            makeSpec("a", "h"),
            makeList(
                false, null,
                true, "a",
                false, "h",
                false, "p",
                false, "y"
            )
        )
        .put(
            makeSpec("d", "u"),
            makeList(
                false, null,
                false, "a",
                true, "h",
                true, "p",
                false, "y"
            )
        )
        .put(
            makeSpec("h", null),
            makeList(
                false, null,
                false, "a",
                true, "h",
                true, "p",
                true, "y"
            )
        )
        .build();

    for (Map.Entry<SingleDimensionShardSpec, List<Pair<Boolean, Map<String, String>>>> entry : tests.entrySet()) {
      SingleDimensionShardSpec spec = entry.getKey();
      for (Pair<Boolean, Map<String, String>> pair : entry.getValue()) {
        final InputRow inputRow = new MapBasedInputRow(
            0, ImmutableList.of("billy"), Maps.transformValues(
            pair.rhs, new Function<String, Object>()
        {
          @Override
          public Object apply(String input)
          {
            return input;
          }
        }
        )
        );
        Assert.assertEquals(StringUtils.format("spec[%s], row[%s]", spec, inputRow), pair.lhs, spec.isInChunk(inputRow.getTimestampFromEpoch(), inputRow));
      }
    }
  }

  private SingleDimensionShardSpec makeSpec(String start, String end)
  {
    return new SingleDimensionShardSpec("billy", start, end, 0);
  }

  private Map<String, String> makeMap(String value)
  {
    return value == null ? ImmutableMap.<String, String>of() : ImmutableMap.of("billy", value);
  }

  private List<Pair<Boolean, Map<String, String>>> makeList(Object... arguments)
  {
    Preconditions.checkState(arguments.length % 2 == 0);

    final ArrayList<Pair<Boolean,Map<String,String>>> retVal = Lists.newArrayList();

    for (int i = 0; i < arguments.length; i += 2) {
      retVal.add(Pair.of((Boolean) arguments[i], makeMap((String) arguments[i + 1])));
    }

    return retVal;
  }

}
