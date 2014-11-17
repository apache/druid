/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.shard;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Pair;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
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
        Assert.assertEquals(String.format("spec[%s], row[%s]", spec, inputRow), pair.lhs, spec.isInChunk(inputRow.getTimestampFromEpoch(), inputRow));
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
