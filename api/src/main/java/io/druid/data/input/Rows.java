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

package io.druid.data.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;

import io.druid.java.util.common.ISE;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class Rows
{
  public static InputRow toCaseInsensitiveInputRow(final Row row, final List<String> dimensions)
  {
    if (row instanceof MapBasedRow) {
      MapBasedRow mapBasedRow = (MapBasedRow) row;

      TreeMap<String, Object> caseInsensitiveMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
      caseInsensitiveMap.putAll(mapBasedRow.getEvent());
      return new MapBasedInputRow(
          mapBasedRow.getTimestamp(),
          dimensions,
          caseInsensitiveMap
      );
    }
    throw new ISE("Can only convert MapBasedRow objects because we are ghetto like that.");
  }

  /**
   * @param timeStamp rollup up timestamp to be used to create group key
   * @param inputRow input row
   * @return groupKey for the given input row
   */
  public static List<Object> toGroupKey(long timeStamp, InputRow inputRow)
  {
    final Map<String, Set<String>> dims = Maps.newTreeMap();
    for (final String dim : inputRow.getDimensions()) {
      final Set<String> dimValues = ImmutableSortedSet.copyOf(inputRow.getDimension(dim));
      if (dimValues.size() > 0) {
        dims.put(dim, dimValues);
      }
    }
    return ImmutableList.of(
        timeStamp,
        dims
    );
  }
}
