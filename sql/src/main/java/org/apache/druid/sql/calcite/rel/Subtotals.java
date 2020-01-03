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

package org.apache.druid.sql.calcite.rel;


import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.query.dimension.DimensionSpec;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents the Druid groupBy query concept of subtotals, which is similar to GROUPING SETS.
 */
public class Subtotals
{
  /**
   * List of subtotals: each one is a list of dimension indexes. (i.e. [0, 1] means use the first and second
   * dimensions).
   */
  private final List<IntList> subtotals;

  Subtotals(List<IntList> subtotals)
  {
    this.subtotals = subtotals;
  }

  public List<IntList> getSubtotals()
  {
    return subtotals;
  }

  @Nullable
  public List<List<String>> toSubtotalsSpec(final List<DimensionSpec> dimensions)
  {
    if (hasEffect(dimensions)) {
      return subtotals.stream()
                      .map(
                          subtotalInts -> {
                            final List<String> subtotalDimensionNames = new ArrayList<>();
                            for (int dimIndex : subtotalInts) {
                              subtotalDimensionNames.add(dimensions.get(dimIndex).getOutputName());
                            }
                            return subtotalDimensionNames;
                          }
                      )
                      .collect(Collectors.toList());
    } else {
      return null;
    }
  }

  /**
   * Returns whether this subtotals spec has an effect, and cannot be ignored.
   */
  public boolean hasEffect(final List<DimensionSpec> dimensionSpecs)
  {
    if (subtotals.isEmpty() || (subtotals.size() == 1 && subtotals.get(0).size() == dimensionSpecs.size())) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Subtotals subtotals1 = (Subtotals) o;
    return subtotals.equals(subtotals1.subtotals);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(subtotals);
  }

  @Override
  public String toString()
  {
    return "Subtotals{" +
           "subtotals=" + subtotals +
           '}';
  }
}
