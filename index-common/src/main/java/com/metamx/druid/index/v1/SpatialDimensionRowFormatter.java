/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Floats;
import com.metamx.druid.input.InputRow;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class SpatialDimensionRowFormatter
{
  private static final Joiner JOINER = Joiner.on(",");

  private final List<SpatialDimensionSchema> spatialDimensions;
  private final Set<String> spatialDimNames;

  public SpatialDimensionRowFormatter(List<SpatialDimensionSchema> spatialDimensions)
  {
    this.spatialDimensions = spatialDimensions;
    this.spatialDimNames = Sets.newHashSet(
        Iterables.concat(
            Lists.transform(
                spatialDimensions,
                new Function<SpatialDimensionSchema, List<String>>()
                {
                  @Override
                  public List<String> apply(SpatialDimensionSchema input)
                  {
                    return input.getDims();
                  }
                }
            )
        )
    );
  }

  public InputRow formatRow(final InputRow row)
  {
    final Map<String, List<String>> spatialLookup = Maps.newHashMap();

    // remove all spatial dimensions
    final List<String> finalDims = Lists.newArrayList(
        Iterables.filter(
            Lists.transform(
                row.getDimensions(),
                new Function<String, String>()
                {
                  @Override
                  public String apply(String input)
                  {
                    return input.toLowerCase();
                  }
                }
            )
            ,
            new Predicate<String>()
            {
              @Override
              public boolean apply(String input)
              {
                return !spatialDimNames.contains(input);
              }
            }
        )
    );

    InputRow retVal = new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return finalDims;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return row.getTimestampFromEpoch();
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        List<String> retVal = spatialLookup.get(dimension);
        return (retVal == null) ? row.getDimension(dimension) : retVal;
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return row.getFloatMetric(metric);
      }
    };

    for (SpatialDimensionSchema spatialDimension : spatialDimensions) {
      if (!row.getDimension(spatialDimension.getDimName()).isEmpty()) {
        continue;
      }

      List<String> spatialDimVals = Lists.newArrayList();

      for (String partialSpatialDim : spatialDimension.getDims()) {
        List<String> dimVals = row.getDimension(partialSpatialDim);
        if (isSpatialDimValsValid(dimVals)) {
          spatialDimVals.addAll(dimVals);
        }
      }

      if (spatialDimVals.size() == spatialDimNames.size()) {
        spatialLookup.put(spatialDimension.getDimName(), Arrays.asList(JOINER.join(spatialDimVals)));
        finalDims.add(spatialDimension.getDimName());
      }
    }

    return retVal;
  }

  private boolean isSpatialDimValsValid(List<String> dimVals)
  {
    if (dimVals == null || dimVals.isEmpty()) {
      return false;
    }
    for (String dimVal : dimVals) {
      if (Floats.tryParse(dimVal) == null) {
        return false;
      }
    }
    return true;
  }
}
