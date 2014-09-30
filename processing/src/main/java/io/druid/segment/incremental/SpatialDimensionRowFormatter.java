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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Floats;
import com.metamx.common.ISE;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.SpatialDimensionSchema;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * We throw away all invalid spatial dimensions
 */
public class SpatialDimensionRowFormatter
{
  private static final Joiner JOINER = Joiner.on(",");
  private static final Splitter SPLITTER = Splitter.on(",");

  private final Map<String, SpatialDimensionSchema> spatialDimensionMap;
  private final Set<String> spatialPartialDimNames;

  public SpatialDimensionRowFormatter(List<SpatialDimensionSchema> spatialDimensions)
  {
    this.spatialDimensionMap = Maps.newHashMap();
    for (SpatialDimensionSchema spatialDimension : spatialDimensions) {
      if (this.spatialDimensionMap.put(spatialDimension.getDimName(), spatialDimension) != null) {
        throw new ISE("Duplicate spatial dimension names found! Check your schema yo!");
      }
    }
    this.spatialPartialDimNames = Sets.newHashSet(
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
                return !spatialDimensionMap.containsKey(input) && !spatialPartialDimNames.contains(input);
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
      public DateTime getTimestamp()
      {
        return row.getTimestamp();
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        List<String> retVal = spatialLookup.get(dimension);
        return (retVal == null) ? row.getDimension(dimension) : retVal;
      }

      @Override
      public Object getRaw(String dimension)
      {
        return row.getRaw(dimension);
      }

      @Override
      public float getFloatMetric(String metric)
      {
        try {
          return row.getFloatMetric(metric);
        }
        catch (ParseException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public String toString()
      {
        return row.toString();
      }

      @Override
      public int compareTo(Row o)
      {
        return getTimestamp().compareTo(o.getTimestamp());
      }
    };

    for (Map.Entry<String, SpatialDimensionSchema> entry : spatialDimensionMap.entrySet()) {
      final String spatialDimName = entry.getKey();
      final SpatialDimensionSchema spatialDim = entry.getValue();

      List<String> dimVals = row.getDimension(spatialDimName);
      if (dimVals != null && !dimVals.isEmpty()) {
        if (dimVals.size() != 1) {
          throw new ISE("Spatial dimension value must be in an array!");
        }
        if (isJoinedSpatialDimValValid(dimVals.get(0))) {
          spatialLookup.put(spatialDimName, dimVals);
          finalDims.add(spatialDimName);
        }
      } else {
        List<String> spatialDimVals = Lists.newArrayList();
        for (String dim : spatialDim.getDims()) {
          List<String> partialDimVals = row.getDimension(dim);
          if (isSpatialDimValsValid(partialDimVals)) {
            spatialDimVals.addAll(partialDimVals);
          }
        }

        if (spatialDimVals.size() == spatialDim.getDims().size()) {
          spatialLookup.put(spatialDimName, Arrays.asList(JOINER.join(spatialDimVals)));
          finalDims.add(spatialDimName);
        }
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

  private boolean isJoinedSpatialDimValValid(String dimVal)
  {
    if (dimVal == null || dimVal.isEmpty()) {
      return false;
    }
    Iterable<String> dimVals = SPLITTER.split(dimVal);
    for (String val : dimVals) {
      if (Floats.tryParse(val) == null) {
        return false;
      }
    }
    return true;
  }
}
