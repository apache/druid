package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
    final Map<String, List<String>> finalDims = Maps.newHashMap();

    // remove all spatial dimensions
    Set<String> filtered = Sets.filter(
        Sets.newHashSet(row.getDimensions()),
        new Predicate<String>()
        {
          @Override
          public boolean apply(String input)
          {
            return !spatialDimNames.contains(input);
          }
        }
    );
    for (String dim : filtered) {
      finalDims.put(dim, row.getDimension(dim));
    }

    InputRow retVal = new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return Lists.newArrayList(finalDims.keySet());
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return row.getTimestampFromEpoch();
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return finalDims.get(dimension);
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return row.getFloatMetric(metric);
      }
    };

    for (SpatialDimensionSchema spatialDimension : spatialDimensions) {
      List<String> spatialDimVals = Lists.newArrayList();
      for (String partialSpatialDim : spatialDimension.getDims()) {
        List<String> dimVals = row.getDimension(partialSpatialDim);
        if (dimVals == null || dimVals.isEmpty()) {
          return retVal;
        }
        spatialDimVals.addAll(dimVals);
      }
      finalDims.put(spatialDimension.getDimName(), Arrays.asList(JOINER.join(spatialDimVals)));
    }

    return retVal;
  }
}
