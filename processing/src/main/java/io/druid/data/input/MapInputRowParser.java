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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.exception.FormattedException;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapInputRowParser implements InputRowParser<Map<String, Object>>
{
  private final TimestampSpec timestampSpec;
  private List<String> dimensions;
  private final Set<String> dimensionExclusions;

  @JsonCreator
  public MapInputRowParser(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions
  )
  {
    this.timestampSpec = timestampSpec;
    if (dimensions != null) {
       this.dimensions = ImmutableList.copyOf(dimensions);
    }
    this.dimensionExclusions = Sets.newHashSet();
    if (dimensionExclusions != null) {
      for (String dimensionExclusion : dimensionExclusions) {
        this.dimensionExclusions.add(dimensionExclusion.toLowerCase());
      }
    }
    this.dimensionExclusions.add(timestampSpec.getTimestampColumn().toLowerCase());
  }

  @Override
  public InputRow parse(Map<String, Object> theMap) throws FormattedException
  {
    final List<String> dimensions = hasCustomDimensions()
                                    ? this.dimensions
                                    : Lists.newArrayList(Sets.difference(theMap.keySet(), dimensionExclusions));

    final DateTime timestamp;
    try {
      timestamp = timestampSpec.extractTimestamp(theMap);
      if (timestamp == null) {
        final String input = theMap.toString();
        throw new NullPointerException(
            String.format(
                "Null timestamp in input: %s",
                input.length() < 100 ? input : input.substring(0, 100) + "..."
            )
        );
      }
    }
    catch (Exception e) {
      throw new FormattedException.Builder()
          .withErrorCode(FormattedException.ErrorCode.UNPARSABLE_TIMESTAMP)
          .withMessage(e.toString())
          .build();
    }

    return new MapBasedInputRow(timestamp.getMillis(), dimensions, theMap);
  }

  private boolean hasCustomDimensions() {
    return dimensions != null;
  }

  @Override
  public void addDimensionExclusion(String dimension)
  {
    dimensionExclusions.add(dimension);
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public Set<String> getDimensionExclusions()
  {
    return dimensionExclusions;
  }
}
