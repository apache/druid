package com.metamx.druid.indexer.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapInputRowParser implements InputRowParser<Map<String, Object>>
{
  private final TimestampSpec timestampSpec;
  private final DataSpec dataSpec;
  private final Set<String> dimensionExclusions;

  @JsonCreator
  public MapInputRowParser(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("data") DataSpec dataSpec,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions
  )
  {
    this.timestampSpec = timestampSpec;
    this.dataSpec = dataSpec;
    this.dimensionExclusions = Sets.newHashSet();
    if (dimensionExclusions != null) {
      for (String dimensionExclusion : dimensionExclusions) {
        this.dimensionExclusions.add(dimensionExclusion.toLowerCase());
      }
    }
    this.dimensionExclusions.add(timestampSpec.getTimestampColumn().toLowerCase());
  }

  @Override
  public InputRow parse(Map<String, Object> theMap)
  {
    final List<String> dimensions = dataSpec.hasCustomDimensions()
                                    ? dataSpec.getDimensions()
                                    : Lists.newArrayList(Sets.difference(theMap.keySet(), dimensionExclusions));

    final DateTime timestamp = timestampSpec.extractTimestamp(theMap);
    if (timestamp == null) {
      final String input = theMap.toString();
      throw new NullPointerException(
          String.format(
              "Null timestamp in input: %s",
              input.length() < 100 ? input : input.substring(0, 100) + "..."
          )
      );
    }

    return new MapBasedInputRow(timestamp.getMillis(), dimensions, theMap);
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

  @JsonProperty("data")
  public DataSpec getDataSpec()
  {
    return dataSpec;
  }

  @JsonProperty
  public Set<String> getDimensionExclusions()
  {
    return dimensionExclusions;
  }
}
