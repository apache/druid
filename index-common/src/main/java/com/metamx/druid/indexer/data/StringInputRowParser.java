package com.metamx.druid.indexer.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.exception.FormattedException;
import com.metamx.common.parsers.Parser;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class StringInputRowParser
{
  private final TimestampSpec timestampSpec;
  private final DataSpec dataSpec;

  private final Set<String> dimensionExclusions;
  private final Parser<String, Object> parser;

  @JsonCreator
  public StringInputRowParser(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("data") DataSpec dataSpec,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions
  )
  {
    this.timestampSpec = timestampSpec;
    this.dataSpec = dataSpec;

    this.dimensionExclusions = Sets.newHashSet();
    if (dimensionExclusions != null) {
      this.dimensionExclusions.addAll(dimensionExclusions);
    }
    this.dimensionExclusions.add(timestampSpec.getTimestampColumn());

    this.parser = dataSpec.getParser();
  }

  public StringInputRowParser addDimensionExclusion(String dimension)
  {
    dimensionExclusions.add(dimension);
    return this;
  }

  public InputRow parse(String input) throws FormattedException
  {
    Map<String, Object> theMap = parser.parse(input);

    final List<String> dimensions = dataSpec.hasCustomDimensions()
                                    ? dataSpec.getDimensions()
                                    : Lists.newArrayList(Sets.difference(theMap.keySet(), dimensionExclusions));

    final DateTime timestamp = timestampSpec.extractTimestamp(theMap);
    if (timestamp == null) {
      throw new NullPointerException(
          String.format(
              "Null timestamp in input string: %s",
              input.length() < 100 ? input : input.substring(0, 100) + "..."
          )
      );
    }

    return new MapBasedInputRow(timestamp.getMillis(), dimensions, theMap);
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
