package com.metamx.druid.indexer.data;

import com.google.common.base.Function;
import com.metamx.common.parsers.ParserUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class TimestampSpec
{
  private final String timestampColumn;
  private final String timestampFormat;
  private final Function<String, DateTime> timestampConverter;

  @JsonCreator
  public TimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format
  )
  {
    this.timestampColumn = timestampColumn;
    this.timestampFormat = format;
    this.timestampConverter = ParserUtils.createTimestampParser(format);
  }

  @JsonProperty("column")
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @JsonProperty("format")
  public String getTimestampFormat()
  {
    return timestampFormat;
  }

  public DateTime extractTimestamp(Map<String, Object> input)
  {
    final Object o = input.get(timestampColumn);

    return o == null ? null : timestampConverter.apply(o.toString());
  }
}
