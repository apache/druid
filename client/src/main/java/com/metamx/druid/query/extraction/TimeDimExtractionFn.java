package com.metamx.druid.query.extraction;

import com.ibm.icu.text.SimpleDateFormat;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;

/**
 */
public class TimeDimExtractionFn implements DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x0;

  private final String timeFormat;
  private final SimpleDateFormat timeFormatter;
  private final String resultFormat;
  private final SimpleDateFormat resultFormatter;

  @JsonCreator
  public TimeDimExtractionFn(
      @JsonProperty("timeFormat") String timeFormat,
      @JsonProperty("resultFormat") String resultFormat
  )
  {
    this.timeFormat = timeFormat;
    this.timeFormatter = new SimpleDateFormat(timeFormat);
    this.timeFormatter.setLenient(true);

    this.resultFormat = resultFormat;
    this.resultFormatter = new SimpleDateFormat(resultFormat);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] timeFormatBytes = timeFormat.getBytes();
    return ByteBuffer.allocate(1 + timeFormatBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(timeFormatBytes)
                     .array();
  }

  @Override
  public String apply(String dimValue)
  {
    Date date;
    try {
      date = timeFormatter.parse(dimValue);
    }
    catch (ParseException e) {
      return dimValue;
    }
    return resultFormatter.format(date);
  }

  @JsonProperty("timeFormat")
  public String getTimeFormat()
  {
    return timeFormat;
  }

  @JsonProperty("resultFormat")
  public String getResultFormat()
  {
    return resultFormat;
  }

  @Override
  public String toString()
  {
    return "TimeDimExtractionFn{" +
           "timeFormat='" + timeFormat + '\'' +
           ", resultFormat='" + resultFormat + '\'' +
           '}';
  }
}
