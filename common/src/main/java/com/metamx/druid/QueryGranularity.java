package com.metamx.druid;

import com.metamx.common.IAE;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.DateTime;
import org.joda.time.ReadableDuration;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property = "type", defaultImpl = QueryGranularity.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "period",   value = PeriodGranularity.class),
    @JsonSubTypes.Type(name = "duration", value = DurationGranularity.class),
    @JsonSubTypes.Type(name = "all",      value = AllGranularity.class),
    @JsonSubTypes.Type(name = "none",     value = NoneGranularity.class)
})
public abstract class QueryGranularity
{
  public abstract long next(long offset);

  public abstract long truncate(long offset);

  public abstract byte[] cacheKey();

  public abstract DateTime toDateTime(long offset);

  public abstract Iterable<Long> iterable(final long start, final long end);

  public static final QueryGranularity ALL = new AllGranularity();
  public static final QueryGranularity NONE = new NoneGranularity();

  public static final QueryGranularity MINUTE = fromString("MINUTE");
  public static final QueryGranularity HOUR   = fromString("HOUR");
  public static final QueryGranularity DAY    = fromString("DAY");
  public static final QueryGranularity SECOND = fromString("SECOND");

  @JsonCreator
  public static QueryGranularity fromString(String str)
  {
    String name = str.toUpperCase();
    if(name.equals("ALL"))
    {
      return QueryGranularity.ALL;
    }
    else if(name.equals("NONE"))
    {
      return QueryGranularity.NONE;
    }
    return new DurationGranularity(convertValue(str), 0);
  }

  private static enum MillisIn
  {
    SECOND         (            1000),
    MINUTE         (       60 * 1000),
    FIFTEEN_MINUTE (15 *   60 * 1000),
    THIRTY_MINUTE  (30 *   60 * 1000),
    HOUR           (     3600 * 1000),
    DAY            (24 * 3600 * 1000);

    private final long millis;
    MillisIn(final long millis) { this.millis = millis; }
  }

  private static long convertValue(Object o)
  {
    if(o instanceof String)
    {
      return MillisIn.valueOf(((String) o).toUpperCase()).millis;
    }
    else if(o instanceof ReadableDuration)
    {
      return ((ReadableDuration)o).getMillis();
    }
    else if(o instanceof Number)
    {
      return ((Number)o).longValue();
    }
    throw new IAE("Cannot convert [%s] to QueryGranularity", o.getClass());
  }
}
