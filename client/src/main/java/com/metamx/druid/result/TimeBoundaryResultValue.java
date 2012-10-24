package com.metamx.druid.result;

import com.metamx.common.IAE;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class TimeBoundaryResultValue
{
  private final Object value;

  @JsonCreator
  public TimeBoundaryResultValue(
      Object value
  )
  {
    this.value = value;
  }

  @JsonValue
  public Object getBaseObject()
  {
    return value;
  }

  public DateTime getMaxTime()
  {
    if (value instanceof Map) {
      return getDateTimeValue(((Map) value).get(TimeBoundaryQuery.MAX_TIME));
    } else {
      return getDateTimeValue(value);
    }
  }

  public DateTime getMinTime()
  {
    if (value instanceof Map) {
      return getDateTimeValue(((Map) value).get(TimeBoundaryQuery.MIN_TIME));
    } else {
      throw new IAE("MinTime not supported!");
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

    TimeBoundaryResultValue that = (TimeBoundaryResultValue) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "TimeBoundaryResultValue{" +
           "value=" + value +
           '}';
  }

  private DateTime getDateTimeValue(Object val)
  {
    if (val instanceof DateTime) {
      return (DateTime) val;
    } else if (val instanceof String) {
      return new DateTime(val);
    } else {
      throw new IAE("Cannot get time from type[%s]", val.getClass());
    }
  }
}