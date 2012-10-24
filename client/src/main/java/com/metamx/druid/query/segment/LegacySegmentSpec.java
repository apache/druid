package com.metamx.druid.query.segment;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import org.codehaus.jackson.annotate.JsonCreator;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class LegacySegmentSpec extends MultipleIntervalSegmentSpec
{
  private static List<Interval> convertValue(Object intervals)
  {
    final List<?> intervalStringList;
    if (intervals instanceof String) {
      intervalStringList = Arrays.asList((((String) intervals).split(",")));
    } else if (intervals instanceof Map) {
      intervalStringList = (List) ((Map) intervals).get("intervals");
    } else if (intervals instanceof List) {
      intervalStringList = (List) intervals;
    } else {
      throw new IAE("Unknown type[%s] for intervals[%s]", intervals.getClass(), intervals);
    }

    return Lists.transform(
        intervalStringList,
        new Function<Object, Interval>()
        {
          @Override
          public Interval apply(Object input)
          {
            return new Interval(input);
          }
        }
    );
  }

  @JsonCreator
  public LegacySegmentSpec(
      Object intervals
  )
  {
    super(convertValue(intervals));
  }
}
