package com.metamx.druid.master.rules;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.Interval;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "loadByPeriod", value = PeriodLoadRule.class),
    @JsonSubTypes.Type(name = "loadByInterval", value = IntervalLoadRule.class),
    @JsonSubTypes.Type(name = "dropByPeriod", value = PeriodDropRule.class),
    @JsonSubTypes.Type(name = "dropByInterval", value = IntervalDropRule.class)
})

public interface Rule
{
  public String getType();

  public boolean appliesTo(Interval interval);
}
