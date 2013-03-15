package com.metamx.druid.realtime.plumber;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.joda.time.Interval;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "intervalStart", value = IntervalStartVersioningPolicy.class)
})
public interface VersioningPolicy
{
  public String getVersion(Interval interval);
}
