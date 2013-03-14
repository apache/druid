package com.metamx.druid.realtime.plumber;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.joda.time.Period;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "serverTime", value = ServerTimeRejectionPolicyFactory.class),
    @JsonSubTypes.Type(name = "messageTime", value = MessageTimeRejectionPolicyFactory.class)
})
public interface RejectionPolicyFactory
{
  public RejectionPolicy create(Period windowPeriod);
}
