package com.metamx.druid.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.druid.input.InputRow;
import org.joda.time.DateTime;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
                  @JsonSubTypes.Type(name = "wikipedia", value = WikipediaIrcDecoder.class)
              })
public interface IrcDecoder
{
  public InputRow decodeMessage(DateTime timestamp, String channel, String msg);
}
