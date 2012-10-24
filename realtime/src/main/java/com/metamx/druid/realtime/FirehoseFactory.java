package com.metamx.druid.realtime;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.IOException;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
                  @JsonSubTypes.Type(name = "kafka-0.6.3", value = KafkaFirehoseFactory.class)
              })
public interface FirehoseFactory
{
  /**
   * Initialization method that connects up the fire hose.  If this method returns successfully it should be safe to
   * call hasMore() on the returned Firehose (which might subsequently block).
   *
   * If this method returns null, then any attempt to call hasMore(), nextRow(), commit() and close() on the return
   * value will throw a surprising NPE.   Throwing IOException on connection failure or runtime exception on
   * invalid configuration is preferred over returning null.
   */
  public Firehose connect() throws IOException;
}
