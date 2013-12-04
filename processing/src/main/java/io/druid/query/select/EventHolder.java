package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class EventHolder
{
  private final long timestamp;
  private final Map<String, Object> entries = Maps.newLinkedHashMap();

  public EventHolder(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public void put(String key, Object val)
  {
    entries.put(key, val);
  }

  public void putAll(Map<String, Object> data)
  {
    entries.putAll(data);
  }

  public Object get(String key)
  {
    return entries.get(key);
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  @JsonValue
  public Map<String, Object> getBaseObject()
  {
    Map<String, Object> retVal = Maps.newLinkedHashMap();
    retVal.put("timestamp", new DateTime(timestamp));
    retVal.putAll(entries);
    return retVal;
  }
}
