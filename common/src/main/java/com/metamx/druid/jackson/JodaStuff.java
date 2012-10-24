package com.metamx.druid.jackson;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.KeyDeserializer;
import org.codehaus.jackson.map.deser.std.StdDeserializer;
import org.codehaus.jackson.map.ext.JodaDeserializers;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.ToStringSerializer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.ReadableInstant;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

/**
 */
public class JodaStuff
{
  public static SimpleModule register(SimpleModule module)
  {
    module.addKeyDeserializer(DateTime.class, new DateTimeKeyDeserializer());
    module.addDeserializer(DateTime.class, new DateTimeDeserializer());
    module.addSerializer(DateTime.class, ToStringSerializer.instance);
    module.addDeserializer(Interval.class, new JodaStuff.IntervalDeserializer());
    module.addSerializer(Interval.class, ToStringSerializer.instance);

    return module;
  }

  /**
   */
  public static class IntervalDeserializer extends StdDeserializer<Interval>
  {
    public IntervalDeserializer()
    {
      super(Interval.class);
    }

    @Override
    public Interval deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException
    {
      return new Interval(jsonParser.getText());
    }
  }

  private static class DateTimeKeyDeserializer extends KeyDeserializer
  {
    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException, JsonProcessingException
    {
      return new DateTime(key);
    }
  }

  public static class DateTimeDeserializer extends StdDeserializer<DateTime>
  {
      public DateTimeDeserializer() {
        super(DateTime.class);
      }

      @SuppressWarnings("unchecked")
      @Override
      public DateTime deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException, JsonProcessingException
      {
          JsonToken t = jp.getCurrentToken();
          if (t == JsonToken.VALUE_NUMBER_INT) {
              return new DateTime(jp.getLongValue());
          }
          if (t == JsonToken.VALUE_STRING) {
              String str = jp.getText().trim();
              if (str.length() == 0) { // [JACKSON-360]
                  return null;
              }
              // make sure to preserve time zone information when parsing timestamps
              return ISODateTimeFormat.dateTimeParser()
                                      .withOffsetParsed()
                                      .parseDateTime(str);
          }
          throw ctxt.mappingException(getValueClass());
      }
  }

}
