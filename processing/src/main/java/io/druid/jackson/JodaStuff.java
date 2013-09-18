/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.joda.deser.DurationDeserializer;
import com.fasterxml.jackson.datatype.joda.deser.PeriodDeserializer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

/**
 */
class JodaStuff
{
  static SimpleModule register(SimpleModule module)
  {
    module.addKeyDeserializer(DateTime.class, new DateTimeKeyDeserializer());
    module.addDeserializer(DateTime.class, new DateTimeDeserializer());
    module.addSerializer(DateTime.class, ToStringSerializer.instance);
    module.addDeserializer(Interval.class, new JodaStuff.IntervalDeserializer());
    module.addSerializer(Interval.class, ToStringSerializer.instance);
    module.addDeserializer(Period.class, new PeriodDeserializer());
    module.addSerializer(Period.class, ToStringSerializer.instance);
    module.addDeserializer(Duration.class, new DurationDeserializer());
    module.addSerializer(Duration.class, ToStringSerializer.instance);

    return module;
  }

  /**
   */
  private static class IntervalDeserializer extends StdDeserializer<Interval>
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

  private static class DateTimeDeserializer extends StdDeserializer<DateTime>
  {
      public DateTimeDeserializer() {
        super(DateTime.class);
      }

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
