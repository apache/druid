/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.joda.deser.DurationDeserializer;
import com.fasterxml.jackson.datatype.joda.deser.PeriodDeserializer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.IOException;

/**
 */
class JodaStuff
{
  @SuppressWarnings("unchecked")
  static SimpleModule register(SimpleModule module)
  {
    module.addKeyDeserializer(DateTime.class, new DateTimeKeyDeserializer());
    module.addDeserializer(DateTime.class, new DateTimeDeserializer());
    module.addSerializer(DateTime.class, ToStringSerializer.instance);
    module.addDeserializer(Interval.class, new JodaStuff.IntervalDeserializer());
    module.addKeyDeserializer(Interval.class, new JodaStuff.IntervalKeyDeserializer());
    module.addSerializer(Interval.class, ToStringSerializer.instance);
    JsonDeserializer<?> periodDeserializer = new PeriodDeserializer();
    module.addDeserializer(Period.class, (JsonDeserializer<Period>) periodDeserializer);
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
        throws IOException
    {
      return Intervals.of(jsonParser.getText());
    }
  }

  private static class IntervalKeyDeserializer extends KeyDeserializer
  {
    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt)
    {
      return Intervals.of(key);
    }
  }

  private static class DateTimeKeyDeserializer extends KeyDeserializer
  {
    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt)
    {
      return DateTimes.of(key);
    }
  }

  private static class DateTimeDeserializer extends StdDeserializer<DateTime>
  {
    public DateTimeDeserializer()
    {
      super(DateTime.class);
    }

    @Override
    public DateTime deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
    {
      JsonToken t = jp.getCurrentToken();
      if (t == JsonToken.VALUE_NUMBER_INT) {
        return DateTimes.utc(jp.getLongValue());
      }
      if (t == JsonToken.VALUE_STRING) {
        String str = jp.getText().trim();
        if (str.length() == 0) { // [JACKSON-360]
          return null;
        }
        // make sure to preserve time zone information when parsing timestamps
        return DateTimes.ISO_DATE_OR_TIME_WITH_OFFSET.parse(str);
      }
      throw ctxt.mappingException(getValueClass());
    }
  }
}
