/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.jackson;

import com.google.common.base.Throwables;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.Serializers;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.ToStringSerializer;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.TimeZone;

/**
 */
public class DefaultObjectMapper extends ObjectMapper
{
  public DefaultObjectMapper()
  {
    this(null);
  }

  public DefaultObjectMapper(JsonFactory factory)
  {
    super(factory);
    SimpleModule serializerModule = new SimpleModule("Druid default serializers", new Version(1, 0, 0, null));
    JodaStuff.register(serializerModule);
    serializerModule.addDeserializer(
        Granularity.class,
        new JsonDeserializer<Granularity>()
        {
          @Override
          public Granularity deserialize(JsonParser jp, DeserializationContext ctxt)
              throws IOException
          {
            return Granularity.valueOf(jp.getText().toUpperCase());
          }
        }
    );
    serializerModule.addDeserializer(
        DateTimeZone.class,
        new JsonDeserializer<DateTimeZone>()
        {
          @Override
          public DateTimeZone deserialize(JsonParser jp, DeserializationContext ctxt)
              throws IOException
          {
            String tzId = jp.getText();
            try {
              return DateTimeZone.forID(tzId);
            } catch(IllegalArgumentException e) {
              // also support Java timezone strings
              return DateTimeZone.forTimeZone(TimeZone.getTimeZone(tzId));
            }
          }
        }
    );
    serializerModule.addSerializer(
        DateTimeZone.class,
        new JsonSerializer<DateTimeZone>()
        {
          @Override
          public void serialize(
              DateTimeZone dateTimeZone,
              JsonGenerator jsonGenerator,
              SerializerProvider serializerProvider
          )
              throws IOException, JsonProcessingException
          {
            jsonGenerator.writeString(dateTimeZone.getID());
          }
        }
    );
    serializerModule.addSerializer(
        Sequence.class,
        new JsonSerializer<Sequence>()
        {
          @Override
          public void serialize(Sequence value, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException, JsonProcessingException
          {
            jgen.writeStartArray();
            value.accumulate(
                null,
                new Accumulator()
                {
                  @Override
                  public Object accumulate(Object o, Object o1)
                  {
                    try {
                      jgen.writeObject(o1);
                    }
                    catch (IOException e) {
                      throw Throwables.propagate(e);
                    }
                    return o;
                  }
                }
            );
            jgen.writeEndArray();
          }
        }
    );
    serializerModule.addSerializer(ByteOrder.class, ToStringSerializer.instance);
    serializerModule.addDeserializer(
        ByteOrder.class,
        new JsonDeserializer<ByteOrder>()
        {
          @Override
          public ByteOrder deserialize(
              JsonParser jp, DeserializationContext ctxt
          ) throws IOException, JsonProcessingException
          {
            if (ByteOrder.BIG_ENDIAN.toString().equals(jp.getText())) {
              return ByteOrder.BIG_ENDIAN;
            }
            return ByteOrder.LITTLE_ENDIAN;
          }
        }
    );
    registerModule(serializerModule);

    configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    configure(SerializationConfig.Feature.AUTO_DETECT_GETTERS, false);
    configure(SerializationConfig.Feature.AUTO_DETECT_FIELDS, false);
    configure(SerializationConfig.Feature.INDENT_OUTPUT, false);
  }
}
