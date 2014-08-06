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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Throwables;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.TimeZone;

/**
 */
public class DruidDefaultSerializersModule extends SimpleModule
{
  public DruidDefaultSerializersModule()
  {
    super("Druid default serializers");

    JodaStuff.register(this);
    addDeserializer(
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
    addDeserializer(
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
    addSerializer(
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
    addSerializer(
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
                new Accumulator<Object, Object>()
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
                    return null;
                  }
                }
            );
            jgen.writeEndArray();
          }
        }
    );
    addSerializer(
        Yielder.class,
        new JsonSerializer<Yielder>()
        {
          @Override
          public void serialize(Yielder yielder, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException, JsonProcessingException
          {
            try {
              jgen.writeStartArray();
              while (!yielder.isDone()) {
                final Object o = yielder.get();
                jgen.writeObject(o);
                yielder = yielder.next(null);
              }
              jgen.writeEndArray();
            } finally {
              yielder.close();
            }
          }
        }
    );
    addSerializer(ByteOrder.class, ToStringSerializer.instance);
    addDeserializer(
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
  }
}
