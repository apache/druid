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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.FrameBasedInlineDataSourceSerializer;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContextDeserializer;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;

/**
 *
 */
@SuppressWarnings("serial")
public class DruidDefaultSerializersModule extends SimpleModule
{
  @SuppressWarnings("rawtypes")
  public DruidDefaultSerializersModule()
  {
    super("Druid default serializers");

    JodaStuff.register(this);

    addSerializer(FrameBasedInlineDataSource.class, new FrameBasedInlineDataSourceSerializer());

    addDeserializer(
        DateTimeZone.class,
        new JsonDeserializer<DateTimeZone>()
        {
          @Override
          public DateTimeZone deserialize(JsonParser jp, DeserializationContext ctxt)
              throws IOException
          {
            String tzId = jp.getText();
            return DateTimes.inferTzFromString(tzId);
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
          ) throws IOException
          {
            jsonGenerator.writeString(dateTimeZone.getID());
          }
        }
    );
    addSerializer(
        Sequence.class,
        new JsonSerializer<Sequence>()
        {
          @SuppressWarnings("unchecked")
          @Override
          public void serialize(Sequence value, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException
          {
            jgen.writeStartArray();
            value.accumulate(
                null,
                new Accumulator<Object, Object>()
                {
                  // Save allocations in jgen.writeObject by caching serializer.
                  JsonSerializer<Object> serializer = null;
                  Class<?> serializerClass = null;

                  @Override
                  public Object accumulate(Object ignored, Object object)
                  {
                    try {
                      if (object == null) {
                        jgen.writeNull();
                      } else {
                        final Class<?> clazz = object.getClass();

                        if (serializerClass != clazz) {
                          serializer = JacksonUtils.getSerializer(provider, clazz);
                          serializerClass = clazz;
                        }

                        serializer.serialize(object, jgen, provider);
                      }
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
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
          @SuppressWarnings("unchecked")
          @Override
          public void serialize(Yielder yielder, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException
          {
            // Save allocations in jgen.writeObject by caching serializer.
            JsonSerializer<Object> serializer = null;
            Class<?> serializerClass = null;

            try {
              jgen.writeStartArray();
              while (!yielder.isDone()) {
                final Object o = yielder.get();
                if (o == null) {
                  jgen.writeNull();
                } else {
                  final Class<?> clazz = o.getClass();

                  if (serializerClass != clazz) {
                    serializer = JacksonUtils.getSerializer(provider, clazz);
                    serializerClass = clazz;
                  }

                  serializer.serialize(o, jgen, provider);
                }

                yielder = yielder.next(null);
              }
              jgen.writeEndArray();
            }
            finally {
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
          public ByteOrder deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
          {
            if (ByteOrder.BIG_ENDIAN.toString().equals(jp.getText())) {
              return ByteOrder.BIG_ENDIAN;
            }
            return ByteOrder.LITTLE_ENDIAN;
          }
        }
    );
    addDeserializer(ResponseContext.class, new ResponseContextDeserializer());

    addSerializer(RowsAndColumns.class, new JsonSerializer<RowsAndColumns>()
    {
      @Override
      public void serialize(
          RowsAndColumns value,
          JsonGenerator gen,
          SerializerProvider serializers
      ) throws IOException
      {
        // It would be really cool if jackson offered an output stream that would allow us to push bytes
        // through, but it doesn't right now, so we have to build a byte[] instead.  Maybe something to contribute
        // back to Jackson at some point.

        if(value instanceof FrameRowsAndColumns) {

          FrameRowsAndColumns frc = (FrameRowsAndColumns) value;

          gen.writeObject(frc.getSignature());

          Frame frame = frc.getFrame();
          final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          frame.writeTo(
              Channels.newChannel(baos),
              false,
              ByteBuffer.allocate(Frame.compressionBufferSize((int) frame.numBytes())),
              ByteTracker.unboundedTracker()
          );
        gen.writeBinary(baos.toByteArray());
//          byte[] b = new byte[] {1, 2, 3};
//          gen.writeBinary(b);

        } else {
          throw DruidException.defensive("expected frame");
        }

      }
    });

    addDeserializer(RowsAndColumns.class, new JsonDeserializer<RowsAndColumns>()
    {
      @Override
      public RowsAndColumns deserialize(
          JsonParser p,
          DeserializationContext ctxt
      ) throws IOException
      {
        RowSignature sig = p.readValueAs(RowSignature.class);
        p.nextValue();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        p.readBinaryValue(baos);
//        p.readBinaryValue(baos2);
        return new FrameRowsAndColumns(Frame.wrap(baos.toByteArray()), sig);
      }
    });
  }
}
