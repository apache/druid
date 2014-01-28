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

package io.druid.query.aggregation;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import gnu.trove.map.hash.TIntByteHashMap;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class HyperloglogDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new HyperloglogJacksonSerdeModule().registerSubtypes(
            new NamedType(HyperloglogAggregatorFactory.class, "hyperloglog")
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType("hyperloglog") == null) {
      ComplexMetrics.registerSerde("hyperloglog", new HyperloglogComplexMetricSerde());
    }
  }

  public static class HyperloglogJacksonSerdeModule extends SimpleModule
  {
    public HyperloglogJacksonSerdeModule()
    {
      super("Hyperloglog deserializers");

      addDeserializer(
          TIntByteHashMap.class,
          new JsonDeserializer<TIntByteHashMap>()
          {
            @Override
            public TIntByteHashMap deserialize(
                JsonParser jp,
                DeserializationContext ctxt
            ) throws IOException
            {
              byte[] ibmapByte = Base64.decodeBase64(jp.getText());

              ByteBuffer buffer = ByteBuffer.wrap(ibmapByte);
              int keylength = buffer.getInt();
              int valuelength = buffer.getInt();
              if (keylength == 0) {
                return (new TIntByteHashMap());
              }
              int[] keys = new int[keylength];
              byte[] values = new byte[valuelength];

              for (int i = 0; i < keylength; i++) {
                keys[i] = buffer.getInt();
              }
              buffer.get(values);

              return (new TIntByteHashMap(keys, values));
            }
          }
      );

      addSerializer(
          TIntByteHashMap.class,
          new JsonSerializer<TIntByteHashMap>()
          {
            @Override
            public void serialize(
                TIntByteHashMap ibmap,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider
            )
                throws IOException, JsonProcessingException
            {
              int[] indexesResult = ibmap.keys();
              byte[] valueResult = ibmap.values();
              ByteBuffer buffer = ByteBuffer
                  .allocate(
                      4 * indexesResult.length
                      + valueResult.length + 8
                  );
              byte[] result = new byte[4 * indexesResult.length
                                       + valueResult.length + 8];
              buffer.putInt((int) indexesResult.length);
              buffer.putInt((int) valueResult.length);
              for (int i = 0; i < indexesResult.length; i++) {
                buffer.putInt(indexesResult[i]);
              }

              buffer.put(valueResult);
              buffer.flip();
              buffer.get(result);
              String str = Base64.encodeBase64String(result);
              jsonGenerator.writeString(str);
            }
          }
      );

    }
  }
}
