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

package org.apache.druid.testing.utils;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.druid.java.util.common.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroEventSerializer implements EventSerializer
{
  public static final String TYPE = "avro";

  private static final Schema SCHEMA = SchemaBuilder
      .record("wikipedia")
      .namespace("org.apache.druid")
      .fields()
      .requiredString("timestamp")
      .requiredString("page")
      .requiredString("language")
      .requiredString("user")
      .requiredString("unpatrolled")
      .requiredString("newPage")
      .requiredString("robot")
      .requiredString("anonymous")
      .requiredString("namespace")
      .requiredString("continent")
      .requiredString("country")
      .requiredString("region")
      .requiredString("city")
      .requiredInt("added")
      .requiredInt("deleted")
      .requiredInt("delta")
      .endRecord();

  private final DatumWriter<Object> writer = new GenericDatumWriter<>(SCHEMA);

  @Override
  public byte[] serialize(List<Pair<String, Object>> event) throws IOException
  {
    final WikipediaRecord record = new WikipediaRecord();
    event.forEach(pair -> record.put(pair.lhs, pair.rhs));
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(record, encoder);
    encoder.flush();
    out.close();
    return out.toByteArray();
  }

  @Override
  public void close()
  {
  }

  private static class WikipediaRecord implements GenericRecord
  {
    private final Map<String, Object> event = new HashMap<>();
    private final BiMap<Integer, String> indexes = HashBiMap.create(SCHEMA.getFields().size());

    private int nextIndex = 0;

    @Override
    public void put(String key, Object v)
    {
      event.put(key, v);
      indexes.inverse().computeIfAbsent(key, k -> nextIndex++);
    }

    @Override
    public Object get(String key)
    {
      return event.get(key);
    }

    @Override
    public void put(int i, Object v)
    {
      final String key = indexes.get(i);
      if (key == null) {
        throw new IndexOutOfBoundsException();
      }
      put(key, v);
    }

    @Override
    public Object get(int i)
    {
      final String key = indexes.get(i);
      if (key == null) {
        throw new IndexOutOfBoundsException();
      }
      return get(key);
    }

    @Override
    public Schema getSchema()
    {
      return SCHEMA;
    }
  }
}
