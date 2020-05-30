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

import com.opencsv.CSVWriter;
import org.apache.druid.java.util.common.Pair;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class CsvEventSerializer implements EventSerializer
{
  public static final String TYPE = "csv";

  private final ByteArrayOutputStream bos = new ByteArrayOutputStream();
  private final CSVWriter writer = new CSVWriter(
      new BufferedWriter(new OutputStreamWriter(bos, StandardCharsets.UTF_8))
  );

  @Override
  public byte[] serialize(List<Pair<String, Object>> event) throws IOException
  {
    //noinspection ConstantConditions
    writer.writeNext(event.stream().map(pair -> pair.rhs.toString()).toArray(String[]::new));
    writer.flush();
    final byte[] serialized = bos.toByteArray();
    bos.reset();
    return serialized;
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }
}
