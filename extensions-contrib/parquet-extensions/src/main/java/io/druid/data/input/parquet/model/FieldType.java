/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.parquet.model;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public enum FieldType
{
  STRING(String.class), MAP(Map.class), INT(Integer.class), LONG(Long.class), UNION(Object.class), STRUCT(Object.class),
  UTF8(Utf8.class), LIST(List.class), GENERIC_RECORD(GenericRecord.class), BYTE_BUFFER(ByteBuffer.class),
  GENERIC_DATA_RECORD(GenericData.Record.class);

  private final Class<?> classz;

  FieldType(Class<?> classz)
  {
    this.classz = classz;
  }

  public Class<?> getClassz()
  {
    return classz;
  }
}
