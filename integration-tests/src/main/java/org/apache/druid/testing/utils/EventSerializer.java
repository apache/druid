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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.apache.druid.java.util.common.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * EventSerializer is for serializing an event into a byte array.
 * This interface is used to write generated events on stream processing systems such as Kafka or Kinesis
 * in integration tests.
 *
 * @see SyntheticStreamGenerator
 * @see StreamEventWriter
 */
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = JsonEventSerializer.TYPE, value = JsonEventSerializer.class),
    @Type(name = CsvEventSerializer.TYPE, value = CsvEventSerializer.class),
    @Type(name = DelimitedEventSerializer.TYPE, value = DelimitedEventSerializer.class),
    @Type(name = AvroEventSerializer.TYPE, value = AvroEventSerializer.class)
})
public interface EventSerializer extends Closeable
{
  byte[] serialize(List<Pair<String, Object>> event) throws IOException;
}
