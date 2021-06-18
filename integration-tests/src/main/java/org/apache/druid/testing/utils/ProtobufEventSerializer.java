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

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;

public class ProtobufEventSerializer implements EventSerializer
{
  public static final String TYPE = "protobuf";

  private static final Logger LOGGER = new Logger(ProtobufEventSerializer.class);

  public static final DynamicSchema SCHEMA;

  static {
    DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
    MessageDefinition wikiDef = MessageDefinition.newBuilder("Wikipedia")
        .addField("optional", "string", "timestamp", 1)
        .addField("optional", "string", "page", 2)
        .addField("optional", "string", "language", 3)
        .addField("optional", "string", "user", 4)
        .addField("optional", "string", "unpatrolled", 5)
        .addField("optional", "string", "newPage", 6)
        .addField("optional", "string", "robot", 7)
        .addField("optional", "string", "anonymous", 8)
        .addField("optional", "string", "namespace", 9)
        .addField("optional", "string", "continent", 10)
        .addField("optional", "string", "country", 11)
        .addField("optional", "string", "region", 12)
        .addField("optional", "string", "city", 13)
        .addField("optional", "int32", "added", 14)
        .addField("optional", "int32", "deleted", 15)
        .addField("optional", "int32", "delta", 16)
        .build();
    schemaBuilder.addMessageDefinition(wikiDef);
    DynamicSchema schema = null;
    try {
      schema = schemaBuilder.build();
    }
    catch (Descriptors.DescriptorValidationException e) {
      LOGGER.error("Could not init protobuf schema.");
    }
    SCHEMA = schema;
  }

  @Override
  public byte[] serialize(List<Pair<String, Object>> event)
  {
    DynamicMessage.Builder builder = SCHEMA.newMessageBuilder("Wikipedia");
    Descriptors.Descriptor msgDesc = builder.getDescriptorForType();
    for (Pair<String, Object> pair : event) {
      builder.setField(msgDesc.findFieldByName(pair.lhs), pair.rhs);
    }
    return builder.build().toByteArray();
  }

  @Override
  public void close()
  {
  }
}
