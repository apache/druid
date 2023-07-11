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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidService;

import java.io.IOException;

/**
 * A custom serializer to handle the bug of duplicate "type" keys in {@link DataNodeService}.
 * This class can be removed together when we entirely remove the deprecated "type" property from DataNodeService.
 * See the Javadoc of DataNodeService for more details.
 */
public class DruidServiceSerializer extends StdSerializer<DruidService>
{
  private final JsonSerializer<Object> defaultSerializer;

  public DruidServiceSerializer(JsonSerializer<Object> defaultSerializer)
  {
    super(DruidService.class);
    this.defaultSerializer = defaultSerializer;
  }

  @Override
  public void serialize(DruidService druidService, JsonGenerator gen, SerializerProvider serializers) throws IOException
  {
    defaultSerializer.serialize(druidService, gen, serializers);
  }

  @Override
  public void serializeWithType(
      DruidService druidService,
      JsonGenerator gen,
      SerializerProvider serializers,
      TypeSerializer typeSer
  ) throws IOException
  {
    if (druidService instanceof DataNodeService) {
      DataNodeService dataNodeService = (DataNodeService) druidService;
      gen.writeStartObject();

      // Write subtype key first. This is important because Jackson picks up the first "type" field as the subtype key
      // for deserialization.
      gen.writeStringField("type", DataNodeService.DISCOVERY_SERVICE_KEY);

      // Write properties of DataNodeService
      gen.writeStringField("tier", dataNodeService.getTier());
      gen.writeNumberField("maxSize", dataNodeService.getMaxSize());
      // NOTE: the below line writes a duplicate key of "type".
      // This is a bug that DataNodeService has a key of the same name as the subtype key.
      // To address the bug, a new "serverType" field has been added.
      // However, we cannot remove the deprecated "type" entirely yet because it will break rolling upgrade.
      // It seems OK to have duplicate keys though because Jackson seems to always pick up the first "type" property
      // as the subtype key for deserialization.
      // This duplicate key should be removed in a future release.
      // See DiscoveryDruidNode.toMap() for deserialization of DruidServices.
      gen.writeObjectField("type", dataNodeService.getServerType());
      gen.writeObjectField("serverType", dataNodeService.getServerType());
      gen.writeNumberField("priority", dataNodeService.getPriority());

      gen.writeEndObject();
    } else {
      defaultSerializer.serializeWithType(druidService, gen, serializers, typeSer);
    }
  }
}
