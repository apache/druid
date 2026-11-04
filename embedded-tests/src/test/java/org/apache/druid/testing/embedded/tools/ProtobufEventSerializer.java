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

package org.apache.druid.testing.embedded.tools;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.druid.data.input.protobuf.FileBasedProtobufBytesDecoder;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testing.embedded.indexing.MoreResources;

import java.util.List;

public class ProtobufEventSerializer implements EventSerializer
{
  public static final String TYPE = "protobuf";

  public static final Descriptors.Descriptor DESCRIPTOR = new FileBasedProtobufBytesDecoder(
      MoreResources.ProtobufData.WIKI_PROTOBUF_BYTES_DECODER_RESOURCE,
      MoreResources.ProtobufData.WIKI_PROTO_MESSAGE_TYPE
  ).getDescriptor();

  @Override
  public byte[] serialize(List<Pair<String, Object>> event)
  {
    final DynamicMessage.Builder builder = DynamicMessage.newBuilder(DESCRIPTOR);
    for (Pair<String, Object> pair : event) {
      builder.setField(DESCRIPTOR.findFieldByName(pair.lhs), pair.rhs);
    }
    return builder.build().toByteArray();
  }

  @Override
  public void close()
  {
  }
}
