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

package org.apache.druid.java.util.http.client.response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BytesFullResponseHolder extends FullResponseHolder<byte[]>
{
  private final List<byte[]> chunks;

  public BytesFullResponseHolder(HttpResponse response)
  {
    super(response);
    this.chunks = new ArrayList<>();
  }

  public BytesFullResponseHolder addChunk(byte[] chunk)
  {
    chunks.add(chunk);
    return this;
  }

  @Override
  public byte[] getContent()
  {
    int size = 0;
    for (byte[] chunk : chunks) {
      size += chunk.length;
    }
    ByteBuffer buf = ByteBuffer.wrap(new byte[size]);

    for (byte[] chunk : chunks) {
      buf.put(chunk);
    }

    return buf.array();
  }

  /**
   * Deserialize a {@link BytesFullResponseHolder} as JSON.
   */
  public <T> T deserialize(final ObjectMapper jsonMapper, final TypeReference<T> typeReference)
  {
    try {
      return jsonMapper.readValue(getContent(), typeReference);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
