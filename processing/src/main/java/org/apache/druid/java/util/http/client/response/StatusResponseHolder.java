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

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 */
public class StatusResponseHolder
{
  private final HttpResponseStatus status;
  private final StringBuilder builder;
  private StreamingCharsetDecoder streamingDecoder;

  public StatusResponseHolder(
      HttpResponseStatus status,
      StringBuilder builder
  )
  {
    this.status = status;
    this.builder = builder;
  }

  public HttpResponseStatus getStatus()
  {
    return status;
  }

  public StringBuilder getBuilder()
  {
    return builder;
  }

  public String getContent()
  {
    return builder.toString();
  }

  /**
   * Append the bytes of a chunk, decoded as UTF-8; multi-byte characters split across chunk
   * boundaries are reassembled. See {@link StreamingCharsetDecoder} for the details.
   */
  public StatusResponseHolder addChunk(ByteBuf chunk)
  {
    return addChunk(chunk, StandardCharsets.UTF_8);
  }

  public StatusResponseHolder addChunk(ByteBuf chunk, Charset charset)
  {
    if (streamingDecoder == null) {
      streamingDecoder = new StreamingCharsetDecoder(builder, charset);
    }
    streamingDecoder.append(chunk);
    return this;
  }

  /**
   * Signal end-of-input to the streaming decoder so any trailing bytes that could not yet form a
   * character emit replacement characters rather than silently disappearing. A no-op if no bytes
   * were ever appended.
   */
  public StatusResponseHolder done()
  {
    if (streamingDecoder != null) {
      streamingDecoder.finish();
    }
    return this;
  }
}
