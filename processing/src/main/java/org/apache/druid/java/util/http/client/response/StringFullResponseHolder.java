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
import io.netty.handler.codec.http.HttpResponse;

import java.nio.charset.Charset;

public class StringFullResponseHolder extends FullResponseHolder<String>
{
  private final StringBuilder builder = new StringBuilder();
  private final StreamingCharsetDecoder streamingDecoder;

  public StringFullResponseHolder(
      HttpResponse response,
      Charset charset
  )
  {
    super(response);
    this.streamingDecoder = new StreamingCharsetDecoder(builder, charset);
  }

  /**
   * Append the bytes of a chunk; multi-byte characters split across chunk boundaries are reassembled.
   * See {@link StreamingCharsetDecoder} for the details.
   */
  public StringFullResponseHolder addChunk(ByteBuf chunk)
  {
    streamingDecoder.append(chunk);
    return this;
  }

  /**
   * Append text that has already been decoded elsewhere. Does not touch the streaming-decoder state.
   */
  public StringFullResponseHolder addChunk(String chunk)
  {
    builder.append(chunk);
    return this;
  }

  /**
   * Signal end-of-input to the streaming decoder so any trailing bytes that could not yet form a
   * character emit replacement characters rather than silently disappearing.
   */
  public StringFullResponseHolder done()
  {
    streamingDecoder.finish();
    return this;
  }

  @Override
  public String getContent()
  {
    return builder.toString();
  }
}
