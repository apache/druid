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

package org.apache.druid.java.util.http.client.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectDecoder;

/**
 */
public class HttpClientInitializer extends ChannelInitializer<Channel>
{
  @Override
  public void initChannel(Channel channel)
  {
    channel.pipeline()
           .addLast("codec", new HttpClientCodec(
               HttpObjectDecoder.DEFAULT_MAX_INITIAL_LINE_LENGTH,
               HttpObjectDecoder.DEFAULT_MAX_HEADER_SIZE,
               HttpObjectDecoder.DEFAULT_MAX_CHUNK_SIZE,
               HttpClientCodec.DEFAULT_FAIL_ON_MISSING_RESPONSE,
               HttpObjectDecoder.DEFAULT_VALIDATE_HEADERS,
               true // continue parsing requests after HTTP CONNECT
           ))
           .addLast("inflater", new HttpContentDecompressor());
  }
}
