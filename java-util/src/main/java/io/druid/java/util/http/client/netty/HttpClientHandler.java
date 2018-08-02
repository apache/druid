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

package io.druid.java.util.http.client.netty;

import io.druid.java.util.common.logger.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 */
@ChannelHandler.Sharable
public class HttpClientHandler extends SimpleChannelUpstreamHandler
{
  private static final Logger LOGGER = new Logger(HttpClientHandler.class);

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
  {
    try {
      final Throwable cause = e.getCause();
      if (cause != null) {
        LOGGER.error(cause, "Caught exception from [%s] channel handler", ctx.getName());
      }
    }
    finally {
      ctx.getChannel().close();
    }
  }
}
