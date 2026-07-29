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

package org.apache.druid.java.util.http.client.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

public class ChannelResourceFactoryTest
{
  private ServerSocket serverSocket;
  private EventLoopGroup eventLoopGroup;
  private Channel channel;
  private ChannelResourceFactory factory;

  @Before
  public void setUp() throws Exception
  {
    serverSocket = new ServerSocket(0);
    eventLoopGroup = new NioEventLoopGroup(1);

    final Bootstrap bootstrap = new Bootstrap()
        .group(eventLoopGroup)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<>()
        {
          @Override
          protected void initChannel(Channel ch)
          {
            // Nothing to add: these tests never exchange data, they only need a real, active channel.
          }
        });

    channel = bootstrap.connect(new InetSocketAddress("localhost", serverSocket.getLocalPort()))
                       .sync()
                       .channel();

    factory = new ChannelResourceFactory(bootstrap, null, null, null, -1);
  }

  @After
  public void tearDown() throws Exception
  {
    if (channel != null) {
      channel.close().await(10, TimeUnit.SECONDS);
    }
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS).await(30, TimeUnit.SECONDS);
    }
    if (serverSocket != null) {
      serverSocket.close();
    }
  }

  /**
   * {@link ResourcePool} calls isGood the moment it takes a resource, which can be while the connect (or a proxy or
   * TLS handshake chained onto it) is still in flight. Judging such a future on the spot would report failure and the
   * pool would close a channel that is about to become usable, only to open another one in its place.
   */
  @Test(timeout = 60_000L)
  public void testIsGoodWaitsForAPendingFuture() throws Exception
  {
    final ChannelPromise pending = channel.newPromise();

    final Thread completer = new Thread(() -> {
      try {
        Thread.sleep(500L);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      pending.setSuccess();
    }, "channel-resource-factory-test-completer");
    completer.setDaemon(true);
    completer.start();

    Assert.assertFalse("the future is still in flight", pending.isDone());
    Assert.assertTrue(factory.isGood(pending));

    completer.join(TimeUnit.SECONDS.toMillis(30));
  }

  @Test(timeout = 60_000L)
  public void testIsGoodRejectsAFailedFuture()
  {
    final ChannelPromise failed = channel.newPromise();
    failed.setFailure(new IllegalStateException("could not connect"));

    Assert.assertFalse(factory.isGood(failed));
  }

  @Test(timeout = 60_000L)
  public void testIsGoodRejectsAClosedChannel() throws Exception
  {
    final ChannelFuture succeeded = channel.newPromise().setSuccess();
    channel.close().await(10, TimeUnit.SECONDS);

    Assert.assertFalse(factory.isGood(succeeded));
  }
}
