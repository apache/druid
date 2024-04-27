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

package org.apache.druid.catalog.sync;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.catalog.sync.RestUpdateSender.RestSender;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.DruidNode;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Test;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class CacheNotifierTest
{
  private static class MockSender implements Consumer<byte[]>
  {
    int sendCount;

    @Override
    public void accept(byte[] update)
    {
      assertEquals(sendCount++, update[0]);
    }
  }

  @Test
  public void testNotifier()
  {
    MockSender sender = new MockSender();
    CacheNotifier notifier = new CacheNotifier("test", sender);
    notifier.start();
    for (int i = 0; i < 100; i++) {
      byte[] msg = new byte[] {(byte) i};
      notifier.send(msg);
    }
    notifier.stopGracefully();
    assertEquals(100, sender.sendCount);
  }

  private static class MockRestSender implements RestSender
  {
    int sendCount;

    @Override
    public ListenableFuture<StatusResponseHolder> send(URL listenerURL, byte[] serializedEntity)
    {
      sendCount++;
      StatusResponseHolder holder = new StatusResponseHolder(HttpResponseStatus.ACCEPTED, new StringBuilder());
      return Futures.immediateFuture(holder);
    }
  }

  @Test
  public void testRestUpdateSender()
  {
    DruidNode node1 = new DruidNode("service", "host1", true, 1000, 0, true, false);
    DruidNode node2 = new DruidNode("service", "host2", true, 1000, 0, true, false);
    List<DruidNode> nodes = Arrays.asList(node1, node2);
    Supplier<Iterable<DruidNode>> nodeSupplier = () -> nodes;
    MockRestSender restSender = new MockRestSender();
    RestUpdateSender updateSender = new RestUpdateSender(
        "test",
        nodeSupplier,
        restSender,
        "/test/foo",
        1000);
    for (int i = 0; i < 100; i++) {
      byte[] msg = new byte[] {(byte) i};
      updateSender.accept(msg);
    }
    assertEquals(200, restSender.sendCount);
  }

  @Test
  public void testStack()
  {
    DruidNode node1 = new DruidNode("service", "host1", true, 1000, 0, true, false);
    DruidNode node2 = new DruidNode("service", "host2", true, 1000, 0, true, false);
    List<DruidNode> nodes = Arrays.asList(node1, node2);
    Supplier<Iterable<DruidNode>> nodeSupplier = () -> nodes;
    MockRestSender restSender = new MockRestSender();
    RestUpdateSender updateSender = new RestUpdateSender(
        "test",
        nodeSupplier,
        restSender,
        "/test/foo",
        1000);
    CacheNotifier notifier = new CacheNotifier("test", updateSender);
    notifier.start();
    for (int i = 0; i < 100; i++) {
      byte[] msg = new byte[] {(byte) i};
      notifier.send(msg);
    }
    notifier.stopGracefully();
    assertEquals(200, restSender.sendCount);
  }
}
