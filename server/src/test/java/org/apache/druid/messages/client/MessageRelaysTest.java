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

package org.apache.druid.messages.client;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.messages.server.OutboxImpl;
import org.apache.druid.server.DruidNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class MessageRelaysTest
{
  private static final String MY_HOST = "me";
  private static final DruidNode OUTBOX_NODE = new DruidNode("service", "host", false, 80, -1, true, false);
  private static final DiscoveryDruidNode OUTBOX_DISCO_NODE = new DiscoveryDruidNode(
      new DruidNode("service", "host", false, 80, -1, true, false),
      NodeRole.HISTORICAL,
      Collections.emptyMap()
  );

  private Outbox<String> outbox;
  private TestMessageListener messageListener;
  private TestDiscovery discovery;
  private MessageRelays<String> messageRelays;

  @Before
  public void setUp()
  {
    outbox = new OutboxImpl<>();
    messageListener = new TestMessageListener();
    discovery = new TestDiscovery();
    messageRelays = new MessageRelays<>(
        () -> discovery,
        node -> {
          Assert.assertEquals(OUTBOX_NODE, node);
          return new MessageRelay<>(
              MY_HOST,
              node,
              new OutboxMessageRelayClient(outbox),
              messageListener
          );
        }
    );
    messageRelays.start();
  }

  @After
  public void tearDown()
  {
    messageRelays.stop();
    Assert.assertEquals(Collections.emptyList(), discovery.getListeners());
  }

  @Test
  public void test_serverAdded_thenRemoved()
  {
    discovery.fire(listener -> listener.nodesAdded(Collections.singletonList(OUTBOX_DISCO_NODE)));
    discovery.fire(listener -> listener.nodesRemoved(Collections.singletonList(OUTBOX_DISCO_NODE)));
    Assert.assertEquals(1, messageListener.getAdds());
    Assert.assertEquals(1, messageListener.getRemoves());
  }

  @Test
  public void test_messageListener()
  {
    discovery.fire(listener -> listener.nodesAdded(Collections.singletonList(OUTBOX_DISCO_NODE)));
    Assert.assertEquals(1, messageListener.getAdds());
    Assert.assertEquals(0, messageListener.getRemoves());

    final ListenableFuture<?> sendFuture = outbox.sendMessage(MY_HOST, "foo");
    Assert.assertEquals(ImmutableList.of("foo"), messageListener.getMessages());
    Assert.assertTrue(sendFuture.isDone());

    final ListenableFuture<?> sendFuture2 = outbox.sendMessage(MY_HOST, "bar");
    Assert.assertEquals(ImmutableList.of("foo", "bar"), messageListener.getMessages());
    Assert.assertTrue(sendFuture2.isDone());
  }

  /**
   * Implementation of {@link MessageListener} that tracks all received messages.
   */
  private static class TestMessageListener implements MessageListener<String>
  {
    @GuardedBy("this")
    private long adds;

    @GuardedBy("this")
    private long removes;

    @GuardedBy("this")
    private final List<String> messages = new ArrayList<>();

    @Override
    public synchronized void serverAdded(DruidNode node)
    {
      adds++;
    }

    @Override
    public synchronized void messageReceived(String message)
    {
      messages.add(message);
    }

    @Override
    public synchronized void serverRemoved(DruidNode node)
    {
      removes++;
    }

    public synchronized long getAdds()
    {
      return adds;
    }

    public synchronized long getRemoves()
    {
      return removes;
    }

    public synchronized List<String> getMessages()
    {
      return ImmutableList.copyOf(messages);
    }
  }

  /**
   * Implementation of {@link MessageRelayClient} that directly uses an {@link Outbox}, rather than contacting
   * a remote outbox.
   */
  private static class OutboxMessageRelayClient implements MessageRelayClient<String>
  {
    private final Outbox<String> outbox;

    public OutboxMessageRelayClient(final Outbox<String> outbox)
    {
      this.outbox = outbox;
    }

    @Override
    public ListenableFuture<MessageBatch<String>> getMessages(String clientHost, long epoch, long startWatermark)
    {
      return outbox.getMessages(clientHost, epoch, startWatermark);
    }
  }

  /**
   * Implementation of {@link DruidNodeDiscovery} that allows firing listeners on command.
   */
  private static class TestDiscovery implements DruidNodeDiscovery
  {
    @GuardedBy("this")
    private final List<Listener> listeners;

    public TestDiscovery()
    {
      listeners = new ArrayList<>();
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
      listeners.add(listener);
    }

    @Override
    public synchronized void removeListener(Listener listener)
    {
      listeners.remove(listener);
    }

    public synchronized List<Listener> getListeners()
    {
      return ImmutableList.copyOf(listeners);
    }

    public synchronized void fire(Consumer<Listener> f)
    {
      for (final Listener listener : listeners) {
        f.accept(listener);
      }
    }
  }
}
