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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageRelayTest
{
  private static final String MY_HOST = "me";
  private static final DruidNode SERVER_NODE = new DruidNode("service", "host", false, 80, -1, true, false);

  @Test
  public void test_epochChange_restartsCollector()
  {
    // Server returns "a" from outbox epoch 1, then resets the outbox (new epoch 2), then returns "b" from epoch 2.
    final ScriptedClient client = new ScriptedClient();
    client.respond(MessageRelay.INIT, 0, new MessageBatch<>(List.of("a"), 1, 0));
    client.respond(1, 1, new MessageBatch<>(List.of(), 2, 0));
    client.respond(2, 0, new MessageBatch<>(List.of("b"), 2, 0));

    @SuppressWarnings("unchecked")
    final MessageListener<String> listener = Mockito.mock(MessageListener.class);
    final MessageRelay<String> relay = new MessageRelay<>(MY_HOST, SERVER_NODE, client, listener);
    relay.start();

    // Collector keeps going after the epoch change, and is now waiting on epoch 2, watermark 1.
    Assertions.assertEquals(List.of("-1:0", "1:1", "2:0", "2:1"), client.getCalls());

    final InOrder inOrder = Mockito.inOrder(listener);
    inOrder.verify(listener).serverAdded(SERVER_NODE);
    inOrder.verify(listener).messageReceived("a");
    inOrder.verify(listener).serverRemoved(SERVER_NODE);
    inOrder.verify(listener).serverAdded(SERVER_NODE);
    inOrder.verify(listener).messageReceived("b");
    inOrder.verifyNoMoreInteractions();

    relay.stop(true);
    Mockito.verify(listener, Mockito.times(2)).serverRemoved(SERVER_NODE);
  }

  /**
   * {@link MessageRelayClient} that returns scripted responses for particular epochs and watermarks, and a future that
   * never resolves otherwise.
   */
  private static class ScriptedClient implements MessageRelayClient<String>
  {
    private final Map<String, MessageBatch<String>> responses = new HashMap<>();
    private final List<String> calls = new ArrayList<>();

    void respond(final long epoch, final long watermark, final MessageBatch<String> batch)
    {
      responses.put(epoch + ":" + watermark, batch);
    }

    List<String> getCalls()
    {
      return calls;
    }

    @Override
    public ListenableFuture<MessageBatch<String>> getMessages(String clientHost, long epoch, long startWatermark)
    {
      Assertions.assertEquals(MY_HOST, clientHost);
      final String key = epoch + ":" + startWatermark;
      calls.add(key);
      final MessageBatch<String> batch = responses.get(key);
      return batch != null ? Futures.immediateFuture(batch) : SettableFuture.create();
    }
  }
}
