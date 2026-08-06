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

package org.apache.druid.messages.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.messages.client.MessageRelay;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class OutboxImplTest
{
  private static final String HOST = "h1";

  private OutboxImpl<String> outbox;

  @BeforeEach
  public void setUp()
  {
    outbox = new OutboxImpl<>();
  }

  @AfterEach
  public void tearDown()
  {
    outbox.stop();
  }

  @Test
  public void test_normalOperation() throws InterruptedException, ExecutionException
  {
    // Send first three messages.
    final ListenableFuture<?> sendFuture1 = outbox.sendMessage(HOST, "1");
    final ListenableFuture<?> sendFuture2 = outbox.sendMessage(HOST, "2");
    final ListenableFuture<?> sendFuture3 = outbox.sendMessage(HOST, "3");

    final long outboxEpoch = outbox.getOutboxEpoch(HOST);

    // No messages are acknowledged.
    Assertions.assertFalse(sendFuture1.isDone());
    Assertions.assertFalse(sendFuture2.isDone());
    Assertions.assertFalse(sendFuture3.isDone());

    // Request all three messages (startWatermark = 0).
    Assertions.assertEquals(
        new MessageBatch<>(ImmutableList.of("1", "2", "3"), outboxEpoch, 0),
        outbox.getMessages(HOST, MessageRelay.INIT, 0).get()
    );

    // No messages are acknowledged.
    Assertions.assertFalse(sendFuture1.isDone());
    Assertions.assertFalse(sendFuture2.isDone());
    Assertions.assertFalse(sendFuture3.isDone());

    // Request two of those messages again (startWatermark = 1).
    Assertions.assertEquals(
        new MessageBatch<>(ImmutableList.of("2", "3"), outboxEpoch, 1),
        outbox.getMessages(HOST, outboxEpoch, 1).get()
    );

    // First message is acknowledged.
    Assertions.assertTrue(sendFuture1.isDone());
    Assertions.assertFalse(sendFuture2.isDone());
    Assertions.assertFalse(sendFuture3.isDone());

    // Request the high watermark (startWatermark = 3).
    final ListenableFuture<MessageBatch<String>> futureBatch = outbox.getMessages(HOST, outboxEpoch, 3);

    // It's not available yet.
    Assertions.assertFalse(futureBatch.isDone());

    // All messages are acknowledged.
    Assertions.assertTrue(sendFuture1.isDone());
    Assertions.assertTrue(sendFuture2.isDone());
    Assertions.assertTrue(sendFuture3.isDone());

    // Send one more message; futureBatch resolves.
    final ListenableFuture<?> sendFuture4 = outbox.sendMessage(HOST, "4");
    Assertions.assertTrue(futureBatch.isDone());

    // sendFuture4 is not resolved.
    Assertions.assertFalse(sendFuture4.isDone());
  }

  @Test
  public void test_getMessages_wrongEpoch() throws InterruptedException, ExecutionException
  {
    final ListenableFuture<?> sendFuture = outbox.sendMessage(HOST, "1");
    final long outboxEpoch = outbox.getOutboxEpoch(HOST);

    // Fetch with the wrong epoch.
    final MessageBatch<String> batch = outbox.getMessages(HOST, outboxEpoch + 1, 0).get();
    Assertions.assertEquals(
        new MessageBatch<>(Collections.emptyList(), outboxEpoch, 0),
        batch
    );

    Assertions.assertFalse(sendFuture.isDone());
  }

  @Test
  public void test_getMessages_nonexistentHost() throws InterruptedException, ExecutionException
  {
    // Calling getMessages with a nonexistent host creates an outbox.
    final String nonexistentHost = "nonexistent";
    final ListenableFuture<MessageBatch<String>> batchFuture = outbox.getMessages(
        nonexistentHost,
        MessageRelay.INIT,
        0
    );
    Assertions.assertFalse(batchFuture.isDone());

    // Check that an outbox was created (it has an epoch).
    assertThat(outbox.getOutboxEpoch(nonexistentHost)).isGreaterThanOrEqualTo(0L);

    // getMessages future resolves when a message is sent.
    final ListenableFuture<?> sendFuture = outbox.sendMessage(nonexistentHost, "foo");
    Assertions.assertTrue(batchFuture.isDone());
    Assertions.assertEquals(
        new MessageBatch<>(ImmutableList.of("foo"), outbox.getOutboxEpoch(nonexistentHost), 0),
        batchFuture.get()
    );

    // As usual, sendFuture resolves when the high watermark is requested.
    Assertions.assertFalse(sendFuture.isDone());
    final ListenableFuture<MessageBatch<String>> batchFuture2 =
        outbox.getMessages(nonexistentHost, outbox.getOutboxEpoch(nonexistentHost), 1);

    Assertions.assertTrue(sendFuture.isDone());

    outbox.resetOutbox(nonexistentHost);
    Assertions.assertTrue(batchFuture2.isDone());
  }

  @Test
  public void test_stop_cancelsSendMessage()
  {
    final ListenableFuture<?> sendFuture = outbox.sendMessage(HOST, "1");
    outbox.stop();
    Assertions.assertTrue(sendFuture.isCancelled());
  }

  @Test
  public void test_stop_cancelsGetMessages()
  {
    final ListenableFuture<MessageBatch<String>> futureBatch = outbox.getMessages(HOST, MessageRelay.INIT, 0);
    outbox.stop();
    Assertions.assertTrue(futureBatch.isCancelled());
  }

  @Test
  public void test_reset_cancelsSendMessage()
  {
    final ListenableFuture<?> sendFuture = outbox.sendMessage(HOST, "1");
    outbox.resetOutbox(HOST);
    Assertions.assertTrue(sendFuture.isCancelled());
  }

  @Test
  public void test_reset_cancelsGetMessages()
  {
    final ListenableFuture<MessageBatch<String>> futureBatch = outbox.getMessages(HOST, MessageRelay.INIT, 0);
    outbox.resetOutbox(HOST);
    Assertions.assertTrue(futureBatch.isCancelled());
  }

  @Test
  public void test_reset_nonexistentHost_doesNothing()
  {
    outbox.resetOutbox("nonexistent");
  }

  @Test
  public void test_stop_preventsSendMessage()
  {
    outbox.stop();
    final ListenableFuture<?> sendFuture = outbox.sendMessage(HOST, "1");
    Assertions.assertTrue(sendFuture.isCancelled());
  }

  @Test
  public void test_stop_preventsGetMessages()
  {
    outbox.stop();
    final ListenableFuture<MessageBatch<String>> futureBatch = outbox.getMessages(HOST, MessageRelay.INIT, 0);
    Assertions.assertTrue(futureBatch.isCancelled());
  }
}
