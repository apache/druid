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

package org.apache.druid.java.util.http.client;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.Timer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for {@link NettyHttpClient.TimerReadTimeoutHandler}, which drives read timeouts off a shared
 * {@link Timer} rather than the channel's event loop.
 */
public class TimerReadTimeoutHandlerTest
{
  private static final long POLL_DEADLINE_MS = 15_000L;

  /**
   * Captures the first exception that reaches the tail of the pipeline (and does not propagate it further, which would
   * otherwise cause {@link EmbeddedChannel} to log a warning).
   */
  private static class ExceptionCapturingHandler extends ChannelInboundHandlerAdapter
  {
    private final AtomicReference<Throwable> caught = new AtomicReference<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
      // Drop inbound messages; they only exist to reset the read-timeout clock.
      ReferenceCountUtil.release(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
      caught.compareAndSet(null, cause);
    }
  }

  private static Throwable awaitException(EmbeddedChannel channel, ExceptionCapturingHandler capture)
      throws InterruptedException
  {
    final long deadline = System.currentTimeMillis() + POLL_DEADLINE_MS;
    while (capture.caught.get() == null && System.currentTimeMillis() < deadline) {
      // The timeout is fired via a task submitted to the (embedded) event loop from the timer thread; run it here.
      channel.runPendingTasks();
      Thread.sleep(5L);
    }
    channel.runPendingTasks();
    return capture.caught.get();
  }

  @Test(timeout = 30_000L)
  public void testTimeoutFiresWhenAddedToActiveChannel() throws Exception
  {
    final Timer timer = new HashedWheelTimer();
    try {
      final ExceptionCapturingHandler capture = new ExceptionCapturingHandler();
      final EmbeddedChannel channel = new EmbeddedChannel(capture);

      // Adding the handler to an already-active channel exercises the handlerAdded() -> initialize() path.
      channel.pipeline().addFirst("read-timeout", new NettyHttpClient.TimerReadTimeoutHandler(timer, 50L));

      final Throwable caught = awaitException(channel, capture);
      Assert.assertTrue(
          "expected a ReadTimeoutException, got " + caught,
          caught instanceof ReadTimeoutException
      );
      channel.finishAndReleaseAll();
    }
    finally {
      timer.stop();
    }
  }

  @Test(timeout = 30_000L)
  public void testTimeoutFiresWhenChannelBecomesActive() throws Exception
  {
    final Timer timer = new HashedWheelTimer();
    try {
      final ExceptionCapturingHandler capture = new ExceptionCapturingHandler();
      // Adding the handler before the channel is active exercises handlerAdded() (inactive) + channelActive().
      final EmbeddedChannel channel = new EmbeddedChannel(
          new NettyHttpClient.TimerReadTimeoutHandler(timer, 50L),
          capture
      );

      final Throwable caught = awaitException(channel, capture);
      Assert.assertTrue(
          "expected a ReadTimeoutException, got " + caught,
          caught instanceof ReadTimeoutException
      );
      channel.finishAndReleaseAll();
    }
    finally {
      timer.stop();
    }
  }

  /**
   * The timer thread observes the deadline and then hands the actual firing to the event loop, so a read can land in
   * between. {@link EmbeddedChannel#writeInbound} reproduces exactly that order: it delivers the read first and only
   * afterwards runs the tasks queued for the loop. The queued task has to notice the read rather than failing a
   * response that has just arrived.
   */
  @Test(timeout = 30_000L)
  public void testReadArrivingBeforeQueuedTimeoutPreventsIt() throws Exception
  {
    final Timer timer = new HashedWheelTimer();
    try {
      final ExceptionCapturingHandler capture = new ExceptionCapturingHandler();
      final EmbeddedChannel channel = new EmbeddedChannel(
          new NettyHttpClient.TimerReadTimeoutHandler(timer, 50L),
          capture
      );

      // Let the timer pass the deadline and queue its task on the event loop, without running that task yet.
      Thread.sleep(500L);

      // Delivers the read, then runs the task the timer queued.
      channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{1}));
      Assert.assertNull("the read landed first, so nothing should have timed out", capture.caught.get());

      // The timeout still applies, measured from that read.
      final Throwable caught = awaitException(channel, capture);
      Assert.assertTrue(
          "expected a ReadTimeoutException, got " + caught,
          caught instanceof ReadTimeoutException
      );
      channel.finishAndReleaseAll();
    }
    finally {
      timer.stop();
    }
  }

  @Test(timeout = 30_000L)
  public void testReadReschedulesTimeoutThenFires() throws Exception
  {
    final Timer timer = new HashedWheelTimer();
    try {
      final ExceptionCapturingHandler capture = new ExceptionCapturingHandler();
      final EmbeddedChannel channel = new EmbeddedChannel(
          new NettyHttpClient.TimerReadTimeoutHandler(timer, 150L),
          capture
      );

      // Feed inbound reads so the first scheduled timeout sees recent activity and reschedules
      // (the nextDelayNanos > 0 branch) rather than firing.
      for (int i = 0; i < 6 && capture.caught.get() == null; i++) {
        channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{1}));
        channel.runPendingTasks();
        Thread.sleep(60L);
      }

      // Now stop reading; the (rescheduled) timeout should eventually fire.
      final Throwable caught = awaitException(channel, capture);
      Assert.assertTrue(
          "expected a ReadTimeoutException, got " + caught,
          caught instanceof ReadTimeoutException
      );
      channel.finishAndReleaseAll();
    }
    finally {
      timer.stop();
    }
  }

  @Test(timeout = 30_000L)
  public void testHandlerRemovalCancelsTimeout() throws Exception
  {
    final Timer timer = new HashedWheelTimer();
    try {
      final ExceptionCapturingHandler capture = new ExceptionCapturingHandler();
      final EmbeddedChannel channel = new EmbeddedChannel(
          new NettyHttpClient.TimerReadTimeoutHandler(timer, 50L),
          capture
      );

      // Removing the handler must cancel the pending timeout (destroy()); nothing should fire afterwards.
      channel.pipeline().remove(NettyHttpClient.TimerReadTimeoutHandler.class);

      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(300L));
      channel.runPendingTasks();

      Assert.assertNull("no timeout should fire after the handler is removed", capture.caught.get());
      channel.finishAndReleaseAll();
    }
    finally {
      timer.stop();
    }
  }

  @Test(timeout = 30_000L)
  public void testChannelInactiveCancelsTimeout() throws Exception
  {
    final Timer timer = new HashedWheelTimer();
    try {
      final ExceptionCapturingHandler capture = new ExceptionCapturingHandler();
      final EmbeddedChannel channel = new EmbeddedChannel(
          new NettyHttpClient.TimerReadTimeoutHandler(timer, 50L),
          capture
      );

      // Closing the channel drives channelInactive() -> destroy(); nothing should fire afterwards.
      channel.close().sync();

      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(300L));
      channel.runPendingTasks();

      Assert.assertNull("no timeout should fire after the channel goes inactive", capture.caught.get());
    }
    finally {
      timer.stop();
    }
  }
}
