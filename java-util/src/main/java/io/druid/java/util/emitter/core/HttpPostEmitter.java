/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import io.druid.concurrent.ConcurrentAwaitableCounter;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.netty.handler.codec.http.HttpHeaders;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPOutputStream;

public class HttpPostEmitter implements Flushable, Closeable, Emitter
{
  private static final int MAX_EVENT_SIZE = 1023 * 1024; // Set max size slightly less than 1M to allow for metadata

  private static final int MAX_SEND_RETRIES = 3;

  /**
   * Threshold of the size of {@link #buffersToEmit} when switch from using {@link
   * BaseHttpEmittingConfig#getHttpTimeoutAllowanceFactor()} to {@link #EQUILIBRIUM_ALLOWANCE_FACTOR}
   */
  private static final int EMIT_QUEUE_THRESHOLD_1 = 5;

  /**
   * Threshold of the size of {@link #buffersToEmit} when switch from using {@link #EQUILIBRIUM_ALLOWANCE_FACTOR}
   * to {@link #TIGHT_ALLOWANCE_FACTOR}.
   */
  private static final int EMIT_QUEUE_THRESHOLD_2 = 10;

  /**
   * 0.9 is to give room for unexpected latency or time out not being respected rigorously.
   */
  private static final double EQUILIBRIUM_ALLOWANCE_FACTOR = 0.9;

  private static final double TIGHT_ALLOWANCE_FACTOR = 0.5;

  /**
   * Used in {@link EmittingThread#emitLargeEvents()} to ensure fair emitting of both large events and batched events.
   */
  private static final byte[] LARGE_EVENTS_STOP = new byte[]{};

  private static final Logger log = new Logger(HttpPostEmitter.class);
  private static final AtomicInteger instanceCounter = new AtomicInteger();

  final BatchingStrategy batchingStrategy;
  final HttpEmitterConfig config;
  private final int bufferSize;
  final int maxBufferWatermark;
  private final int largeEventThreshold;
  private final AsyncHttpClient client;
  private final ObjectMapper jsonMapper;
  private final String url;

  private final ConcurrentLinkedQueue<byte[]> buffersToReuse = new ConcurrentLinkedQueue<>();
  /**
   * "Approximate" because not exactly atomically synchronized with {@link #buffersToReuse} updates. {@link
   * ConcurrentLinkedQueue#size()} is not used, because it's O(n).
   */
  private final AtomicInteger approximateBuffersToReuseCount = new AtomicInteger();

  /**
   * concurrentBatch.get() == null means the service is closed. concurrentBatch.get() is the instance of Integer,
   * it means that some thread has failed with a serious error during {@link #onSealExclusive} (with the batch number
   * corresponding to the Integer object) and {@link #tryRecoverCurrentBatch} needs to be called. Otherwise (i. e.
   * normally), an instance of {@link Batch} is stored in this atomic reference.
   */
  private final AtomicReference<Object> concurrentBatch = new AtomicReference<>();

  private final ConcurrentLinkedDeque<Batch> buffersToEmit = new ConcurrentLinkedDeque<>();
  /**
   * See {@link #approximateBuffersToReuseCount}
   */
  private final AtomicInteger approximateBuffersToEmitCount = new AtomicInteger();
  /**
   * See {@link #approximateBuffersToReuseCount}
   */
  private final AtomicLong approximateEventsToEmitCount = new AtomicLong();

  private final ConcurrentLinkedQueue<byte[]> largeEventsToEmit = new ConcurrentLinkedQueue<>();
  /**
   * See {@link #approximateBuffersToReuseCount}
   */
  private final AtomicInteger approximateLargeEventsToEmitCount = new AtomicInteger();

  private final ConcurrentAwaitableCounter emittedBatchCounter = new ConcurrentAwaitableCounter();
  private final EmittingThread emittingThread;
  private final AtomicLong totalEmittedEvents = new AtomicLong();
  private final AtomicInteger allocatedBuffers = new AtomicInteger();
  private final AtomicInteger droppedBuffers = new AtomicInteger();

  private volatile long lastFillTimeMillis;
  private final ConcurrentTimeCounter batchFillingTimeCounter = new ConcurrentTimeCounter();

  private final Object startLock = new Object();
  private final CountDownLatch startLatch = new CountDownLatch(1);
  private boolean running = false;

  public HttpPostEmitter(HttpEmitterConfig config, AsyncHttpClient client)
  {
    this(config, client, new ObjectMapper());
  }

  public HttpPostEmitter(HttpEmitterConfig config, AsyncHttpClient client, ObjectMapper jsonMapper)
  {
    batchingStrategy = config.getBatchingStrategy();
    final int batchOverhead = batchingStrategy.batchStartLength() + batchingStrategy.batchEndLength();
    Preconditions.checkArgument(
        config.getMaxBatchSize() >= MAX_EVENT_SIZE + batchOverhead,
        StringUtils.format(
            "maxBatchSize must be greater than MAX_EVENT_SIZE[%,d] + overhead[%,d].",
            MAX_EVENT_SIZE,
            batchOverhead
        )
    );
    this.config = config;
    this.bufferSize = config.getMaxBatchSize();
    this.maxBufferWatermark = bufferSize - batchingStrategy.batchEndLength();
    // Chosen so that if event size < largeEventThreshold, at least 2 events could fit the standard buffer.
    this.largeEventThreshold = (bufferSize - batchOverhead - batchingStrategy.separatorLength()) / 2;
    this.client = client;
    this.jsonMapper = jsonMapper;
    try {
      this.url = new URL(config.getRecipientBaseUrl()).toString();
    }
    catch (MalformedURLException e) {
      throw new ISE(e, "Bad URL: %s", config.getRecipientBaseUrl());
    }
    emittingThread = new EmittingThread(config);
    long firstBatchNumber = 1;
    concurrentBatch.set(new Batch(this, acquireBuffer(), firstBatchNumber));
    // lastFillTimeMillis must not be 0, minHttpTimeoutMillis could be.
    lastFillTimeMillis = Math.max(config.minHttpTimeoutMillis, 1);
  }

  @Override
  @LifecycleStart
  public void start()
  {
    synchronized (startLock) {
      if (!running) {
        if (startLatch.getCount() == 0) {
          throw new IllegalStateException("Already started.");
        }
        running = true;
        startLatch.countDown();
        emittingThread.start();
      }
    }
  }

  private void awaitStarted()
  {
    try {
      if (!startLatch.await(1, TimeUnit.SECONDS)) {
        throw new RejectedExecutionException("Service is not started.");
      }
      if (isTerminated()) {
        throw new RejectedExecutionException("Service is closed.");
      }
    }
    catch (InterruptedException e) {
      log.debug("Interrupted waiting for start");
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private boolean isTerminated()
  {
    return concurrentBatch.get() == null;
  }

  @Override
  public void emit(Event event)
  {
    emitAndReturnBatch(event);
  }

  @VisibleForTesting
  @Nullable
  Batch emitAndReturnBatch(Event event)
  {
    awaitStarted();

    final byte[] eventBytes = eventToBytes(event);

    if (eventBytes.length > MAX_EVENT_SIZE) {
      log.error(
          "Event too large to emit (%,d > %,d): %s ...",
          eventBytes.length,
          MAX_EVENT_SIZE,
          StringUtils.fromUtf8(ByteBuffer.wrap(eventBytes), 1024)
      );
      return null;
    }

    if (eventBytes.length > largeEventThreshold) {
      writeLargeEvent(eventBytes);
      return null;
    }

    while (true) {
      Object batchObj = concurrentBatch.get();
      if (batchObj instanceof Integer) {
        tryRecoverCurrentBatch((Integer) batchObj);
        continue;
      }
      if (batchObj == null) {
        throw new RejectedExecutionException("Service is closed.");
      }
      Batch batch = (Batch) batchObj;
      if (batch.tryAddEvent(eventBytes)) {
        return batch;
      } else {
        log.debug("Failed to emit an event in batch [%s]", batch);
      }
      // Spin loop, until the thread calling onSealExclusive() updates the concurrentBatch. This update becomes visible
      // eventually, because concurrentBatch.get() is a volatile read.
    }
  }

  private byte[] eventToBytes(Event event)
  {
    try {
      return jsonMapper.writeValueAsBytes(event);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void writeLargeEvent(byte[] eventBytes)
  {
    // It's better to drop the oldest, not latest event, but dropping the oldest is not easy to implement, because
    // LARGE_EVENTS_STOP could be added into the queue concurrently. So just not adding the latest event.
    // >, not >=, because largeEventsToEmit could contain LARGE_EVENTS_STOP
    if (approximateBuffersToEmitCount.get() > config.getBatchQueueSizeLimit()) {
      log.error(
          "largeEventsToEmit queue size reached the limit [%d], dropping the latest large event",
          config.getBatchQueueSizeLimit()
      );
    } else {
      largeEventsToEmit.add(eventBytes);
      approximateBuffersToEmitCount.incrementAndGet();
      approximateLargeEventsToEmitCount.incrementAndGet();
      approximateEventsToEmitCount.incrementAndGet();
    }
    wakeUpEmittingThread();
  }

  /**
   * Called from {@link Batch} only once for each Batch in existence.
   */
  void onSealExclusive(Batch batch, long elapsedTimeMillis)
  {
    try {
      doOnSealExclusive(batch, elapsedTimeMillis);
    }
    catch (Throwable t) {
      try {
        if (!concurrentBatch.compareAndSet(batch, batch.batchNumber)) {
          log.error("Unexpected failure to set currentBatch to the failed Batch.batchNumber");
        }
        log.error(t, "Serious error during onSealExclusive(), set currentBatch to the failed Batch.batchNumber");
      }
      catch (Throwable t2) {
        t.addSuppressed(t2);
      }
      throw t;
    }
  }

  private void doOnSealExclusive(Batch batch, long elapsedTimeMillis)
  {
    batchFillingTimeCounter.add((int) Math.max(elapsedTimeMillis, 0));
    if (elapsedTimeMillis > 0) {
      // If elapsedTimeMillis is 0 or negative, it's likely because System.currentTimeMillis() is not monotonic, so not
      // accounting this time for determining batch sending timeout.
      lastFillTimeMillis = elapsedTimeMillis;
    }
    addBatchToEmitQueue(batch);
    wakeUpEmittingThread();
    if (!isTerminated()) {
      long nextBatchNumber = ConcurrentAwaitableCounter.nextCount(batch.batchNumber);
      byte[] newBuffer = acquireBuffer();
      if (!concurrentBatch.compareAndSet(batch, new Batch(this, newBuffer, nextBatchNumber))) {
        buffersToReuse.add(newBuffer);
        // If compareAndSet failed, the service should be closed concurrently, i. e. we expect isTerminated() = true.
        // If we don't see this, there should be some bug in HttpPostEmitter.
        Preconditions.checkState(isTerminated());
      }
    }
  }

  private void tryRecoverCurrentBatch(Integer failedBatchNumber)
  {
    log.info("Trying to recover currentBatch");
    long nextBatchNumber = ConcurrentAwaitableCounter.nextCount(failedBatchNumber);
    byte[] newBuffer = acquireBuffer();
    if (concurrentBatch.compareAndSet(failedBatchNumber, new Batch(this, newBuffer, nextBatchNumber))) {
      log.info("Successfully recovered currentBatch");
    } else {
      // It's normal, a concurrent thread could succeed to recover first.
      buffersToReuse.add(newBuffer);
    }
  }

  private void addBatchToEmitQueue(Batch batch)
  {
    limitBuffersToEmitSize();
    buffersToEmit.addLast(batch);
    approximateBuffersToEmitCount.incrementAndGet();
    approximateEventsToEmitCount.addAndGet(batch.eventCount.get());
  }

  private void limitBuffersToEmitSize()
  {
    if (approximateBuffersToEmitCount.get() >= config.getBatchQueueSizeLimit()) {
      Batch droppedBatch = buffersToEmit.pollFirst();
      if (droppedBatch != null) {
        batchFinalized();
        approximateBuffersToEmitCount.decrementAndGet();
        approximateEventsToEmitCount.addAndGet(-droppedBatch.eventCount.get());
        droppedBuffers.incrementAndGet();
        log.error(
            "buffersToEmit queue size reached the limit [%d], dropping the oldest buffer to emit",
            config.getBatchQueueSizeLimit()
        );
      }
    }
  }

  private void batchFinalized()
  {
    // Notify HttpPostEmitter.flush(), that the batch is emitted, or failed, or dropped.
    emittedBatchCounter.increment();
  }

  private Batch pollBatchFromEmitQueue()
  {
    Batch result = buffersToEmit.pollFirst();
    if (result == null) {
      return null;
    }
    approximateBuffersToEmitCount.decrementAndGet();
    approximateEventsToEmitCount.addAndGet(-result.eventCount.get());
    return result;
  }

  private void wakeUpEmittingThread()
  {
    LockSupport.unpark(emittingThread);
  }

  @Override
  public void flush() throws IOException
  {
    awaitStarted();
    Object batchObj = concurrentBatch.get();
    if (batchObj instanceof Batch) {
      flush((Batch) batchObj);
    }
  }

  private void flush(Batch batch) throws IOException
  {
    if (batch == null) {
      return;
    }
    batch.seal();
    try {
      // This check doesn't always awaits for this exact batch to be emitted, because another batch could be dropped
      // from the queue ahead of this one, in limitBuffersToEmitSize(). But there is no better way currently to wait for
      // the exact batch, and it's not that important.
      emittedBatchCounter.awaitCount(batch.batchNumber, config.getFlushTimeOut(), TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException e) {
      String message = StringUtils.format("Timed out after [%d] millis during flushing", config.getFlushTimeOut());
      throw new IOException(message, e);
    }
    catch (InterruptedException e) {
      log.debug("Thread Interrupted");
      Thread.currentThread().interrupt();
      throw new IOException("Thread Interrupted while flushing", e);
    }
  }

  @Override
  @LifecycleStop
  public void close() throws IOException
  {
    synchronized (startLock) {
      if (running) {
        running = false;
        Object lastBatch = concurrentBatch.getAndSet(null);
        if (lastBatch instanceof Batch) {
          flush((Batch) lastBatch);
        }
        emittingThread.shuttingDown = true;
        // EmittingThread is interrupted after the last batch is flushed.
        emittingThread.interrupt();
      }
    }
  }

  @Override
  public String toString()
  {
    return "HttpPostEmitter{" +
           "config=" + config +
           '}';
  }

  private class EmittingThread extends Thread
  {
    private final ArrayDeque<FailedBuffer> failedBuffers = new ArrayDeque<>();
    /**
     * "Approximate", because not exactly synchronized with {@link #failedBuffers} updates. Not using size() on
     * {@link #failedBuffers}, because access to it is not synchronized, while approximateFailedBuffersCount is queried
     * not within EmittingThread.
     */
    private final AtomicInteger approximateFailedBuffersCount = new AtomicInteger();

    private final ConcurrentTimeCounter successfulSendingTimeCounter = new ConcurrentTimeCounter();
    private final ConcurrentTimeCounter failedSendingTimeCounter = new ConcurrentTimeCounter();

    /**
     * Cache the exception. Need an exception because {@link RetryUtils} operates only via exceptions.
     */
    private final TimeoutException timeoutLessThanMinimumException;

    private boolean shuttingDown = false;
    private ZeroCopyByteArrayOutputStream gzipBaos;

    EmittingThread(HttpEmitterConfig config)
    {
      super("HttpPostEmitter-" + instanceCounter.incrementAndGet());
      setDaemon(true);
      timeoutLessThanMinimumException = new TimeoutException(
          "Timeout less than minimum [" + config.getMinHttpTimeoutMillis() + "] ms."
      );
      // To not showing and writing nonsense and misleading stack trace in logs.
      timeoutLessThanMinimumException.setStackTrace(new StackTraceElement[]{});
    }

    @Override
    public void run()
    {
      while (true) {
        boolean needsToShutdown = needsToShutdown();
        try {
          emitLargeEvents();
          emitBatches();
          tryEmitOneFailedBuffer();

          if (needsToShutdown) {
            tryEmitAndDrainAllFailedBuffers();
            // Make GC life easier
            drainBuffersToReuse();
            return;
          }
        }
        catch (Throwable t) {
          log.error(t, "Uncaught exception in EmittingThread.run()");
        }
        if (failedBuffers.isEmpty()) {
          // Waiting for 1/2 of config.getFlushMillis() in order to flush events not more than 50% later than specified.
          // If nanos=0 parkNanos() doesn't wait at all, then we don't want.
          long waitNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(config.getFlushMillis()) / 2, 1);
          LockSupport.parkNanos(HttpPostEmitter.this, waitNanos);
        }
      }
    }

    private boolean needsToShutdown()
    {
      boolean needsToShutdown = Thread.interrupted() || shuttingDown;
      if (needsToShutdown) {
        Object lastBatch = concurrentBatch.getAndSet(null);
        if (lastBatch instanceof Batch) {
          ((Batch) lastBatch).seal();
        }
      } else {
        Object batch = concurrentBatch.get();
        if (batch instanceof Batch) {
          ((Batch) batch).sealIfFlushNeeded();
        } else {
          // batch == null means that HttpPostEmitter is terminated. Batch object could also be Integer, if some
          // thread just failed with a serious error in onSealExclusive(), in this case we don't want to shutdown
          // the emitter thread.
          needsToShutdown = batch == null;
        }
      }
      return needsToShutdown;
    }

    private void emitBatches()
    {
      for (Batch batch; (batch = pollBatchFromEmitQueue()) != null; ) {
        emit(batch);
      }
    }

    private void emit(final Batch batch)
    {
      // Awaits until all concurrent event writers finish copy their event bytes to the buffer. This call provides
      // memory visibility guarantees.
      batch.awaitEmittingAllowed();
      try {
        final int bufferWatermark = batch.getSealedBufferWatermark();
        if (bufferWatermark == 0) { // sealed while empty
          return;
        }
        int eventCount = batch.eventCount.get();
        log.debug(
            "Sending batch #%d to url[%s], event count[%d], bytes[%d]",
            batch.batchNumber,
            url,
            eventCount,
            bufferWatermark
        );
        int bufferEndOffset = batchingStrategy.writeBatchEnd(batch.buffer, bufferWatermark);

        if (sendWithRetries(batch.buffer, bufferEndOffset, eventCount, true)) {
          buffersToReuse.add(batch.buffer);
          approximateBuffersToReuseCount.incrementAndGet();
        } else {
          limitFailedBuffersSize();
          failedBuffers.addLast(new FailedBuffer(batch.buffer, bufferEndOffset, eventCount));
          approximateFailedBuffersCount.incrementAndGet();
        }
      }
      finally {
        batchFinalized();
      }
    }

    private void limitFailedBuffersSize()
    {
      if (failedBuffers.size() >= config.getBatchQueueSizeLimit()) {
        failedBuffers.removeFirst();
        approximateFailedBuffersCount.decrementAndGet();
        droppedBuffers.incrementAndGet();
        log.error(
            "failedBuffers queue size reached the limit [%d], dropping the oldest failed buffer",
            config.getBatchQueueSizeLimit()
        );
      }
    }

    @SuppressWarnings("ArrayEquality")
    private void emitLargeEvents()
    {
      if (largeEventsToEmit.isEmpty()) {
        return;
      }
      // Don't try to emit large events until exhaustion, to avoid starvation of "normal" batches, if large event
      // posting rate is too high, though it should never happen in practice.
      largeEventsToEmit.add(LARGE_EVENTS_STOP);
      for (byte[] largeEvent; (largeEvent = largeEventsToEmit.poll()) != LARGE_EVENTS_STOP; ) {
        emitLargeEvent(largeEvent);
        approximateBuffersToEmitCount.decrementAndGet();
        approximateLargeEventsToEmitCount.decrementAndGet();
        approximateEventsToEmitCount.decrementAndGet();
      }
    }

    private void emitLargeEvent(byte[] eventBytes)
    {
      byte[] buffer = acquireBuffer();
      int bufferOffset = batchingStrategy.writeBatchStart(buffer);
      System.arraycopy(eventBytes, 0, buffer, bufferOffset, eventBytes.length);
      bufferOffset += eventBytes.length;
      bufferOffset = batchingStrategy.writeBatchEnd(buffer, bufferOffset);
      if (sendWithRetries(buffer, bufferOffset, 1, true)) {
        buffersToReuse.add(buffer);
        approximateBuffersToReuseCount.incrementAndGet();
      } else {
        limitFailedBuffersSize();
        failedBuffers.addLast(new FailedBuffer(buffer, bufferOffset, 1));
        approximateFailedBuffersCount.incrementAndGet();
      }
    }

    private void tryEmitOneFailedBuffer()
    {
      FailedBuffer failedBuffer = failedBuffers.peekFirst();
      if (failedBuffer != null) {
        if (sendWithRetries(failedBuffer.buffer, failedBuffer.length, failedBuffer.eventCount, false)) {
          // Remove from the queue of failed buffer.
          failedBuffers.pollFirst();
          approximateFailedBuffersCount.decrementAndGet();
          // Don't add the failed buffer back to the buffersToReuse queue here, because in a situation when we were not
          // able to emit events for a while we don't have a way to discard buffers that were used to accumulate events
          // during that period, if they are added back to buffersToReuse. For instance it may result in having 100
          // buffers in rotation even if we need just 2.
        }
      }
    }

    private void tryEmitAndDrainAllFailedBuffers()
    {
      for (FailedBuffer failedBuffer; (failedBuffer = failedBuffers.pollFirst()) != null; ) {
        sendWithRetries(failedBuffer.buffer, failedBuffer.length, failedBuffer.eventCount, false);
        approximateFailedBuffersCount.decrementAndGet();
      }
    }

    /**
     * Returns true if sent successfully.
     */
    private boolean sendWithRetries(final byte[] buffer, final int length, final int eventCount, boolean withTimeout)
    {
      long deadLineMillis = System.currentTimeMillis() + sendRequestTimeoutMillis(lastFillTimeMillis);
      try {
        RetryUtils.retry(
            new RetryUtils.Task<Object>()
            {
              @Override
              public Void perform() throws Exception
              {
                send(buffer, length);
                return null;
              }
            },
            new Predicate<Throwable>()
            {
              @Override
              public boolean apply(Throwable e)
              {
                if (withTimeout && deadLineMillis - System.currentTimeMillis() <= 0) { // overflow-aware
                  return false;
                }
                if (e == timeoutLessThanMinimumException) {
                  return false; // Doesn't make sense to retry, because the result will be the same.
                }
                return !(e instanceof InterruptedException);
              }
            },
            MAX_SEND_RETRIES
        );
        totalEmittedEvents.addAndGet(eventCount);
        return true;
      }
      catch (InterruptedException e) {
        return false;
      }
      catch (Exception e) {
        if (e == timeoutLessThanMinimumException) {
          log.debug(e, "Failed to send events to url[%s] with timeout less than minimum", config.getRecipientBaseUrl());
        } else {
          log.error(e, "Failed to send events to url[%s]", config.getRecipientBaseUrl());
        }
        return false;
      }
    }

    private void send(byte[] buffer, int length) throws Exception
    {
      long lastFillTimeMillis = HttpPostEmitter.this.lastFillTimeMillis;
      final long timeoutMillis = sendRequestTimeoutMillis(lastFillTimeMillis);
      if (timeoutMillis < config.getMinHttpTimeoutMillis()) {
        throw timeoutLessThanMinimumException;
      }
      long sendingStartMs = System.currentTimeMillis();

      final RequestBuilder request = new RequestBuilder("POST");
      request.setUrl(url);
      byte[] payload;
      int payloadLength;
      ContentEncoding contentEncoding = config.getContentEncoding();
      if (contentEncoding != null) {
        switch (contentEncoding) {
          case GZIP:
            try (GZIPOutputStream gzipOutputStream = acquireGzipOutputStream(length)) {
              gzipOutputStream.write(buffer, 0, length);
            }
            payload = gzipBaos.getBuffer();
            payloadLength = gzipBaos.size();
            request.setHeader(HttpHeaders.Names.CONTENT_ENCODING, HttpHeaders.Values.GZIP);
            break;
          default:
            throw new ISE("Unsupported content encoding [%s]", contentEncoding.name());
        }
      } else {
        payload = buffer;
        payloadLength = length;
      }


      request.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json");
      request.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(payloadLength));
      request.setBody(ByteBuffer.wrap(payload, 0, payloadLength));

      if (config.getBasicAuthentication() != null) {
        final String[] parts = config.getBasicAuthentication().split(":", 2);
        final String user = parts[0];
        final String password = parts.length > 1 ? parts[1] : "";
        String encoded = Base64.getEncoder().encodeToString((user + ':' + password).getBytes(StandardCharsets.UTF_8));
        request.setHeader(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded);
      }

      request.setRequestTimeout(Ints.saturatedCast(timeoutMillis));

      ListenableFuture<Response> future = client.executeRequest(request);
      Response response;
      try {
        // Don't use Future.get(timeout), because we want to avoid sending the same data twice, in case the send
        // succeeds finally, but after the timeout.
        response = future.get();
      }
      catch (ExecutionException e) {
        accountFailedSending(sendingStartMs);
        if (e.getCause() instanceof TimeoutException) {
          log.error(
              "Timing out emitter batch send, last batch fill time [%,d] ms, timeout [%,d] ms",
              lastFillTimeMillis,
              timeoutMillis
          );
        }
        throw e;
      }

      if (response.getStatusCode() == 413) {
        accountFailedSending(sendingStartMs);
        throw new ISE(
            "Received HTTP status 413 from [%s]. Batch size of [%d] may be too large, "
            + "try adjusting maxBatchSizeBatch property",
            config.getRecipientBaseUrl(),
            config.getMaxBatchSize()
        );
      }

      if (response.getStatusCode() / 100 != 2) {
        accountFailedSending(sendingStartMs);
        throw new ISE(
            "Emissions of events not successful[%d: %s], with message[%s].",
            response.getStatusCode(),
            response.getStatusText(),
            response.getResponseBody(StandardCharsets.UTF_8).trim()
        );
      }

      accountSuccessfulSending(sendingStartMs);
    }

    private long sendRequestTimeoutMillis(long lastFillTimeMillis)
    {
      int emitQueueSize = approximateBuffersToEmitCount.get();
      if (emitQueueSize < EMIT_QUEUE_THRESHOLD_1) {
        return (long) (lastFillTimeMillis * config.httpTimeoutAllowanceFactor);
      }
      if (emitQueueSize < EMIT_QUEUE_THRESHOLD_2) {
        // The idea is to not let buffersToEmit queue to grow faster than we can emit buffers.
        return (long) (lastFillTimeMillis * EQUILIBRIUM_ALLOWANCE_FACTOR);
      }
      // If buffersToEmit still grows, try to restrict even more
      return (long) (lastFillTimeMillis * TIGHT_ALLOWANCE_FACTOR);
    }

    private void accountSuccessfulSending(long sendingStartMs)
    {
      successfulSendingTimeCounter.add((int) Math.max(System.currentTimeMillis() - sendingStartMs, 0));
    }

    private void accountFailedSending(long sendingStartMs)
    {
      failedSendingTimeCounter.add((int) Math.max(System.currentTimeMillis() - sendingStartMs, 0));
    }

    GZIPOutputStream acquireGzipOutputStream(int length) throws IOException
    {
      if (gzipBaos == null) {
        gzipBaos = new ZeroCopyByteArrayOutputStream(length);
      } else {
        gzipBaos.reset();
      }
      return new GZIPOutputStream(gzipBaos, true);
    }
  }

  private static class FailedBuffer
  {
    final byte[] buffer;
    final int length;
    final int eventCount;

    private FailedBuffer(byte[] buffer, int length, int eventCount)
    {
      this.buffer = buffer;
      this.length = length;
      this.eventCount = eventCount;
    }
  }

  private byte[] acquireBuffer()
  {
    byte[] buffer = buffersToReuse.poll();
    if (buffer == null) {
      buffer = new byte[bufferSize];
      allocatedBuffers.incrementAndGet();
    } else {
      approximateBuffersToReuseCount.decrementAndGet();
    }
    return buffer;
  }

  private void drainBuffersToReuse()
  {
    while (buffersToReuse.poll() != null) {
      approximateBuffersToReuseCount.decrementAndGet();
    }
  }

  /**
   * This and the following methods are public for external monitoring purposes.
   */
  public int getTotalAllocatedBuffers()
  {
    return allocatedBuffers.get();
  }

  public int getBuffersToEmit()
  {
    return approximateBuffersToEmitCount.get();
  }

  public int getBuffersToReuse()
  {
    return approximateBuffersToReuseCount.get();
  }

  public int getFailedBuffers()
  {
    return emittingThread.approximateFailedBuffersCount.get();
  }

  public int getDroppedBuffers()
  {
    return droppedBuffers.get();
  }

  public long getTotalEmittedEvents()
  {
    return totalEmittedEvents.get();
  }

  public long getEventsToEmit()
  {
    return approximateEventsToEmitCount.get();
  }

  public long getLargeEventsToEmit()
  {
    return approximateLargeEventsToEmitCount.get();
  }

  public ConcurrentTimeCounter getBatchFillingTimeCounter()
  {
    return batchFillingTimeCounter;
  }

  public ConcurrentTimeCounter getSuccessfulSendingTimeCounter()
  {
    return emittingThread.successfulSendingTimeCounter;
  }

  public ConcurrentTimeCounter getFailedSendingTimeCounter()
  {
    return emittingThread.successfulSendingTimeCounter;
  }

  @VisibleForTesting
  void waitForEmission(int batchNumber) throws Exception
  {
    emittedBatchCounter.awaitCount(batchNumber, 10, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  void joinEmitterThread() throws InterruptedException
  {
    emittingThread.join();
  }
}
