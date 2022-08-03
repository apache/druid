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

package org.apache.druid.frame.channel;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.commons.io.IOUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Channel backed by an {@link InputStream}.
 *
 * Frame channels are expected to be nonblocking, but InputStreams cannot be read in nonblocking fashion.
 * This implementation deals with that by using an {@link ExecutorService} to read from the stream in a
 * separate thread.
 */
public class ReadableInputStreamFrameChannel implements ReadableFrameChannel
{
  private final InputStream inputStream;
  private final ReadableByteChunksFrameChannel delegate;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private final byte[] buffer = new byte[8 * 1024];

  @GuardedBy("lock")
  private long totalInputStreamBytesRead = 0;

  @GuardedBy("lock")
  private boolean inputStreamFinished = false;

  @GuardedBy("lock")
  private boolean inputStreamError = false;

  private volatile boolean readingStarted = false;
  private volatile boolean keepReading = true;

  private ExecutorService executorService;

  /**
   * Parameter for manipulating retry sleep duration
   */
  private static final int BASE_SLEEP_MILLIS = 100;

  /**
   * Parameter for manipulating retry sleep duration.
   */
  private static final int MAX_SLEEP_MILLIS = 2000;


  public ReadableInputStreamFrameChannel(InputStream inputStream, String id, ExecutorService executorService)
  {
    this.inputStream = inputStream;
    this.delegate = ReadableByteChunksFrameChannel.create(id);
    this.executorService = executorService;
  }

  /**
   * Method needs to be called for reading of input streams into ByteChunksFrameChannel
   */
  public void startReading()
  {
    // submit the reading task to the executor service only once
    if (readingStarted) {
      return;
    } else {
      synchronized (lock) {
        if (readingStarted) {
          return;
        }
        readingStarted = true;
      }
      executorService.submit(() -> {
        int nTry = 1;
        while (true) {
          if (!keepReading) {
            try {
              Thread.sleep(nextRetrySleepMillis(nTry));
              synchronized (lock) {
                if (inputStreamFinished || inputStreamError || delegate.isErrorOrFinished()) {
                  return;
                }
              }
              ++nTry;
            }
            catch (InterruptedException e) {
              // close inputstream anyway if the thread interrups
              IOUtils.closeQuietly(inputStream);
              throw new ISE(e, Thread.currentThread().getName() + "interrupted");
            }

          } else {
            synchronized (lock) {
              nTry = 1; // Reset the value of try because we are not waiting on the data from the inputStream
              // if done reading method is called we should not read input stream further
              if (inputStreamFinished) {
                delegate.doneWriting();
                break;
              }
              try {

                int bytesRead = inputStream.read(buffer);
                if (bytesRead == -1) {
                  inputStreamFinished = true;
                  delegate.doneWriting();
                  break;
                } else {
                  ListenableFuture<?> backpressureFuture = delegate.addChunk(Arrays.copyOfRange(buffer, 0, bytesRead));
                  totalInputStreamBytesRead += bytesRead;
                  if (backpressureFuture != null) {
                    keepReading = false;
                    backpressureFuture.addListener(() -> keepReading = true, Execs.directExecutor());
                  } else {
                    keepReading = true;
                    // continue adding data to delegate
                    // give up lock so that other threads have a change to do some work
                  }
                }
              }
              catch (Exception e) {
                //handle exception
                long currentStreamOffset = totalInputStreamBytesRead;
                delegate.setError(new ISE(e,
                                          "Found error while reading input stream at %d", currentStreamOffset
                ));
                inputStreamError = true;
                // close the stream in case done reading is not called.
                IOUtils.closeQuietly(inputStream);
                break;
              }
            }
          }
        }
      });
    }
  }

  @Override
  public boolean isFinished()
  {
    synchronized (lock) {
      return delegate.isFinished();
    }
  }

  @Override
  public boolean canRead()
  {
    synchronized (lock) {
      if (!readingStarted) {
        throw new ISE("Please call startReading method before calling canRead()");
      }
      return delegate.canRead();
    }
  }

  @Override
  public Frame read()
  {
    synchronized (lock) {
      if (!readingStarted) {
        throw new ISE("Please call startReading method before calling read");
      }
      return delegate.read();
    }
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    synchronized (lock) {
      if (!readingStarted) {
        throw new ISE("Please call startReading method before calling readabilityFuture()");
      }
      return delegate.readabilityFuture();
    }
  }


  @Override
  public void close()
  {
    synchronized (lock) {
      inputStreamFinished = true;
      delegate.close();
      IOUtils.closeQuietly(inputStream);
    }
  }

  /**
   * Function to implement exponential backoff. The calculations are similar to the function that is being used in
   * {@link org.apache.druid.java.util.common.RetryUtils} but with a different MAX_SLEEP_MILLIS and BASE_SLEEP_MILLIS
   */
  private static long nextRetrySleepMillis(final int nTry)
  {
    final double fuzzyMultiplier = Math.min(Math.max(1 + 0.2 * ThreadLocalRandom.current().nextGaussian(), 0), 2);
    final long sleepMillis = (long) (Math.min(MAX_SLEEP_MILLIS, BASE_SLEEP_MILLIS * Math.pow(2, nTry - 1))
                                     * fuzzyMultiplier);
    return sleepMillis;
  }

}
