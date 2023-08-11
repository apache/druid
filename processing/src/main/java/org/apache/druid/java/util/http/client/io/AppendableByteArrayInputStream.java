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

package org.apache.druid.java.util.http.client.io;

import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 */
public class AppendableByteArrayInputStream extends InputStream
{
  private static final Logger log = new Logger(AppendableByteArrayInputStream.class);

  private final Deque<byte[]> bytes = new ArrayDeque<>();
  private final SingleByteReaderDoer singleByteReaderDoer = new SingleByteReaderDoer();

  private volatile boolean done = false;
  private volatile Throwable throwable;
  private volatile int available = 0;

  private byte[] curr = new byte[]{};
  private int currIndex = 0;

  public void add(byte[] bytesToAdd)
  {
    if (bytesToAdd.length == 0) {
      return;
    }

    synchronized (singleByteReaderDoer) {
      bytes.addLast(bytesToAdd);
      available += bytesToAdd.length;
      singleByteReaderDoer.notify();
    }
  }

  public void done()
  {
    synchronized (singleByteReaderDoer) {
      done = true;
      singleByteReaderDoer.notify();
    }
  }

  public void exceptionCaught(Throwable t)
  {
    synchronized (singleByteReaderDoer) {
      done = true;
      throwable = t;
      singleByteReaderDoer.notify();
    }
  }

  @Override
  public int read() throws IOException
  {
    if (scanThroughBytesAndDoSomething(1, singleByteReaderDoer) == 0) {
      return -1;
    }
    return singleByteReaderDoer.getRetVal();
  }

  @Override
  public int read(final byte[] b, final int off, int len) throws IOException
  {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    final long retVal = scanThroughBytesAndDoSomething(
        len,
        new Doer()
        {
          int currOff = off;

          @Override
          public void doSomethingWithByteArray(int numRead)
          {
            System.arraycopy(curr, currIndex, b, currOff, numRead);
            currOff += numRead;
          }
        }
    );
    return retVal == 0 ? -1 : (int) retVal;
  }

  @Override
  public long skip(long n) throws IOException
  {
    return scanThroughBytesAndDoSomething(
        n,
        new Doer()
        {
          @Override
          public void doSomethingWithByteArray(int numToScan)
          {
          }
        }
    );
  }

  private long scanThroughBytesAndDoSomething(long numToScan, Doer doer) throws IOException
  {
    long numScanned = 0;
    long numPulled = 0;

    while (numToScan > numScanned) {
      if (currIndex >= curr.length) {
        synchronized (singleByteReaderDoer) {
          if (bytes.isEmpty()) {
            if (done) {
              break;
            }
            try {
              available -= numPulled;
              numPulled = 0;
              singleByteReaderDoer.wait();
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IOException("Interrupted!");
            }
          }

          if (throwable != null) {
            throw new IOException(throwable);
          }

          if (bytes.isEmpty()) {
            if (done) {
              break;
            } else {
              log.debug("bytes was empty, but read thread was awakened without being done.  This shouldn't happen.");
              continue;
            }
          }

          curr = bytes.removeFirst();
          currIndex = 0;
        }
      }

      final long numToPullFromCurr = Math.min(curr.length - currIndex, numToScan - numScanned);
      doer.doSomethingWithByteArray((int) numToPullFromCurr);
      numScanned += numToPullFromCurr;
      currIndex += numToPullFromCurr;
      numPulled += numToPullFromCurr;
    }

    synchronized (singleByteReaderDoer) {
      available -= numPulled;
    }

    return numScanned;
  }

  @Override
  public int available()
  {
    return available;
  }

  private interface Doer
  {
    void doSomethingWithByteArray(int numToScan);
  }

  private class SingleByteReaderDoer implements Doer
  {
    private int retVal;

    public SingleByteReaderDoer()
    {
    }

    @Override
    public void doSomethingWithByteArray(int numToScan)
    {
      retVal = curr[currIndex];
    }

    public int getRetVal()
    {
      return retVal;
    }
  }
}
