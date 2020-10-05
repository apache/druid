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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(EasyMockRunner.class)
public class TimedShutoffInputSourceReaderTest
{
  @Mock
  private InputSourceReader mockInputSourceReader;

  @Test
  public void testCloseWhenNoTimeoutAndIteratorCloseNormally() throws Exception
  {
    int timeout = Integer.MAX_VALUE;
    DateTime timeoutDateTime = DateTimes.nowUtc().plusMillis(timeout);
    CloseableIterator<InputRowListPlusRawValues> delegateTestIterator = createTestCloseableIterator();
    EasyMock.expect(mockInputSourceReader.sample()).andReturn(delegateTestIterator);
    EasyMock.replay(mockInputSourceReader);

    TimedShutoffInputSourceReader timeoutReader = new TimedShutoffInputSourceReader(mockInputSourceReader, timeoutDateTime);
    CloseableIterator<InputRowListPlusRawValues> timeoutIterator = timeoutReader.sample();
    // Should be True as iterator is still open
    Assert.assertTrue(timeoutIterator.hasNext());
    Assert.assertTrue(delegateTestIterator.hasNext());
    // Close manually (no timeout)
    timeoutIterator.close();
    Assert.assertFalse(timeoutIterator.hasNext());
    Assert.assertFalse(delegateTestIterator.hasNext());
  }

  @Test
  public void testCloseWhenTimeoutBeforeIteratorCloseNormally() throws Exception
  {
    int timeout = 300;
    DateTime timeoutDateTime = DateTimes.nowUtc().plusMillis(timeout);
    CloseableIterator<InputRowListPlusRawValues> delegateTestIterator = createTestCloseableIterator();
    EasyMock.expect(mockInputSourceReader.sample()).andReturn(delegateTestIterator);
    EasyMock.replay(mockInputSourceReader);

    TimedShutoffInputSourceReader timeoutReader = new TimedShutoffInputSourceReader(mockInputSourceReader, timeoutDateTime);
    CloseableIterator<InputRowListPlusRawValues> timeoutIterator = timeoutReader.sample();
    // Should be True as iterator is still open
    Assert.assertTrue(timeoutIterator.hasNext());
    Assert.assertTrue(delegateTestIterator.hasNext());
    // Close due to timeout (timeout happen automatically as we sleep for more than timeout period)
    Thread.sleep(1000);
    Assert.assertFalse(timeoutIterator.hasNext());
    Assert.assertFalse(delegateTestIterator.hasNext());
  }

  @Test
  public void testCloseConcurrentlyTimeoutFirst() throws Exception
  {
    int timeout = 300;
    DateTime timeoutDateTime = DateTimes.nowUtc().plusMillis(timeout);
    CloseableIterator<InputRowListPlusRawValues> delegateTestIterator = createTestCloseableIterator();
    EasyMock.expect(mockInputSourceReader.sample()).andReturn(delegateTestIterator);
    EasyMock.replay(mockInputSourceReader);

    TimedShutoffInputSourceReader timeoutReader = new TimedShutoffInputSourceReader(mockInputSourceReader, timeoutDateTime);
    CloseableIterator<InputRowListPlusRawValues> timeoutIterator = timeoutReader.sample();
    // Should be True as iterator is still open
    Assert.assertTrue(timeoutIterator.hasNext());
    Assert.assertTrue(delegateTestIterator.hasNext());
    // Wait for timeout
    Thread.sleep(400);
    // Close manually while timeout is already closing the iterator
    timeoutIterator.close();
    Thread.sleep(1000);
    Assert.assertFalse(timeoutIterator.hasNext());
    Assert.assertFalse(delegateTestIterator.hasNext());
  }

  @Test
  public void testCloseConcurrentlyTimeoutAfter() throws Exception
  {
    int timeout = 300;
    DateTime timeoutDateTime = DateTimes.nowUtc().plusMillis(timeout);
    CloseableIterator<InputRowListPlusRawValues> delegateTestIterator = createTestCloseableIterator();
    EasyMock.expect(mockInputSourceReader.sample()).andReturn(delegateTestIterator);
    EasyMock.replay(mockInputSourceReader);

    TimedShutoffInputSourceReader timeoutReader = new TimedShutoffInputSourceReader(mockInputSourceReader, timeoutDateTime);
    CloseableIterator<InputRowListPlusRawValues> timeoutIterator = timeoutReader.sample();
    // Should be True as iterator is still open
    Assert.assertTrue(timeoutIterator.hasNext());
    Assert.assertTrue(delegateTestIterator.hasNext());
    // Close before timeout. However, since closing takes longer than timeout period. The timeout will call close again
    // while close from this call is still ongoing.
    timeoutIterator.close();
    Thread.sleep(1000);
    Assert.assertFalse(timeoutIterator.hasNext());
    Assert.assertFalse(delegateTestIterator.hasNext());
  }

  private CloseableIterator<InputRowListPlusRawValues> createTestCloseableIterator()
  {
    return new CloseableIterator<InputRowListPlusRawValues>()
    {

      private final AtomicBoolean closedSuccessfully = new AtomicBoolean(false);

      /**
       * Always return True unless this iterator is already closed.
       * We use this method to test if the iterator is closed or not.
       */
      @Override
      public boolean hasNext()
      {
        return !closedSuccessfully.get();
      }

      /**
       * This fuctionality is not used and not needed for this testing
       */
      @Override
      public InputRowListPlusRawValues next()
      {
        return null;
      }

      @Override
      public void close() throws IOException
      {
        try {
          // Simulate time required to close the iterator
          Thread.sleep(500);
          closedSuccessfully.set(true);
        }
        catch (InterruptedException e) {
          throw new IOException();
        }

      }
    };
  }
}
