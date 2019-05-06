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

import org.apache.druid.java.util.common.ISE;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class ResourcePoolTest
{
  ResourceFactory<String, String> resourceFactory;
  ResourcePool<String, String> pool;

  @Before
  public void setUp()
  {
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);

    EasyMock.replay(resourceFactory);
    pool = new ResourcePool<String, String>(
        resourceFactory,
        new ResourcePoolConfig(2, TimeUnit.MINUTES.toMillis(4))
    );

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testSanity()
  {
    primePool();
    EasyMock.replay(resourceFactory);
  }

  private void primePool()
  {
    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.generate("sally")).andAnswer(new StringIncrementingAnswer("sally")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("sally0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    ResourceContainer<String> sallyString = pool.take("sally");
    Assert.assertEquals("billy0", billyString.get());
    Assert.assertEquals("sally0", sallyString.get());

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    billyString.returnResource();
    sallyString.returnResource();
  }

  @Test
  public void testFailedResource()
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(false).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy2", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testFaultyFailedResourceReplacement()
  {
    primePool();

    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(false).times(1);
    resourceFactory.close("billy1");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new ISE("where's billy?")).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(false).times(1);
    resourceFactory.close("billy0");
    EasyMock.expectLastCall();
    EasyMock.expect(resourceFactory.generate("billy")).andThrow(new ISE("where's billy?")).times(1);
    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy2").times(1);
    EasyMock.replay(resourceFactory);

    IllegalStateException e1 = null;
    try {
      pool.take("billy");
    }
    catch (IllegalStateException e) {
      e1 = e;
    }
    Assert.assertNotNull("exception", e1);
    Assert.assertEquals("where's billy?", e1.getMessage());

    IllegalStateException e2 = null;
    try {
      pool.take("billy");
    }
    catch (IllegalStateException e) {
      e2 = e;
    }
    Assert.assertNotNull("exception", e2);
    Assert.assertEquals("where's billy?", e2.getMessage());

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy2", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  @Test
  public void testTakeMoreThanAllowed() throws Exception
  {
    primePool();
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    MyThread billy1Thread = new MyThread(latch1, "billy");
    billy1Thread.start();
    billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    MyThread billy0Thread = new MyThread(latch2, "billy");
    billy0Thread.start();
    billy0Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    MyThread blockedThread = new MyThread(latch3, "billy");
    blockedThread.start();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    latch2.countDown();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    latch1.countDown();
    latch3.countDown();

    Assert.assertEquals("billy1", billy1Thread.getValue());
    Assert.assertEquals("billy0", billy0Thread.getValue());
    Assert.assertEquals("billy0", blockedThread.getValue());
  }

  @Test
  public void testCloseUnblocks() throws InterruptedException
  {
    primePool();
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    resourceFactory.close("sally1");
    EasyMock.expectLastCall().times(1);
    resourceFactory.close("sally0");
    EasyMock.expectLastCall().times(1);
    EasyMock.replay(resourceFactory);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    MyThread billy1Thread = new MyThread(latch1, "billy");
    billy1Thread.start();
    billy1Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    MyThread billy0Thread = new MyThread(latch2, "billy");
    billy0Thread.start();
    billy0Thread.waitForValueToBeGotten(1, TimeUnit.SECONDS);

    MyThread blockedThread = new MyThread(latch3, "billy");
    blockedThread.start();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    pool.close();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
    // billy0Thread calling ResourceContainer.returnResource() will result
    // in a call to resourceFactory.close() when latch2 is triggered
    resourceFactory.close("billy0");
    EasyMock.expectLastCall().once();
    EasyMock.replay(resourceFactory);

    latch2.countDown();
    blockedThread.waitForValueToBeGotten(1, TimeUnit.SECONDS);
    // wait for billy0Thread to have called resourceFactory.close() to avoid race
    // between billy0Thread calling it and verify() checking for the call
    billy0Thread.join();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    latch1.countDown();
    latch3.countDown();

    Assert.assertEquals("billy1", billy1Thread.getValue());
    Assert.assertEquals("billy0", billy0Thread.getValue());
    blockedThread.join();
    // pool returns null after close
    Assert.assertEquals(null, blockedThread.getValue());
  }

  @Test
  public void testTimedOutResource() throws Exception
  {
    resourceFactory = (ResourceFactory<String, String>) EasyMock.createMock(ResourceFactory.class);

    pool = new ResourcePool<String, String>(
        resourceFactory,
        new ResourcePoolConfig(2, TimeUnit.MILLISECONDS.toMillis(10))
    );

    EasyMock.expect(resourceFactory.generate("billy")).andAnswer(new StringIncrementingAnswer("billy")).times(2);
    EasyMock.expect(resourceFactory.isGood("billy0")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billyString = pool.take("billy");
    Assert.assertEquals("billy0", billyString.get());

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);

    billyString.returnResource();

    //make sure resources have been timed out.
    Thread.sleep(100);

    EasyMock.expect(resourceFactory.generate("billy")).andReturn("billy1").times(1);
    resourceFactory.close("billy1");
    EasyMock.expect(resourceFactory.isGood("billy1")).andReturn(true).times(1);
    EasyMock.replay(resourceFactory);

    ResourceContainer<String> billy = pool.take("billy");
    Assert.assertEquals("billy1", billy.get());
    billy.returnResource();

    EasyMock.verify(resourceFactory);
    EasyMock.reset(resourceFactory);
  }

  private static class StringIncrementingAnswer implements IAnswer<String>
  {
    int count = 0;
    private String string;

    public StringIncrementingAnswer(String string)
    {
      this.string = string;
    }

    @Override
    public String answer()
    {
      return string + count++;
    }
  }

  private class MyThread extends Thread
  {
    private final CountDownLatch gotValueLatch = new CountDownLatch(1);

    private final CountDownLatch latch1;
    private String resourceName;

    volatile String value = null;

    public MyThread(CountDownLatch latch1, String resourceName)
    {
      this.latch1 = latch1;
      this.resourceName = resourceName;
    }

    @Override
    public void run()
    {
      ResourceContainer<String> resourceContainer = pool.take(resourceName);
      value = resourceContainer.get();
      gotValueLatch.countDown();
      try {
        latch1.await();
      }
      catch (InterruptedException e) {

      }
      resourceContainer.returnResource();
    }

    public String getValue()
    {
      return value;
    }

    public void waitForValueToBeGotten(long length, TimeUnit timeUnit) throws InterruptedException
    {
      gotValueLatch.await(length, timeUnit);
    }
  }
}
