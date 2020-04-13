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

package org.apache.druid.collections;

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.ISE;
import org.easymock.EasyMock;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StupidPoolTest
{
  private Supplier<String> generator;
  private CloseableStupidPool<String> poolOfString;
  private ResourceHolder<String> resourceHolderObj;
  private String defaultString = new String("test");

  @Before
  public void setUp()
  {
    generator = EasyMock.createMock(Supplier.class);
    EasyMock.expect(generator.get()).andReturn(defaultString).anyTimes();
    EasyMock.replay(generator);
    poolOfString = new CloseableStupidPool<>("poolOfString", generator);
    resourceHolderObj = poolOfString.take();
  }

  @After
  public void tearDown()
  {
    if (resourceHolderObj != null) {
      resourceHolderObj.close();
    }
    poolOfString.close();
  }

  @Test
  public void testTake()
  {
    Assert.assertThat(resourceHolderObj, new IsInstanceOf(ResourceHolder.class));
    Object expectedObject = resourceHolderObj.get();
    Assert.assertEquals(expectedObject, defaultString);
  }

  @Test(expected = ISE.class)
  public void testExceptionInResourceHolderGet()
  {
    resourceHolderObj.close();
    resourceHolderObj.get();
  }

  @Test(timeout = 60_000L)
  public void testResourceHandlerClearedByJVM() throws InterruptedException
  {
    String leakedString = createDanglingObjectHandler();
    // Wait until dangling object string is returned to the pool
    for (int i = 0; i < 6000 && poolOfString.leakedObjectsCount() == 0; i++) {
      System.gc();
      @SuppressWarnings("unused")
      byte[] garbage = new byte[10_000_000];
      Thread.sleep(10);
    }
    Assert.assertEquals(leakedString, 1, poolOfString.leakedObjectsCount());
  }

  private String createDanglingObjectHandler()
  {
    return poolOfString.take().get();
  }
}
