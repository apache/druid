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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class StupidPoolTest
{
  private Supplier<String> generator;
  private CloseableStupidPool<String> poolOfString;
  private ResourceHolder<String> resourceHolderObj;
  private String defaultString = new String("test");

  @BeforeEach
  public void setUp()
  {
    generator = EasyMock.createMock(Supplier.class);
    EasyMock.expect(generator.get()).andReturn(defaultString).anyTimes();
    EasyMock.replay(generator);
    poolOfString = new CloseableStupidPool<>("poolOfString", generator);
    resourceHolderObj = poolOfString.take();
  }

  @AfterEach
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
    Assertions.assertInstanceOf(ResourceHolder.class, resourceHolderObj);
    Object expectedObject = resourceHolderObj.get();
    Assertions.assertEquals(expectedObject, defaultString);
  }

  @Test
  public void testExceptionInResourceHolderGet()
  {
    resourceHolderObj.close();
    Assertions.assertThrows(ISE.class, () -> resourceHolderObj.get());
  }

  @Test
  @Timeout(60)
  public void testResourceHandlerClearedByJVM()
  {
    StupidPool<String> poolOfString = new StupidPool<>("poolOfString", () -> "billybob");

    final StupidPool.ObjectResourceHolder take = (StupidPool.ObjectResourceHolder) poolOfString.take();
    take.forceClean();

    Assertions.assertEquals(1, poolOfString.leakedObjectsCount(), "Expected there to be one leak");

    boolean exceptionThrown = false;
    try {
      poolOfString.take();
    }
    catch (Exception e) {
      exceptionThrown = true;
    }
    Assertions.assertTrue(exceptionThrown, "Expect the pool to throw an exception as it should be poisoned");
  }
}
