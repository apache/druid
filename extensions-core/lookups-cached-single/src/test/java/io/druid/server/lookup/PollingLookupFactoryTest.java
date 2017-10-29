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

package io.druid.server.lookup;

import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class PollingLookupFactoryTest
{

  PollingLookup pollingLookup = EasyMock.createMock(PollingLookup.class);
  PollingLookupFactory pollingLookupFactory = new PollingLookupFactory(Period.ZERO, null, null, pollingLookup);

  @Test
  public void testStart()
  {
    EasyMock.expect(pollingLookup.isOpen()).andReturn(true).once();
    EasyMock.replay(pollingLookup);
    Assert.assertTrue(pollingLookupFactory.start());
    EasyMock.verify(pollingLookup);
  }

  @Test
  public void testClose()
  {
    Assert.assertTrue(pollingLookupFactory.close());
  }

  @Test
  public void testReplacesWithNull()
  {
    Assert.assertTrue(pollingLookupFactory.replaces(null));
  }

  @Test
  public void testReplaces()
  {
    Assert.assertTrue(pollingLookupFactory.replaces(new PollingLookupFactory(
        Period.millis(1),
        null,
        null,
        pollingLookup
    )));
  }

  @Test
  public void testGet()
  {
    Assert.assertEquals(pollingLookup, pollingLookupFactory.get());
  }

}
