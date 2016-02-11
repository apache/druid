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

package io.druid.query.extraction;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LookupReferencesManagerTest
{
  LookupReferencesManager lookupReferencesManager;

  @Before
  public void setUp()
  {
    lookupReferencesManager = new LookupReferencesManager();
    Assert.assertTrue("must be closed before start call", lookupReferencesManager.isClosed());
    lookupReferencesManager.start();
    Assert.assertFalse("must start after start call", lookupReferencesManager.isClosed());
  }

  @After
  public void tearDown()
  {
    lookupReferencesManager.stop();
    Assert.assertTrue("stop call should close it", lookupReferencesManager.isClosed());
  }

  @Test(expected = ISE.class)
  public void testGetExceptionWhenClosed()
  {
    lookupReferencesManager.stop();
    lookupReferencesManager.get("test");
  }

  @Test(expected = ISE.class)
  public void testAddExceptionWhenClosed()
  {
    lookupReferencesManager.stop();
    lookupReferencesManager.put("test", EasyMock.createMock(LookupExtractorFactory.class));
  }

  @Test
  public void testPutGetRemove()
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertNull(lookupReferencesManager.get("test"));
    lookupReferencesManager.put("test", lookupExtractorFactory);
    Assert.assertEquals(lookupExtractorFactory, lookupReferencesManager.get("test"));
    Assert.assertTrue(lookupReferencesManager.remove("test"));
    Assert.assertNull(lookupReferencesManager.get("test"));
  }

  @Test
  public void testCloseIsCalledAfterStopping() throws IOException
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    lookupReferencesManager.put("testMock", lookupExtractorFactory);
    lookupReferencesManager.stop();
    EasyMock.verify(lookupExtractorFactory);
  }

  @Test
  public void testCloseIsCalledAfterRemove() throws IOException
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    lookupReferencesManager.put("testMock", lookupExtractorFactory);
    lookupReferencesManager.remove("testMock");
    EasyMock.verify(lookupExtractorFactory);
  }

  @Test
  public void testRemoveInExisting()
  {
    Assert.assertFalse(lookupReferencesManager.remove("notThere"));
  }

  @Test
  public void testGetNotThere()
  {
    Assert.assertNull(lookupReferencesManager.get("notThere"));
  }

  @Test
  public void testAddingWithSameLookupName()
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    LookupExtractorFactory lookupExtractorFactory2 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory2.start()).andReturn(true).times(2);
    EasyMock.replay(lookupExtractorFactory, lookupExtractorFactory2);
    Assert.assertTrue(lookupReferencesManager.put("testName", lookupExtractorFactory));
    Assert.assertFalse(lookupReferencesManager.put("testName", lookupExtractorFactory2));
    ImmutableMap<String, LookupExtractorFactory> extractorImmutableMap = ImmutableMap.of(
        "testName",
        lookupExtractorFactory2
    );
    lookupReferencesManager.put(extractorImmutableMap);
    Assert.assertEquals(lookupExtractorFactory, lookupReferencesManager.get("testName"));
  }

  @Test
  public void testAddLookupsThenGetAll()
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    LookupExtractorFactory lookupExtractorFactory2 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory2.start()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory, lookupExtractorFactory2);
    ImmutableMap<String, LookupExtractorFactory> extractorImmutableMap = ImmutableMap.of(
        "name1",
        lookupExtractorFactory,
        "name2",
        lookupExtractorFactory2
    );
    lookupReferencesManager.put(extractorImmutableMap);
    Assert.assertEquals(extractorImmutableMap, lookupReferencesManager.getAll());
  }

  @Test(expected = ISE.class)
  public void testExceptionWhenStartFail()
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(false).once();
    EasyMock.replay(lookupExtractorFactory);
    lookupReferencesManager.put("testMock", lookupExtractorFactory);
  }

  @Test(expected = ISE.class)
  public void testputAllExceptionWhenStartFail()
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(false).once();
    ImmutableMap<String, LookupExtractorFactory> extractorImmutableMap = ImmutableMap.of(
        "name1",
        lookupExtractorFactory
    );
    lookupReferencesManager.put(extractorImmutableMap);
  }
}
