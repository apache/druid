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

package io.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.EmittingLogger;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class LookupReferencesManagerTest
{
  LookupReferencesManager lookupReferencesManager;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp() throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(temporaryFolder.newFolder().getAbsolutePath()),
        mapper,
        true
    );
  }

  @Test
  public void testStartStop()
  {
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(null),
        mapper
    );

    Assert.assertFalse(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertNull(lookupReferencesManager.mainThread);
    Assert.assertNull(lookupReferencesManager.stateRef.get());

    lookupReferencesManager.start();
    Assert.assertTrue(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertTrue(lookupReferencesManager.mainThread.isAlive());
    Assert.assertNotNull(lookupReferencesManager.stateRef.get());

    lookupReferencesManager.stop();
    Assert.assertFalse(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertFalse(lookupReferencesManager.mainThread.isAlive());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetExceptionWhenClosed()
  {
    lookupReferencesManager.get("test");
  }

  @Test(expected = IllegalStateException.class)
  public void testAddExceptionWhenClosed()
  {
    lookupReferencesManager.add("test", EasyMock.createMock(LookupExtractorFactoryContainer.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testRemoveExceptionWhenClosed()
  {
    lookupReferencesManager.remove("test");
  }

  @Test(expected = IllegalStateException.class)
  public void testGetAllLookupsStateExceptionWhenClosed()
  {
    lookupReferencesManager.getAllLookupsState();
  }

  @Test
  public void testAddGetRemove() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    lookupReferencesManager.start();
    Assert.assertNull(lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);

    lookupReferencesManager.add("test", testContainer);
    lookupReferencesManager.handlePendingNotices();

    Assert.assertEquals(testContainer, lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test");
    lookupReferencesManager.handlePendingNotices();

    Assert.assertNull(lookupReferencesManager.get("test"));
  }

  @Test
  public void testCloseIsCalledAfterStopping() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testMock", new LookupExtractorFactoryContainer("0", lookupExtractorFactory));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.stop();
    EasyMock.verify(lookupExtractorFactory);
  }

  @Test
  public void testCloseIsCalledAfterRemove() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);

    lookupReferencesManager.start();
    lookupReferencesManager.add("testMock", new LookupExtractorFactoryContainer("0", lookupExtractorFactory));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.remove("testMock");
    lookupReferencesManager.handlePendingNotices();

    EasyMock.verify(lookupExtractorFactory);
  }

  @Test
  public void testGetNotThere()
  {
    lookupReferencesManager.start();
    Assert.assertNull(lookupReferencesManager.get("notThere"));
  }

  @Test
  public void testUpdateWithHigherVersion() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory1 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory1.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory1.close()).andReturn(true).once();

    LookupExtractorFactory lookupExtractorFactory2 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory2.start()).andReturn(true).once();

    EasyMock.replay(lookupExtractorFactory1, lookupExtractorFactory2);

    lookupReferencesManager.start();
    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("1", lookupExtractorFactory1));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("2", lookupExtractorFactory2));
    lookupReferencesManager.handlePendingNotices();

    EasyMock.verify(lookupExtractorFactory1, lookupExtractorFactory2);
  }

  @Test
  public void testUpdateWithLowerVersion() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory1 = EasyMock.createNiceMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory1.start()).andReturn(true).once();

    LookupExtractorFactory lookupExtractorFactory2 = EasyMock.createNiceMock(LookupExtractorFactory.class);

    EasyMock.replay(lookupExtractorFactory1, lookupExtractorFactory2);

    lookupReferencesManager.start();
    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("1", lookupExtractorFactory1));
    lookupReferencesManager.handlePendingNotices();

    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("0", lookupExtractorFactory2));
    lookupReferencesManager.handlePendingNotices();

    EasyMock.verify(lookupExtractorFactory1, lookupExtractorFactory2);
  }

  @Test
  public void testRemoveNonExisting() throws Exception
  {
    lookupReferencesManager.start();
    lookupReferencesManager.remove("test");
    lookupReferencesManager.handlePendingNotices();
  }

  @Test
  public void testBootstrapFromFile() throws Exception
  {
    LookupExtractorFactory lookupExtractorFactory = new MapLookupExtractorFactory(
        ImmutableMap.<String, String>of(
            "key",
            "value"
        ), true
    );
    LookupExtractorFactoryContainer container = new LookupExtractorFactoryContainer("v0", lookupExtractorFactory);
    lookupReferencesManager.start();
    lookupReferencesManager.add("testMockForBootstrap", container);
    lookupReferencesManager.handlePendingNotices();
    lookupReferencesManager.stop();

    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(lookupReferencesManager.lookupSnapshotTaker.getPersistFile().getParent()),
        mapper,
        true
    );
    lookupReferencesManager.start();
    Assert.assertEquals(container, lookupReferencesManager.get("testMockForBootstrap"));
  }

  @Test
  public void testGetAllLookupsState() throws Exception
  {
    LookupExtractorFactoryContainer container1 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key1",
                "value1"
            ), true
        )
    );

    LookupExtractorFactoryContainer container2 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key2",
                "value2"
            ), true
        )
    );

    LookupExtractorFactoryContainer container3 = new LookupExtractorFactoryContainer(
        "0",
        new MapLookupExtractorFactory(
            ImmutableMap.of(
                "key3",
                "value3"
            ), true
        )
    );

    lookupReferencesManager.start();
    lookupReferencesManager.add("one", container1);
    lookupReferencesManager.add("two", container2);
    lookupReferencesManager.handlePendingNotices();
    lookupReferencesManager.remove("one");
    lookupReferencesManager.add("three", container3);

    LookupsState state = lookupReferencesManager.getAllLookupsState();

    Assert.assertEquals(2, state.getCurrent().size());
    Assert.assertEquals(container1, state.getCurrent().get("one"));
    Assert.assertEquals(container2, state.getCurrent().get("two"));

    Assert.assertEquals(1, state.getToLoad().size());
    Assert.assertEquals(container3, state.getToLoad().get("three"));

    Assert.assertEquals(1, state.getToDrop().size());
    Assert.assertTrue(state.getToDrop().contains("one"));
  }

  @Test (timeout = 20000)
  public void testRealModeWithMainThread() throws Exception
  {
    LookupReferencesManager lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(temporaryFolder.newFolder().getAbsolutePath()),
        mapper
    );

    lookupReferencesManager.start();
    Assert.assertTrue(lookupReferencesManager.mainThread.isAlive());

    LookupExtractorFactory lookupExtractorFactory = EasyMock.createMock(LookupExtractorFactory.class);
    EasyMock.expect(lookupExtractorFactory.start()).andReturn(true).once();
    EasyMock.expect(lookupExtractorFactory.close()).andReturn(true).once();
    EasyMock.replay(lookupExtractorFactory);
    Assert.assertNull(lookupReferencesManager.get("test"));

    LookupExtractorFactoryContainer testContainer = new LookupExtractorFactoryContainer("0", lookupExtractorFactory);
    lookupReferencesManager.add("test", testContainer);

    while (!testContainer.equals(lookupReferencesManager.get("test"))) {
      Thread.sleep(100);
    }

    lookupReferencesManager.remove("test");

    while (lookupReferencesManager.get("test") != null) {
      Thread.sleep(100);
    }

    lookupReferencesManager.stop();

    Assert.assertFalse(lookupReferencesManager.mainThread.isAlive());
  }
}
