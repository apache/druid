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
        mapper,
        false
    );

    Assert.assertFalse(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertNull(lookupReferencesManager.mainThread);

    lookupReferencesManager.start();
    Assert.assertTrue(lookupReferencesManager.lifecycleLock.awaitStarted(1, TimeUnit.MICROSECONDS));
    Assert.assertTrue(lookupReferencesManager.mainThread.isAlive());

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
    handleOneNotice(lookupReferencesManager);

    Assert.assertEquals(testContainer, lookupReferencesManager.get("test"));

    lookupReferencesManager.remove("test");
    handleOneNotice(lookupReferencesManager);

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
    handleOneNotice(lookupReferencesManager);

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
    handleOneNotice(lookupReferencesManager);

    lookupReferencesManager.remove("testMock");
    handleOneNotice(lookupReferencesManager);

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
    handleOneNotice(lookupReferencesManager);

    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("2", lookupExtractorFactory2));
    handleOneNotice(lookupReferencesManager);

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
    handleOneNotice(lookupReferencesManager);

    lookupReferencesManager.add("testName", new LookupExtractorFactoryContainer("0", lookupExtractorFactory2));
    handleOneNotice(lookupReferencesManager);

    EasyMock.verify(lookupExtractorFactory1, lookupExtractorFactory2);
  }

  @Test
  public void testRemoveNonExisting() throws Exception
  {
    lookupReferencesManager.start();
    lookupReferencesManager.remove("test");
    handleOneNotice(lookupReferencesManager);
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
    handleOneNotice(lookupReferencesManager);
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
    lookupReferencesManager.remove("one");
    lookupReferencesManager.add("three", container3);

    handleOneNotice(lookupReferencesManager);
    handleOneNotice(lookupReferencesManager);

    LookupsState state = lookupReferencesManager.getAllLookupsState();

    Assert.assertEquals(2, state.getCurrent().size());
    Assert.assertEquals(container1, state.getCurrent().get("one"));
    Assert.assertEquals(container2, state.getCurrent().get("two"));

    Assert.assertEquals(1, state.getToLoad().size());
    Assert.assertEquals(container3, state.getToLoad().get("three"));

    Assert.assertEquals(1, state.getToDrop().size());
    Assert.assertTrue(state.getToDrop().contains("one"));
  }

  private void handleOneNotice(LookupReferencesManager mgr) throws Exception {
    mgr.queue.take().handle();
  }
}
