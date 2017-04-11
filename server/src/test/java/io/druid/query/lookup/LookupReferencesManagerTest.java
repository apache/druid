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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.druid.concurrent.Execs;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class LookupReferencesManagerTest
{
  private static final int CONCURRENT_THREADS = 16;
  LookupReferencesManager lookupReferencesManager;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  ObjectMapper mapper = new DefaultObjectMapper();
  private final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Execs.multiThreaded(
      CONCURRENT_THREADS,
      "hammer-time-%s"
  ));

  @Before
  public void setUp() throws IOException
  {
    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    lookupReferencesManager = new LookupReferencesManager(
        new LookupConfig(Files.createTempDir().getAbsolutePath()),
        mapper
    );
    Assert.assertTrue("must be closed before start call", lookupReferencesManager.isClosed());
    lookupReferencesManager.start();
    Assert.assertFalse("must start after start call", lookupReferencesManager.isClosed());
  }

  @After
  public void tearDown()
  {
    lookupReferencesManager.stop();
    Assert.assertTrue("stop call should close it", lookupReferencesManager.isClosed());
    executorService.shutdownNow();
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

  @Test
  public void testUpdateIfNewOnlyIfIsNew()
  {
    final String lookupName = "some lookup";
    LookupExtractorFactory oldFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    LookupExtractorFactory newFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);

    EasyMock.expect(oldFactory.replaces(EasyMock.<LookupExtractorFactory>isNull())).andReturn(true).once();
    EasyMock.expect(oldFactory.start()).andReturn(true).once();
    EasyMock.expect(oldFactory.replaces(EasyMock.eq(oldFactory))).andReturn(false).once();
    // Add new

    EasyMock.expect(newFactory.replaces(EasyMock.eq(oldFactory))).andReturn(true).once();
    EasyMock.expect(newFactory.start()).andReturn(true).once();
    EasyMock.expect(oldFactory.close()).andReturn(true).once();
    EasyMock.expect(newFactory.close()).andReturn(true).once();

    EasyMock.replay(oldFactory, newFactory);

    Assert.assertTrue(lookupReferencesManager.updateIfNew(lookupName, oldFactory));
    Assert.assertFalse(lookupReferencesManager.updateIfNew(lookupName, oldFactory));
    Assert.assertTrue(lookupReferencesManager.updateIfNew(lookupName, newFactory));

    // Remove now or else EasyMock gets confused on lazy lookup manager stop handling
    lookupReferencesManager.remove(lookupName);

    EasyMock.verify(oldFactory, newFactory);
  }

  @Test(expected = ISE.class)
  public void testUpdateIfNewExceptional()
  {
    final String lookupName = "some lookup";
    LookupExtractorFactory newFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    EasyMock.expect(newFactory.replaces(EasyMock.<LookupExtractorFactory>isNull())).andReturn(true).once();
    EasyMock.expect(newFactory.start()).andReturn(false).once();
    EasyMock.replay(newFactory);
    try {
      lookupReferencesManager.updateIfNew(lookupName, newFactory);
    }
    finally {
      EasyMock.verify(newFactory);
    }
  }

  @Test
  public void testUpdateIfNewSuppressOldCloseProblem()
  {
    final String lookupName = "some lookup";
    LookupExtractorFactory oldFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);
    LookupExtractorFactory newFactory = EasyMock.createStrictMock(LookupExtractorFactory.class);

    EasyMock.expect(oldFactory.replaces(EasyMock.<LookupExtractorFactory>isNull())).andReturn(true).once();
    EasyMock.expect(oldFactory.start()).andReturn(true).once();
    // Add new
    EasyMock.expect(newFactory.replaces(EasyMock.eq(oldFactory))).andReturn(true).once();
    EasyMock.expect(newFactory.start()).andReturn(true).once();
    EasyMock.expect(oldFactory.close()).andReturn(false).once();
    EasyMock.expect(newFactory.close()).andReturn(true).once();

    EasyMock.replay(oldFactory, newFactory);

    lookupReferencesManager.updateIfNew(lookupName, oldFactory);
    lookupReferencesManager.updateIfNew(lookupName, newFactory);

    // Remove now or else EasyMock gets confused on lazy lookup manager stop handling
    lookupReferencesManager.remove(lookupName);

    EasyMock.verify(oldFactory, newFactory);
  }

  @Test
  public void testBootstrapFromFile() throws IOException
  {
    LookupExtractorFactory lookupExtractorFactory = new MapLookupExtractorFactory(ImmutableMap.<String, String>of(
        "key",
        "value"
    ), true);
    lookupReferencesManager.put("testMockForBootstrap", lookupExtractorFactory);
    lookupReferencesManager.stop();
    lookupReferencesManager.start();
    Assert.assertEquals(lookupExtractorFactory, lookupReferencesManager.get("testMockForBootstrap"));

  }

  @Test
  public void testConcurrencyStaaaaaaaaaaartStop() throws Exception
  {
    lookupReferencesManager.stop();
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(CONCURRENT_THREADS);
    final Runnable start = new Runnable()
    {
      @Override
      public void run()
      {
        try {
          cyclicBarrier.await();
        }
        catch (InterruptedException | BrokenBarrierException e) {
          throw Throwables.propagate(e);
        }
        lookupReferencesManager.start();
      }
    };
    final Collection<ListenableFuture<?>> futures = new ArrayList<>(CONCURRENT_THREADS);
    for (int i = 0; i < CONCURRENT_THREADS; ++i) {
      futures.add(executorService.submit(start));
    }
    lookupReferencesManager.stop();
    Futures.allAsList(futures).get(100, TimeUnit.MILLISECONDS);
    for (ListenableFuture future : futures) {
      Assert.assertNull(future.get());
    }
  }

  @Test
  public void testConcurrencyStartStoooooooooop() throws Exception
  {
    lookupReferencesManager.stop();
    lookupReferencesManager.start();
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(CONCURRENT_THREADS);
    final Runnable start = new Runnable()
    {
      @Override
      public void run()
      {
        try {
          cyclicBarrier.await();
        }
        catch (InterruptedException | BrokenBarrierException e) {
          throw Throwables.propagate(e);
        }
        lookupReferencesManager.stop();
      }
    };
    final Collection<ListenableFuture<?>> futures = new ArrayList<>(CONCURRENT_THREADS);
    for (int i = 0; i < CONCURRENT_THREADS; ++i) {
      futures.add(executorService.submit(start));
    }
    Futures.allAsList(futures).get(100, TimeUnit.MILLISECONDS);
    for (ListenableFuture future : futures) {
      Assert.assertNull(future.get());
    }
  }

  @Test(timeout = 10000L)
  public void testConcurrencySequentialChaos() throws Exception
  {
    final CountDownLatch runnableStartBarrier = new CountDownLatch(1);
    final Random random = new Random(478137498L);
    final int numUpdates = 100000;
    final int numNamespaces = 100;
    final CountDownLatch runnablesFinishedBarrier = new CountDownLatch(numUpdates);
    final List<Runnable> runnables = new ArrayList<>(numUpdates);
    final Map<String, Integer> maxNumber = new HashMap<>();
    for (int i = 1; i <= numUpdates; ++i) {
      final boolean shouldStart = random.nextInt(10) == 1;
      final boolean shouldClose = random.nextInt(10) == 1;
      final String name = Integer.toString(random.nextInt(numNamespaces));
      final int position = i;

      final LookupExtractorFactory lookupExtractorFactory = new LookupExtractorFactory()
      {
        @Override
        public boolean start()
        {
          return shouldStart;
        }

        @Override
        public boolean close()
        {
          return shouldClose;
        }

        @Override
        public boolean replaces(@Nullable LookupExtractorFactory other)
        {
          if (other == null) {
            return true;
          }
          final NamedIntrospectionHandler introspectionHandler = (NamedIntrospectionHandler) other.getIntrospectHandler();
          return position > introspectionHandler.position;
        }

        @Nullable
        @Override
        public LookupIntrospectHandler getIntrospectHandler()
        {
          return new NamedIntrospectionHandler(position);
        }

        @Override
        public String toString()
        {
          return String.format("TestFactroy position %d", position);
        }

        @Override
        public LookupExtractor get()
        {
          return null;
        }
      };

      if (shouldStart && (!maxNumber.containsKey(name) || maxNumber.get(name) < position)) {
        maxNumber.put(name, position);
      }
      runnables.add(new LookupUpdatingRunnable(
          name,
          lookupExtractorFactory,
          runnableStartBarrier,
          lookupReferencesManager
      ));
    }
    ////// Add some CHAOS!
    Collections.shuffle(runnables, random);
    final Runnable decrementFinished = new Runnable()
    {
      @Override
      public void run()
      {
        runnablesFinishedBarrier.countDown();
      }
    };
    for (Runnable runnable : runnables) {
      executorService.submit(runnable).addListener(decrementFinished, MoreExecutors.sameThreadExecutor());
    }

    runnableStartBarrier.countDown();
    do {
      for (String name : maxNumber.keySet()) {
        final LookupExtractorFactory factory;
        try {
          factory = lookupReferencesManager.get(name);
        }
        catch (ISE e) {
          continue;
        }
        if (null == factory) {
          continue;
        }
        final NamedIntrospectionHandler introspectionHandler = (NamedIntrospectionHandler) factory.getIntrospectHandler();
        Assert.assertTrue(introspectionHandler.position >= 0);
      }
    } while (runnablesFinishedBarrier.getCount() > 0);

    lookupReferencesManager.start();

    for (String name : maxNumber.keySet()) {
      final LookupExtractorFactory factory = lookupReferencesManager.get(name);
      if (null == factory) {
        continue;
      }
      final NamedIntrospectionHandler introspectionHandler = (NamedIntrospectionHandler) factory.getIntrospectHandler();
      Assert.assertNotNull(introspectionHandler);
      Assert.assertEquals(
          StringUtils.safeFormat("Named position %s failed", name),
          maxNumber.get(name),
          Integer.valueOf(introspectionHandler.position)
      );
    }
    Assert.assertEquals(maxNumber.size(), lookupReferencesManager.getAll().size());
  }

  @Test(timeout = 10000L)
  public void testConcurrencyStartStopChaos() throws Exception
  {
    // Don't want to exercise snapshot here
    final LookupReferencesManager manager = new LookupReferencesManager(new LookupConfig(null), mapper);
    final Runnable chaosStart = new Runnable()
    {
      @Override
      public void run()
      {
        manager.start();
      }
    };
    final Runnable chaosStop = new Runnable()
    {
      @Override
      public void run()
      {
        manager.stop();
      }
    };
    final CountDownLatch runnableStartBarrier = new CountDownLatch(1);
    final Random random = new Random(478137498L);
    final int numUpdates = 100000;
    final int numNamespaces = 100;
    final CountDownLatch runnablesFinishedBarrier = new CountDownLatch(numUpdates);
    final List<Runnable> runnables = new ArrayList<>(numUpdates);
    final Map<String, Integer> maxNumber = new HashMap<>();
    for (int i = 1; i <= numUpdates; ++i) {
      final boolean shouldStart = random.nextInt(10) == 1;
      final boolean shouldClose = random.nextInt(10) == 1;
      final String name = Integer.toString(random.nextInt(numNamespaces));
      final int position = i;

      final LookupExtractorFactory lookupExtractorFactory = new LookupExtractorFactory()
      {
        @Override
        public boolean start()
        {
          return shouldStart;
        }

        @Override
        public boolean close()
        {
          return shouldClose;
        }

        @Override
        public boolean replaces(@Nullable LookupExtractorFactory other)
        {
          if (other == null) {
            return true;
          }
          final NamedIntrospectionHandler introspectionHandler = (NamedIntrospectionHandler) other.getIntrospectHandler();
          return position > introspectionHandler.position;
        }

        @Nullable
        @Override
        public LookupIntrospectHandler getIntrospectHandler()
        {
          return new NamedIntrospectionHandler(position);
        }

        @Override
        public String toString()
        {
          return String.format("TestFactroy position %d", position);
        }

        @Override
        public LookupExtractor get()
        {
          return null;
        }
      };
      if (random.nextFloat() < 0.001) {
        if (random.nextBoolean()) {
          runnables.add(chaosStart);
        } else {
          runnables.add(chaosStop);
        }
      } else {
        if (shouldStart && (!maxNumber.containsKey(name) || maxNumber.get(name) < position)) {
          maxNumber.put(name, position);
        }
        runnables.add(new LookupUpdatingRunnable(
            name,
            lookupExtractorFactory,
            runnableStartBarrier,
            manager
        ));
      }
    }
    ////// Add some CHAOS!
    Collections.shuffle(runnables, random);
    final Runnable decrementFinished = new Runnable()
    {
      @Override
      public void run()
      {
        runnablesFinishedBarrier.countDown();
      }
    };
    for (Runnable runnable : runnables) {
      executorService.submit(runnable).addListener(decrementFinished, MoreExecutors.sameThreadExecutor());
    }

    runnableStartBarrier.countDown();
    do {
      for (String name : maxNumber.keySet()) {
        final LookupExtractorFactory factory;
        try {
          factory = manager.get(name);
        }
        catch (ISE e) {
          continue;
        }
        if (null == factory) {
          continue;
        }
        final NamedIntrospectionHandler introspectionHandler = (NamedIntrospectionHandler) factory.getIntrospectHandler();
        Assert.assertTrue(introspectionHandler.position >= 0);
      }
    } while (runnablesFinishedBarrier.getCount() > 0);
  }
}

class LookupUpdatingRunnable implements Runnable
{
  final String name;
  final LookupExtractorFactory factory;
  final CountDownLatch startLatch;
  final LookupReferencesManager lookupReferencesManager;

  LookupUpdatingRunnable(
      String name,
      LookupExtractorFactory factory,
      CountDownLatch startLatch,
      LookupReferencesManager lookupReferencesManager
  )
  {
    this.name = name;
    this.factory = factory;
    this.startLatch = startLatch;
    this.lookupReferencesManager = lookupReferencesManager;
  }

  @Override
  public void run()
  {
    try {
      startLatch.await();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    lookupReferencesManager.updateIfNew(name, factory);
  }
}

class NamedIntrospectionHandler implements LookupIntrospectHandler
{
  final int position;

  NamedIntrospectionHandler(final int position)
  {
    this.position = position;
  }
}
