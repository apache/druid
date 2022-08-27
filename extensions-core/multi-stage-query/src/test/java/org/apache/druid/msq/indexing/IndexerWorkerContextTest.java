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

package org.apache.druid.msq.indexing;

import com.google.common.util.concurrent.Futures;
import com.google.inject.Injector;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class IndexerWorkerContextTest
{

  private IndexerWorkerContext indexerWorkerContext = null;

  @Before
  public void setup()
  {
    final Injector injectorMock = Mockito.mock(Injector.class);
    Mockito.when(injectorMock.getInstance(SegmentCacheManagerFactory.class))
           .thenReturn(Mockito.mock(SegmentCacheManagerFactory.class));

    indexerWorkerContext = new IndexerWorkerContext(
        Mockito.mock(TaskToolbox.class),
        injectorMock,
        null,
        null,
        null
    );
  }

  @Test
  public void testControllerCheckerRunnableExitsWhenEmptyStatus()
  {
    final ServiceLocator controllerLocatorMock = Mockito.mock(ServiceLocator.class);
    Mockito.when(controllerLocatorMock.locate())
           .thenReturn(Futures.immediateFuture(ServiceLocations.forLocations(Collections.emptySet())));

    final Worker workerMock = Mockito.mock(Worker.class);

    indexerWorkerContext.controllerCheckerRunnable(controllerLocatorMock, workerMock);
    Mockito.verify(controllerLocatorMock, Mockito.times(1)).locate();
    Mockito.verify(workerMock, Mockito.times(1)).controllerFailed();
  }

  @Test
  public void testControllerCheckerRunnableExitsOnlyWhenClosedStatus()
  {
    final ServiceLocator controllerLocatorMock = Mockito.mock(ServiceLocator.class);
    Mockito.when(controllerLocatorMock.locate())
           .thenReturn(Futures.immediateFuture(ServiceLocations.forLocation(new ServiceLocation("h", 1, -1, "/"))))
           // Done to check the behavior of the runnable, the situation of exiting after success might not occur actually
           .thenReturn(Futures.immediateFuture(ServiceLocations.forLocation(new ServiceLocation("h", 1, -1, "/"))))
           .thenReturn(Futures.immediateFuture(ServiceLocations.closed()));

    final Worker workerMock = Mockito.mock(Worker.class);

    indexerWorkerContext.controllerCheckerRunnable(controllerLocatorMock, workerMock);
    Mockito.verify(controllerLocatorMock, Mockito.times(3)).locate();
    Mockito.verify(workerMock, Mockito.times(1)).controllerFailed();
  }
}
