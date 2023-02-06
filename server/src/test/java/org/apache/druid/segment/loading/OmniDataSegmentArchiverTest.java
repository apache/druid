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

package org.apache.druid.segment.loading;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

public class OmniDataSegmentArchiverTest
{
  @Test
  public void testArchiveSegmentWithType() throws SegmentLoadingException
  {
    final DataSegmentArchiver archiver = Mockito.mock(DataSegmentArchiver.class);
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "sane"));

    final Injector injector = createInjector(archiver);
    final DataSegmentArchiver segmentArchiver = injector.getInstance(OmniDataSegmentArchiver.class);
    segmentArchiver.archive(segment);
    Mockito.verify(archiver, Mockito.times(1)).archive(segment);
  }

  @Test
  public void testArchiveSegmentUnknowType()
  {
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "unknown-type"));

    final Injector injector = createInjector(null);
    final OmniDataSegmentArchiver segmentArchiver = injector.getInstance(OmniDataSegmentArchiver.class);
    Assert.assertThrows(
        "Unknown loader type[unknown-type]. Known types are [explode]",
        SegmentLoadingException.class,
        () -> segmentArchiver.archive(segment)
    );
  }

  @Test
  public void testBadSegmentArchiverAccessException()
  {
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "bad"));

    final Injector injector = createInjector(null);
    final OmniDataSegmentArchiver segmentArchiver = injector.getInstance(OmniDataSegmentArchiver.class);
    Assert.assertThrows(
        "BadSegmentArchiver must not have been initialized",
        RuntimeException.class,
        () -> segmentArchiver.archive(segment)
    );
  }

  private static Injector createInjector(@Nullable DataSegmentArchiver archiver)
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> {
              MapBinder<String, DataSegmentArchiver> mapBinder = Binders.dataSegmentArchiverBinder(binder);
              if (archiver != null) {
                mapBinder.addBinding("sane").toInstance(archiver);
              }
            },
            binder -> {
              MapBinder<String, DataSegmentArchiver> mapBinder = Binders.dataSegmentArchiverBinder(binder);
              mapBinder.addBinding("bad").to(BadSegmentArchiver.class);
            }
        )
    );
  }
  
  @LazySingleton
  private static class BadSegmentArchiver implements DataSegmentArchiver
  {

    @Inject
    BadSegmentArchiver()
    {
      throw new RuntimeException("BadSegmentArchiver must not have been initialized");
    }

    @Override
    public DataSegment archive(DataSegment segment)
    {
      return null;
    }

    @Override
    public DataSegment restore(DataSegment segment)
    {
      return null;
    }
  }
}
