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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

public class OmniDataSegmentKillerTest
{
  @Test
  public void testKillSegmentWithType() throws SegmentLoadingException
  {
    final DataSegmentKiller killer = Mockito.mock(DataSegmentKiller.class);
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.isTombstone()).thenReturn(false);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "sane"));

    final Injector injector = createInjector(killer);
    final OmniDataSegmentKiller segmentKiller = injector.getInstance(OmniDataSegmentKiller.class);
    segmentKiller.kill(segment);
    Mockito.verify(killer, Mockito.times(1)).kill(segment);
  }

  @Test
  public void testKillSegmentUnknowType()
  {
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "unknown-type"));

    final Injector injector = createInjector(null);
    final OmniDataSegmentKiller segmentKiller = injector.getInstance(OmniDataSegmentKiller.class);
    Assert.assertThrows(
        "Unknown loader type[unknown-type]. Known types are [explode]",
        SegmentLoadingException.class,
        () -> segmentKiller.kill(segment)
    );
  }

  @Test
  public void testBadSegmentKillerAccessException()
  {
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "bad"));

    final Injector injector = createInjector(null);
    final OmniDataSegmentKiller segmentKiller = injector.getInstance(OmniDataSegmentKiller.class);
    Assert.assertThrows(
        "BadSegmentKiller must not have been initialized",
        RuntimeException.class,
        () -> segmentKiller.kill(segment)
    );
  }

  private static Injector createInjector(@Nullable DataSegmentKiller killer)
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> {
              MapBinder<String, DataSegmentKiller> mapBinder = Binders.dataSegmentKillerBinder(binder);
              if (killer != null) {
                mapBinder.addBinding("sane").toInstance(killer);
              }
            },
            binder -> {
              MapBinder<String, DataSegmentKiller> mapBinder = Binders.dataSegmentKillerBinder(binder);
              mapBinder.addBinding("bad").to(BadSegmentKiller.class);
            }
        )
    );
  }

  @Test
  public void testKillTombstone() throws Exception
  {
    // tombstone
    DataSegment tombstone =
        DataSegment.builder()
                   .dataSource("test")
                   .interval(Intervals.of("2021-01-01/P1D"))
                   .version("version")
                   .size(1)
                   .loadSpec(ImmutableMap.of("type", "tombstone", "path", "null"))
                   .shardSpec(new TombstoneShardSpec())
                   .build();

    final Injector injector = createInjector(null);
    final OmniDataSegmentKiller segmentKiller = injector.getInstance(OmniDataSegmentKiller.class);
    segmentKiller.kill(tombstone);
  }

  @LazySingleton
  private static class BadSegmentKiller implements DataSegmentKiller
  {
    @Inject
    BadSegmentKiller()
    {
      throw new RuntimeException("BadSegmentKiller must not have been initialized");
    }

    @Override
    public void kill(DataSegment segment)
    {

    }

    @Override
    public void killAll()
    {

    }
  }

}
