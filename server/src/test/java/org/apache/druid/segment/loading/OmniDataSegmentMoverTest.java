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
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.Binders;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

public class OmniDataSegmentMoverTest
{
  @Test
  public void testMoveSegmentWithType() throws SegmentLoadingException
  {
    final DataSegmentMover mover = Mockito.mock(DataSegmentMover.class);
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "sane"));

    final Injector injector = createInjector(mover);
    final OmniDataSegmentMover segmentMover = injector.getInstance(OmniDataSegmentMover.class);
    segmentMover.move(segment, ImmutableMap.of());
    Mockito.verify(mover, Mockito.times(1)).move(segment, ImmutableMap.of());
  }

  @Test
  public void testMoveSegmentUnknownType()
  {
    final DataSegment segment = Mockito.mock(DataSegment.class);
    Mockito.when(segment.getLoadSpec()).thenReturn(ImmutableMap.of("type", "unknown-type"));

    final Injector injector = createInjector(null);
    final OmniDataSegmentMover segmentMover = injector.getInstance(OmniDataSegmentMover.class);
    Assert.assertThrows(
        "Unknown loader type[unknown-type]. Known types are [explode]",
        SegmentLoadingException.class,
        () -> segmentMover.move(segment, ImmutableMap.of())
    );
  }

  private static Injector createInjector(@Nullable DataSegmentMover mover)
  {
    return GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            binder -> {
              MapBinder<String, DataSegmentMover> mapBinder = Binders.dataSegmentMoverBinder(binder);
              if (mover != null) {
                mapBinder.addBinding("sane").toInstance(mover);
              }
            }
        )
    );
  }
}
