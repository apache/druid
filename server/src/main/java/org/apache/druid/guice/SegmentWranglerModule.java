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

package org.apache.druid.guice;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.LookupSegmentWrangler;
import org.apache.druid.segment.MapSegmentWrangler;
import org.apache.druid.segment.SegmentWrangler;

import java.util.Map;

/**
 * Module that installs DataSource-class-specific {@link SegmentWrangler} implementations.
 */
public class SegmentWranglerModule implements Module
{
  /**
   * Default mappings of datasources to factories.
   */
  @VisibleForTesting
  static final Map<Class<? extends DataSource>, Class<? extends SegmentWrangler>> WRANGLER_MAPPINGS =
      ImmutableMap.of(
          InlineDataSource.class, InlineSegmentWrangler.class,
          LookupDataSource.class, LookupSegmentWrangler.class
      );

  @Override
  public void configure(Binder binder)
  {
    final MapBinder<Class<? extends DataSource>, SegmentWrangler> segmentWranglers =
        DruidBinders.segmentWranglerBinder(binder);

    WRANGLER_MAPPINGS.forEach((ds, wrangler) -> {
      segmentWranglers.addBinding(ds).to(wrangler);
      binder.bind(wrangler).in(LazySingleton.class);
    });

    binder.bind(SegmentWrangler.class).to(MapSegmentWrangler.class)
          .in(Scopes.SINGLETON);
  }
}
