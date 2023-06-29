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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.java.util.common.MapUtils;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class OmniDataSegmentKiller implements DataSegmentKiller
{
  private final Map<String, Supplier<DataSegmentKiller>> killers;

  @Inject
  public OmniDataSegmentKiller(
      Map<String, Provider<DataSegmentKiller>> killers
  )
  {
    this.killers = new HashMap<>();
    for (Map.Entry<String, Provider<DataSegmentKiller>> entry : killers.entrySet()) {
      String type = entry.getKey();
      Provider<DataSegmentKiller> provider = entry.getValue();
      this.killers.put(type, Suppliers.memoize(provider::get));
    }
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    DataSegmentKiller dataSegmentKiller = getKiller(segment);
    if (dataSegmentKiller != null) {
      dataSegmentKiller.kill(segment);
    }
  }

  @Nullable
  private DataSegmentKiller getKiller(DataSegment segment) throws SegmentLoadingException
  {
    if (segment.isTombstone()) {
      return null;
    }
    String type = MapUtils.getString(segment.getLoadSpec(), "type");
    Supplier<DataSegmentKiller> killer = killers.get(type);

    if (killer == null) {
      throw new SegmentLoadingException("Unknown segment killer type[%s].  Known types are %s", type, killers.keySet());
    }

    return killer.get();
  }

  @Override
  public void killAll()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @VisibleForTesting
  public Map<String, Supplier<DataSegmentKiller>> getKillers()
  {
    return killers;
  }
}
