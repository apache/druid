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

package io.druid.segment.loading;

import com.google.inject.Inject;

import io.druid.java.util.common.MapUtils;
import io.druid.timeline.DataSegment;

import java.util.Map;

/**
 */
public class OmniDataSegmentKiller implements DataSegmentKiller
{
  private final Map<String, DataSegmentKiller> killers;

  @Inject
  public OmniDataSegmentKiller(
      Map<String, DataSegmentKiller> killers
  )
  {
    this.killers = killers;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    getKiller(segment).kill(segment);
  }

  private DataSegmentKiller getKiller(DataSegment segment) throws SegmentLoadingException
  {
    String type = MapUtils.getString(segment.getLoadSpec(), "type");
    DataSegmentKiller loader = killers.get(type);

    if (loader == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, killers.keySet());
    }

    return loader;
  }

}
