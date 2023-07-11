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

import java.util.HashMap;
import java.util.Map;

public class OmniDataSegmentArchiver implements DataSegmentArchiver
{
  private final Map<String, Supplier<DataSegmentArchiver>> archivers;

  @Inject
  public OmniDataSegmentArchiver(
      Map<String, Provider<DataSegmentArchiver>> archivers
  )
  {
    this.archivers = new HashMap<>();
    for (Map.Entry<String, Provider<DataSegmentArchiver>> entry : archivers.entrySet()) {
      String type = entry.getKey();
      Provider<DataSegmentArchiver> provider = entry.getValue();
      this.archivers.put(type, Suppliers.memoize(provider::get));
    }
  }

  @Override
  public DataSegment archive(DataSegment segment) throws SegmentLoadingException
  {
    return getArchiver(segment).archive(segment);
  }

  @Override
  public DataSegment restore(DataSegment segment) throws SegmentLoadingException
  {
    return getArchiver(segment).restore(segment);
  }

  private DataSegmentArchiver getArchiver(DataSegment segment) throws SegmentLoadingException
  {
    String type = MapUtils.getString(segment.getLoadSpec(), "type");
    Supplier<DataSegmentArchiver> archiver = archivers.get(type);

    if (archiver == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, archivers.keySet());
    }

    return archiver.get();
  }

  @VisibleForTesting
  public Map<String, Supplier<DataSegmentArchiver>> getArchivers()
  {
    return archivers;
  }
}
