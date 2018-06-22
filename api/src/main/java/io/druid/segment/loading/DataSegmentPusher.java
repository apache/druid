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

import com.google.common.base.Joiner;
import io.druid.guice.annotations.ExtensionPoint;
import io.druid.java.util.common.StringUtils;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ExtensionPoint
public interface DataSegmentPusher
{
  Joiner JOINER = Joiner.on("/").skipNulls();

  @Deprecated
  String getPathForHadoop(String dataSource);
  String getPathForHadoop();

  /**
   * Pushes index files and segment descriptor to deep storage.
   * @param file directory containing index files
   * @param segment segment descriptor
   * @param useUniquePath if true, pushes to a unique file path. This prevents situations where task failures or replica
   *                      tasks can either overwrite or fail to overwrite existing segments leading to the possibility
   *                      of different versions of the same segment ID containing different data. As an example, a Kafka
   *                      indexing task starting at offset A and ending at offset B may push a segment to deep storage
   *                      and then fail before writing the loadSpec to the metadata table, resulting in a replacement
   *                      task being spawned. This replacement will also start at offset A but will read to offset C and
   *                      will then push a segment to deep storage and write the loadSpec metadata. Without unique file
   *                      paths, this can only work correctly if new segments overwrite existing segments. Suppose that
   *                      at this point the task then fails so that the supervisor retries again from offset A. This 3rd
   *                      attempt will overwrite the segments in deep storage before failing to write the loadSpec
   *                      metadata, resulting in inconsistencies in the segment data now in deep storage and copies of
   *                      the segment already loaded by historicals.
   *
   *                      If unique paths are used, caller is responsible for cleaning up segments that were pushed but
   *                      were not written to the metadata table (for example when using replica tasks).
   * @return segment descriptor
   * @throws IOException
   */
  DataSegment push(File file, DataSegment segment, boolean useUniquePath) throws IOException;

  //use map instead of LoadSpec class to avoid dependency pollution.
  Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath);

  /**
   * @deprecated backward-compatibiliy shim that should be removed on next major release;
   * use {@link #getStorageDir(DataSegment, boolean)} instead.
   */
  @Deprecated
  default String getStorageDir(DataSegment dataSegment)
  {
    return getStorageDir(dataSegment, false);
  }

  default String getStorageDir(DataSegment dataSegment, boolean useUniquePath)
  {
    return getDefaultStorageDir(dataSegment, useUniquePath);
  }

  default String makeIndexPathName(DataSegment dataSegment, String indexName)
  {
    // This is only called from Hadoop batch which doesn't require unique segment paths so set useUniquePath=false
    return StringUtils.format("./%s/%s", getStorageDir(dataSegment, false), indexName);
  }

  /**
   * Property prefixes that should be added to the "allowedHadoopPrefix" config for passing down to Hadoop jobs. These
   * should be property prefixes like "druid.xxx", which means to include "druid.xxx" and "druid.xxx.*".
   */
  default List<String> getAllowedPropertyPrefixesForHadoop()
  {
    return Collections.emptyList();
  }

  // Note: storage directory structure format = .../dataSource/interval/version/partitionNumber/
  // If above format is ever changed, make sure to change it appropriately in other places
  // e.g. HDFSDataSegmentKiller uses this information to clean the version, interval and dataSource directories
  // on segment deletion if segment being deleted was the only segment
  static String getDefaultStorageDir(DataSegment segment, boolean useUniquePath)
  {
    return JOINER.join(
        segment.getDataSource(),
        StringUtils.format("%s_%s", segment.getInterval().getStart(), segment.getInterval().getEnd()),
        segment.getVersion(),
        segment.getShardSpec().getPartitionNum(),
        useUniquePath ? generateUniquePath() : null
    );
  }

  static String generateUniquePath()
  {
    return UUID.randomUUID().toString();
  }
}
