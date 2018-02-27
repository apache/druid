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
   * @param replaceExisting overwrites existing objects if true, else leaves existing objects unchanged on conflict.
   *                        The behavior of the indexer determines whether this should be true or false. For example,
   *                        since Tranquility does not guarantee that replica tasks will generate indexes with the same
   *                        data, the first segment pushed should be favored since otherwise multiple historicals may
   *                        load segments with the same identifier but different contents which is a bad situation. On
   *                        the other hand, indexers that maintain exactly-once semantics by storing checkpoint data can
   *                        lose or repeat data if it fails to write a segment because it already exists and overwriting
   *                        is not permitted. This situation can occur if a task fails after pushing to deep storage but
   *                        before writing to the metadata storage, see: https://github.com/druid-io/druid/issues/5161.
   *
   *                        If replaceExisting is true, existing objects MUST be overwritten, since failure to do so
   *                        will break exactly-once semantics. If replaceExisting is false, existing objects SHOULD be
   *                        prioritized but it is acceptable if they are overwritten (deep storages may be eventually
   *                        consistent or otherwise unable to support transactional writes).
   * @return segment descriptor
   * @throws IOException
   */
  DataSegment push(File file, DataSegment segment, boolean replaceExisting) throws IOException;

  //use map instead of LoadSpec class to avoid dependency pollution.
  Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath);

  default String getStorageDir(DataSegment dataSegment)
  {
    return getDefaultStorageDir(dataSegment);
  }

  default String makeIndexPathName(DataSegment dataSegment, String indexName)
  {
    return StringUtils.format("./%s/%s", getStorageDir(dataSegment), indexName);
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
  static String getDefaultStorageDir(DataSegment segment)
  {
    return JOINER.join(
        segment.getDataSource(),
        StringUtils.format("%s_%s", segment.getInterval().getStart(), segment.getInterval().getEnd()),
        segment.getVersion(),
        segment.getShardSpec().getPartitionNum()
    );
  }
}
