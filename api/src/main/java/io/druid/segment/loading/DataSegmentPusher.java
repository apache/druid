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
import io.druid.java.util.common.StringUtils;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface DataSegmentPusher
{
  Joiner JOINER = Joiner.on("/").skipNulls();

  @Deprecated
  String getPathForHadoop(String dataSource);
  String getPathForHadoop();
  DataSegment push(File file, DataSegment segment) throws IOException;
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
