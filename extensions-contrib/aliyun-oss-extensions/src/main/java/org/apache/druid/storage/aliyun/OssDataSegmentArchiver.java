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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

public class OssDataSegmentArchiver extends OssDataSegmentMover implements DataSegmentArchiver
{
  private final OssDataSegmentArchiverConfig archiveConfig;
  private final OssStorageConfig restoreConfig;
  private final ObjectMapper mapper;

  @Inject
  public OssDataSegmentArchiver(
      @Json ObjectMapper mapper,
      OSS client,
      OssDataSegmentArchiverConfig archiveConfig,
      OssStorageConfig restoreConfig
  )
  {
    super(client, restoreConfig);
    this.mapper = mapper;
    this.archiveConfig = archiveConfig;
    this.restoreConfig = restoreConfig;
  }

  @Override
  public DataSegment archive(DataSegment segment) throws SegmentLoadingException
  {
    String targetBucket = archiveConfig.getArchiveBucket();
    String targetKey = archiveConfig.getArchiveBaseKey();

    final DataSegment archived = move(
        segment,
        ImmutableMap.of(
            "bucket", targetBucket,
            "baseKey", targetKey
        )
    );
    if (sameLoadSpec(segment, archived)) {
      return null;
    }
    return archived;
  }

  @Override
  public DataSegment restore(DataSegment segment) throws SegmentLoadingException
  {
    String targetBucket = restoreConfig.getBucket();
    String targetKey = restoreConfig.getPrefix();

    final DataSegment restored = move(
        segment,
        ImmutableMap.of(
            "bucket", targetBucket,
            "baseKey", targetKey
        )
    );

    if (sameLoadSpec(segment, restored)) {
      return null;
    }
    return restored;
  }

  boolean sameLoadSpec(DataSegment s1, DataSegment s2)
  {
    final OssLoadSpec s1LoadSpec = (OssLoadSpec) mapper.convertValue(s1.getLoadSpec(), LoadSpec.class);
    final OssLoadSpec s2LoadSpec = (OssLoadSpec) mapper.convertValue(s2.getLoadSpec(), LoadSpec.class);
    return Objects.equal(s1LoadSpec.getBucket(), s2LoadSpec.getBucket()) && Objects.equal(
        s1LoadSpec.getKey(),
        s2LoadSpec.getKey()
    );
  }
}
