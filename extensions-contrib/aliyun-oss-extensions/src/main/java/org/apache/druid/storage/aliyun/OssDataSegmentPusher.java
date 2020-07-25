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
import com.aliyun.oss.OSSException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class OssDataSegmentPusher implements DataSegmentPusher
{
  private static final EmittingLogger log = new EmittingLogger(OssDataSegmentPusher.class);

  private final OSS client;
  private final OssStorageConfig config;

  @Inject
  public OssDataSegmentPusher(
      OSS client,
      OssStorageConfig config
  )
  {
    this.client = client;
    this.config = config;
  }

  @Override
  public String getPathForHadoop()
  {
    return StringUtils.format("%s/%s", config.getBucket(), config.getPrefix());
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public List<String> getAllowedPropertyPrefixesForHadoop()
  {
    return ImmutableList.of("druid.oss");
  }

  @Override
  public DataSegment push(final File indexFilesDir, final DataSegment inSegment, final boolean useUniquePath)
      throws IOException
  {
    final String path = OssUtils.constructSegmentPath(config.getPrefix(), getStorageDir(inSegment, useUniquePath));

    log.debug("Copying segment[%s] to OSS at location[%s]", inSegment.getId(), path);

    final File zipOutFile = File.createTempFile("druid", "index.zip");
    final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

    final DataSegment outSegment = inSegment.withSize(indexSize)
                                            .withLoadSpec(makeLoadSpec(config.getBucket(), path))
                                            .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

    try {
      return OssUtils.retry(
          () -> {
            OssUtils.uploadFileIfPossible(client, config.getBucket(), path, zipOutFile);

            return outSegment;
          }
      );
    }
    catch (OSSException e) {
      throw new IOException(e);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      log.debug("Deleting temporary cached index.zip");
      zipOutFile.delete();
    }
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI finalIndexZipFilePath)
  {
    // remove the leading "/"
    return makeLoadSpec(finalIndexZipFilePath.getHost(), finalIndexZipFilePath.getPath().substring(1));
  }

  private Map<String, Object> makeLoadSpec(String bucket, String key)
  {
    return ImmutableMap.of(
        "type",
        OssStorageDruidModule.SCHEME_ZIP,
        "bucket",
        bucket,
        "key",
        key
    );
  }

}
