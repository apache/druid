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

package io.druid.storage.oss;

import com.aliyun.oss.OSSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.CompressionUtils;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

public class OssDataSegmentPusher implements DataSegmentPusher {

    private static final EmittingLogger log = new EmittingLogger(OssDataSegmentPusher.class);

    private final OSSClient ossClient;
    private final OssDataSegmentPusherConfig config;
    private final ObjectMapper jsonMapper;

    @Inject
    public OssDataSegmentPusher(OSSClient ossClient, OssDataSegmentPusherConfig config, ObjectMapper jsonMapper) {

        this.ossClient = ossClient;
        this.config = config;
        this.jsonMapper = jsonMapper;

        log.info("configured oss as deep storage, endpoint is [%s], bucket is [%s], basekey is [%s]",
                ossClient.getEndpoint(),
                config.getBucket(),
                config.getBaseKey()
        );
    }

    @Override
    public String getPathForHadoop() {
        return String.format("oss://%s/%s", config.getBucket(), config.getBaseKey());
    }

    @Deprecated
    @Override
    public String getPathForHadoop(String dataSource) {
        return getPathForHadoop();
    }

    @Override
    public DataSegment push(final File indexFilesDir, final DataSegment inSegment) throws IOException {

        //  get data full key
        final String key = OssUtils.constructSegmentPath(config.getBaseKey(), inSegment);

        log.info("copying segment [%s] to oss at location [%s] ...", inSegment.getIdentifier(), key);

        //  temp file for data zip
        final File zipOutFile = File.createTempFile("druid", "index.zip");

        //  write data to data zip
        final long indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);

        try {
            return OssUtils.retryOssOperation(
                    new Callable<DataSegment>() {
                        @Override
                        public DataSegment call() throws Exception {

                            log.info("pushing index file [%s] to oss [%s] ...", zipOutFile.getAbsolutePath(), key);

                            //  push index file to oss
                            ossClient.putObject(config.getBucket(), key, zipOutFile);

                            //  return data segment, include type/bucket/key info
                            final DataSegment outSegment = inSegment.withSize(indexSize)
                                    .withLoadSpec(
                                            ImmutableMap.<String, Object>of(
                                                    "type",
                                                    "oss",
                                                    "bucket",
                                                    config.getBucket(),
                                                    "key",
                                                    key
                                            )
                                    )
                                    .withBinaryVersion(SegmentUtils.getVersionFromDir(indexFilesDir));

                            //  get descriptor file from insegment
                            File descriptorFile = File.createTempFile("druid", "descriptor.json");
                            Files.write(jsonMapper.writeValueAsBytes(inSegment), descriptorFile);

                            //  push descriptor file to oss
                            String descKey = OssUtils.descriptorPathForSegmentPath(key);
                            log.info("pushing descriptor file [%s] to oss [%s] ...", descriptorFile.getAbsolutePath(), descKey);
                            ossClient.putObject(config.getBucket(), descKey, descriptorFile);

                            //  delete data index and descriptor file
                            log.info("deleting index file [%s] ...", zipOutFile);
                            zipOutFile.delete();

                            log.info("deleting descriptor file [%s] ...", descriptorFile);
                            descriptorFile.delete();

                            return outSegment;
                        }
                    }
            );
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}