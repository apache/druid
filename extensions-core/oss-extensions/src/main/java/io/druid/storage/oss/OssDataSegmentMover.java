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
import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.util.Map;
import java.util.concurrent.Callable;

public class OssDataSegmentMover implements DataSegmentMover {

    private static final Logger log = new Logger(OssDataSegmentMover.class);

    private final OSSClient ossClient;
    private final OssDataSegmentPusherConfig config;

    @Inject
    public OssDataSegmentMover(OSSClient ossClient, OssDataSegmentPusherConfig config) {
        this.ossClient = ossClient;
        this.config = config;
    }

    @Override
    public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException {

        Map<String, Object> loadSpec = segment.getLoadSpec();
        String srcBucket = MapUtils.getString(loadSpec, "bucket");
        String srcKey = MapUtils.getString(loadSpec, "key");
        String srcDescPath = OssUtils.descriptorPathForSegmentPath(srcKey);

        final String dstBucket = MapUtils.getString(targetLoadSpec, "bucket");
        final String dstKey = MapUtils.getString(targetLoadSpec, "baseKey");
        final String targetOssPath = OssUtils.constructSegmentPath(dstKey, segment);
        String targetOssDescriptorPath = OssUtils.descriptorPathForSegmentPath(targetOssPath);

        if (dstBucket.isEmpty()) {
            throw new SegmentLoadingException("target oss bucket is not specified");
        }
        if (targetOssPath.isEmpty()) {
            throw new SegmentLoadingException("target oss key is not specified");
        }

        safeMove(srcBucket, srcKey, dstBucket, targetOssPath);
        safeMove(srcBucket, srcDescPath, dstBucket, targetOssDescriptorPath);

        return segment.withLoadSpec(
                ImmutableMap.<String, Object>builder()
                        .putAll(
                                Maps.filterKeys(
                                        loadSpec, new Predicate<String>() {
                                            @Override
                                            public boolean apply(String input) {
                                                return !(input.equals("bucket") || input.equals("key"));
                                            }
                                        }
                                )
                        )
                        .put("bucket", dstBucket)
                        .put("key", targetOssPath)
                        .build()
        );
    }

    private void safeMove(final String srcBucket, final String srcKey, final String dstBucket, final String dstKey) throws SegmentLoadingException {

        try {
            OssUtils.retryOssOperation(
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {

                            final String movePath = StringUtils.safeFormat(
                                    "[%s/%s] to [%s/%s]",
                                    srcBucket,
                                    srcKey,
                                    dstBucket,
                                    dstKey
                            );

                            if (srcBucket.equals(dstBucket) && srcKey.equals(dstKey)) {
                                log.info("no need to move file[%s/%s] into itself", srcBucket, srcKey);
                                return null;
                            }

                            if (ossClient.doesObjectExist(srcBucket, srcKey)) {

                                final OSSObject ossObject = ossClient.getObject(srcBucket, srcKey);

                                if (ossObject == null) {
                                    throw new ISE("unable to get object [%s/%s]", srcBucket, srcKey);
                                }

                                log.info("moving file %s", movePath);

                                //  copy object to dst bucket
                                ossClient.copyObject(srcBucket, srcKey, dstBucket, dstKey);

                                //  if dst exists object then delete src object
                                if (ossClient.doesObjectExist(dstBucket, dstKey)) {
                                    ossClient.deleteObject(srcBucket, srcKey);
                                } else {
                                    throw new SegmentLoadingException(
                                            "unable to copy file %s, will retry", movePath
                                    );
                                }
                            } else {
                                // ensure object exists in target location
                                if (ossClient.doesObjectExist(dstBucket, dstKey)) {
                                    log.info(
                                            "not moving file [%s/%s], already present in target location [%s/%s]",
                                            srcBucket,
                                            srcKey,
                                            dstBucket,
                                            dstKey
                                    );
                                } else {
                                    throw new SegmentLoadingException(
                                            "unable to move file %s, not present in either source or target location", movePath
                                    );
                                }
                            }
                            return null;
                        }
                    }
            );
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
            throw Throwables.propagate(e);
        }
    }
}