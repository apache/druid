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
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

/**
 * Provides task logs archived on OSS.
 */
public class OssTaskLogs implements TaskLogs {
    private static final Logger log = new Logger(OssTaskLogs.class);

    private final OSSClient ossClient;
    private final OssTaskLogsConfig config;

    @Inject
    public OssTaskLogs(OssTaskLogsConfig config, OSSClient ossClient) {
        this.config = config;
        this.ossClient = ossClient;
    }

    @Override
    public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) {

        final String taskKey = getTaskLogKey(taskid);

        final OSSObject ossObject = ossClient.getObject(config.getLogBucket(), taskKey);

        return Optional.<ByteSource>of(
                new ByteSource() {
                    @Override
                    public InputStream openStream() throws IOException {
                        try {
                            final long start;
                            final long end = ossObject.getObjectMetadata().getContentLength() - 1;

                            if (offset > 0 && offset < ossObject.getObjectMetadata().getContentLength()) {
                                start = offset;
                            } else if (offset < 0 && (-1 * offset) < ossObject.getObjectMetadata().getContentLength()) {
                                start = ossObject.getObjectMetadata().getContentLength() + offset;
                            } else {
                                start = 0;
                            }

                            GetObjectRequest gor = new GetObjectRequest(config.getLogBucket(), taskKey);
                            gor.setRange(start, end);

                            return ossClient.getObject(gor).getObjectContent();
                        } catch (Exception e) {
                            throw new IOException(e);
                        }
                    }
                }
        );
    }

    public void pushTaskLog(final String taskid, final File logFile) throws IOException {
        final String taskKey = getTaskLogKey(taskid);
        log.info("Pushing task log %s to: %s", logFile, taskKey);

        try {
            OssUtils.retryOssOperation(
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            ossClient.putObject(config.getLogBucket(), taskKey, logFile);
                            return null;
                        }
                    }
            );
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, IOException.class);
            throw Throwables.propagate(e);
        }
    }

    private String getTaskLogKey(String taskid) {
        return String.format("%s/%s/log", config.getLogBucket(), taskid);
    }
}
