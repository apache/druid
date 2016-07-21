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

import com.aliyun.oss.model.OSSObject;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.metamx.common.RetryUtils;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * oss utils
 */
public class OssUtils {

    //  path joiner with /
    private static final Joiner JOINER = Joiner.on("/").skipNulls();

    //  the max retry times
    private static final int maxTries = 3;

    /**
     * close object stream
     *
     * @param ossObject
     */
    public static void closeStreamsQuietly(OSSObject ossObject) {

        if (ossObject != null && ossObject.getObjectContent() != null) {
            try {
                ossObject.getObjectContent().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * get if service exception can retry
     */
    public static final Predicate<Throwable> OssRETRY = new Predicate<Throwable>() {
        @Override
        public boolean apply(Throwable e) {
            if (e == null) {
                return false;
            } else if (e instanceof IOException) {
                return true;
            } else {
                return apply(e.getCause());
            }
        }
    };

    /**
     * Retries OSS operations that fail due to io-related exceptions. Service-level exceptions (access denied, file not found, etc) are not retried.
     *
     * @param f
     * @param <T>
     *
     * @return
     *
     * @throws Exception
     */
    public static <T> T retryOssOperation(Callable<T> f) throws Exception {

        return RetryUtils.retry(f, OssRETRY, maxTries);
    }

    /**
     * get data segment path
     *
     * @param baseKey
     * @param segment
     *
     * @return
     */
    public static String constructSegmentPath(String baseKey, DataSegment segment) {
        return JOINER.join(
                baseKey.isEmpty() ? null : baseKey,
                DataSegmentPusherUtil.getStorageDir(segment)
        ) + "/index.zip";
    }

    /**
     * get data segment descriptor path
     *
     * @param key
     *
     * @return
     */
    public static String descriptorPathForSegmentPath(String key) {
        return key.substring(0, key.lastIndexOf("/")) + "/descriptor.json";
    }
}