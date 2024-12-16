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

package org.apache.druid.storage.s3;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.DataSegmentCopier;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

public class S3DataSegmentCopier extends S3DataSegmentTransferUtility implements DataSegmentCopier
{
  private static final Logger log = new Logger(S3DataSegmentCopier.class);

  @Inject
  public S3DataSegmentCopier(
      Supplier<ServerSideEncryptingAmazonS3> s3ClientSupplier,
      S3DataSegmentPusherConfig config
  )
  {
    super(config, s3ClientSupplier);
  }

  @Override
  public DataSegment copy(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
  {
    return this.transfer(segment, targetLoadSpec, false);
  }

  @Override
  protected Logger log()
  {
    return log;
  }
}
