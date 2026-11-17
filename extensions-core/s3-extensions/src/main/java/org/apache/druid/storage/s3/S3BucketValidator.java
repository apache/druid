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

import org.apache.druid.data.input.impl.CloudBucketValidator;
import org.apache.druid.java.util.common.IAE;

/**
 * Accepts plain bucket names and S3 Access Point / Multi-Region Access Point ARNs in colon form.
 *
 * {@link org.apache.druid.data.input.impl.CloudObjectLocation} permits ':' in any bucket, so this rejects colons
 * outside a well-formed ARN. That also catches slash-form ARNs inside a URI, such as
 * s3://arn:aws:s3::&lt;account&gt;:accesspoint/alias.mrap/key, where {@link java.net.URI} truncates the bucket to
 * arn:aws:s3::&lt;account&gt;:accesspoint and moves the access point name into the key.
 */
public class S3BucketValidator implements CloudBucketValidator
{
  public static final S3BucketValidator INSTANCE = new S3BucketValidator();

  private S3BucketValidator()
  {
  }

  @Override
  public void validate(String bucket)
  {
    if (bucket.indexOf(':') >= 0 && !S3Utils.isS3Arn(bucket)) {
      throw new IAE(
          "Invalid S3 bucket [%s]. Use a bucket name, or an S3 Access Point or Multi-Region Access Point ARN"
          + " in colon form: arn:<partition>:s3:<region>:<account>:accesspoint:<name>",
          bucket
      );
    }
  }
}
