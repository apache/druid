/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.coordination.legacy;

import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;

import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.util.Map;

/**
 */
public class S3SizeLookup implements SizeLookup
{
  private static final Logger log = new Logger(S3SizeLookup.class);

  private final RestS3Service s3Client;

  public S3SizeLookup(
      RestS3Service s3Client
  )
  {
    this.s3Client = s3Client;
  }

  @Override
  public Long lookupSize(Map<String, Object> loadSpec)
  {
    String s3Bucket = MapUtils.getString(loadSpec, "bucket");
    String s3Path = MapUtils.getString(loadSpec, "key");

    S3Object s3Obj = null;
    try {
      s3Obj = s3Client.getObjectDetails(new S3Bucket(s3Bucket), s3Path);
    }
    catch (S3ServiceException e) {
      log.warn(e, "Exception when trying to lookup size for s3://%s/%s", s3Bucket, s3Path);
      return null;
    }

    if (s3Obj == null) {
      log.warn("s3Object for s3://%s/%s was null.", s3Bucket, s3Path);
      return null;
    }

    return s3Obj.getContentLength();
  }
}
