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

package com.metamx.druid.common.s3;

import com.metamx.common.logger.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 *
 */
public class S3Utils
{
  private static final Logger log = new Logger(S3Utils.class);

  public static void putFileToS3(
      File localFile, RestS3Service s3Client, String outputS3Bucket, String outputS3Path
  )
      throws S3ServiceException, IOException, NoSuchAlgorithmException
  {
    S3Object s3Obj = new S3Object(localFile);
    s3Obj.setBucketName(outputS3Bucket);
    s3Obj.setKey(outputS3Path);

    log.info("Uploading file[%s] to [s3://%s/%s]", localFile, s3Obj.getBucketName(), s3Obj.getKey());
    s3Client.putObject(new S3Bucket(outputS3Bucket), s3Obj);
  }

  public static void putFileToS3WrapExceptions(
      File localFile, RestS3Service s3Client, String outputS3Bucket, String outputS3Path
  )
  {
    try {
      putFileToS3(localFile, s3Client, outputS3Bucket, outputS3Path);
    }
    catch (S3ServiceException e) {
      throw new RuntimeException(e);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static void closeStreamsQuietly(S3Object s3Obj)
  {
    if (s3Obj == null) {
      return;
    }

    try {
      s3Obj.closeDataInputStream();
    }
    catch (IOException e) {

    }
  }
}
