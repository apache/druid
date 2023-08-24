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

package org.apache.druid.data.input.s3;

import com.amazonaws.services.s3.internal.ServiceUtils;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.internal.TransferManagerUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.RetryingInputEntity;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3Entity extends RetryingInputEntity
{
  private static final Logger log = new Logger(S3Entity.class);

  private final ServerSideEncryptingAmazonS3 s3Client;
  private final CloudObjectLocation object;
  private final int maxRetries;
  private final File tempDir;
  private final TransferManager transferManager;


  S3Entity(ServerSideEncryptingAmazonS3 s3Client, CloudObjectLocation coords, File tempDir)
  {
    this(s3Client, coords, RetryUtils.DEFAULT_MAX_TRIES, tempDir);
  }

  // this was added for testing but it might be useful in other cases (you can
  // configure maxRetries...
  S3Entity(ServerSideEncryptingAmazonS3 s3Client, CloudObjectLocation coords, int maxRetries, File tempDir)
  {
    Preconditions.checkArgument(maxRetries >= 0);
    this.s3Client = s3Client;
    if (true) {
      log.info("Using transfer manager in S3Entity");
      this.transferManager = TransferManagerBuilder.standard()
                                                   .withS3Client(s3Client.getUnderlyingS3Client())
                                                   .build();
    } else {
      transferManager = null;
    }
    this.object = coords;
    this.maxRetries = maxRetries;
    this.tempDir = tempDir;
  }

  @Override
  protected int getMaxRetries()
  {
    return maxRetries;
  }

  @Override
  public URI getUri()
  {
    return object.toUri(S3StorageDruidModule.SCHEME);
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer) throws IOException
  {

    log.info("doing fetch on s3 file");
    if (!isUseTransferManager()) {
      return super.fetch(temporaryDirectory, fetchBuffer);
    }

    final GetObjectRequest request = new GetObjectRequest(object.getBucket(), object.getPath());

    File tempFile = new File(tempDir.getAbsolutePath(), UUID.randomUUID().toString());
    boolean parallelizable = TransferManagerUtils.isDownloadParallelizable(
        s3Client.getUnderlyingS3Client(),
        request,
        ServiceUtils.getPartCount(request, s3Client.getUnderlyingS3Client())
    );
    log.info("[%s] : [%s] download parallelizable ? [%s]", object.getBucket(), object.getPath(), parallelizable);
    DateTime start = DateTimes.nowUtc();
    Download download = transferManager.download(request, tempFile);
    try {
      download.waitForCompletion();
    }
    catch (InterruptedException e) {
      throw new RE(e);
    }

    log.info(
        "Loaded from [%s] to [%s] in [%d] ms",
        object.getPath(),
        tempFile.getPath(),
        new Interval(start, DateTimes.nowUtc()).toDurationMillis()
    );

    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return tempFile;
      }

      @Override
      public void close()
      {
        tempFile.delete();
      }
    };
  }

  @Override
  protected InputStream readFrom(long offset) throws IOException
  {
    final GetObjectRequest request = new GetObjectRequest(object.getBucket(), object.getPath());
    if (offset != 0) {
      request.setRange(offset);
    }
    try {
      if (!isUseTransferManager()) {
        final S3Object s3Object = s3Client.getObject(request);
        if (s3Object == null) {
          throw new ISE(
              "Failed to get an s3 object for bucket[%s], key[%s], and start[%d]",
              object.getBucket(),
              object.getPath(),
              offset
          );
        }
        return s3Object.getObjectContent();

      }
      File tempFile = new File(tempDir.getAbsolutePath(), UUID.randomUUID().toString());
      boolean parallelizable = TransferManagerUtils.isDownloadParallelizable(
          s3Client.getUnderlyingS3Client(),
          request,
          ServiceUtils.getPartCount(request, s3Client.getUnderlyingS3Client())
      );
      log.info("[%s] : [%s] download parallelizable ? [%s]", object.getBucket(), object.getPath(), parallelizable);
      DateTime start = DateTimes.nowUtc();
      Download download = transferManager.download(request, tempFile);
      try {
        download.waitForCompletion();
      }
      catch (InterruptedException e) {
        throw new RE(e);
      }

      log.info(
          "Loaded from [%s] to [%s] in [%d] ms",
          object.getPath(),
          tempFile.getPath(),
          new Interval(start, DateTimes.nowUtc()).toDurationMillis()
      );

      return new FileInputStream(tempFile)
      {
        final AtomicBoolean fisClosed = new AtomicBoolean(false);

        @Override
        public void close() throws IOException
        {
          if (fisClosed.get()) {
            return;
          }
          super.close();
          tempFile.delete();
          fisClosed.set(true);
        }
      };
    }
    catch (AmazonS3Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected String getPath()
  {
    return object.getPath();
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return S3Utils.S3RETRY;
  }

  private boolean isUseTransferManager()
  {
    return transferManager != null;
  }
}
