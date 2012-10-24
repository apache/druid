package com.metamx.druid.loading;

import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.StreamUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.common.s3.S3Utils;
import org.apache.commons.io.FileUtils;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 */
public class S3SegmentGetter implements SegmentGetter
{
  private static final Logger log = new Logger(S3SegmentGetter.class);
  private static final long DEFAULT_TIMEOUT = 5 * 60 * 1000;

  private static final String BUCKET = "bucket";
  private static final String KEY = "key";

  private final RestS3Service s3Client;
  private final S3SegmentGetterConfig config;

  @Inject
  public S3SegmentGetter(
      RestS3Service s3Client,
      S3SegmentGetterConfig config
  )
  {
    this.s3Client = s3Client;
    this.config = config;
  }

  @Override
  public File getSegmentFiles(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    String s3Bucket = MapUtils.getString(loadSpec, "bucket");
    String s3Path = MapUtils.getString(loadSpec, "key");

    log.info("Loading index at path[s3://%s/%s]", s3Bucket, s3Path);

    S3Object s3Obj = null;
    File tmpFile = null;
    try {
      if (!s3Client.isObjectInBucket(s3Bucket, s3Path)) {
        throw new StorageAdapterLoadingException("IndexFile[s3://%s/%s] does not exist.", s3Bucket, s3Path);
      }

      File cacheFile = new File(config.getCacheDirectory(), computeCacheFilePath(s3Bucket, s3Path));

      if (cacheFile.exists()) {
        S3Object objDetails = s3Client.getObjectDetails(new S3Bucket(s3Bucket), s3Path);
        DateTime cacheFileLastModified = new DateTime(cacheFile.lastModified());
        DateTime s3ObjLastModified = new DateTime(objDetails.getLastModifiedDate().getTime());
        if (cacheFileLastModified.isAfter(s3ObjLastModified)) {
          log.info(
              "Found cacheFile[%s] with modified[%s], which is after s3Obj[%s].  Using.",
              cacheFile,
              cacheFileLastModified,
              s3ObjLastModified
          );
          return cacheFile.getParentFile();
        }
        FileUtils.deleteDirectory(cacheFile.getParentFile());
      }

      long currTime = System.currentTimeMillis();

      tmpFile = File.createTempFile(s3Bucket, new DateTime().toString());
      log.info(
          "Downloading file[s3://%s/%s] to local tmpFile[%s] for cacheFile[%s]",
          s3Bucket, s3Path, tmpFile, cacheFile
      );

      s3Obj = s3Client.getObject(new S3Bucket(s3Bucket), s3Path);
      StreamUtils.copyToFileAndClose(s3Obj.getDataInputStream(), tmpFile, DEFAULT_TIMEOUT);
      final long downloadEndTime = System.currentTimeMillis();
      log.info("Download of file[%s] completed in %,d millis", cacheFile, downloadEndTime - currTime);

      if (!cacheFile.getParentFile().mkdirs()) {
        log.info("Unable to make parent file[%s]", cacheFile.getParentFile());
      }
      cacheFile.delete();

      if (s3Path.endsWith("gz")) {
        log.info("Decompressing file[%s] to [%s]", tmpFile, cacheFile);
        StreamUtils.copyToFileAndClose(
            new GZIPInputStream(new FileInputStream(tmpFile)),
            cacheFile
        );
        if (!tmpFile.delete()) {
          log.error("Could not delete tmpFile[%s].", tmpFile);
        }
      } else {
        log.info("Rename tmpFile[%s] to cacheFile[%s]", tmpFile, cacheFile);
        if (!tmpFile.renameTo(cacheFile)) {
          log.warn("Error renaming tmpFile[%s] to cacheFile[%s].  Copying instead.", tmpFile, cacheFile);

          StreamUtils.copyToFileAndClose(new FileInputStream(tmpFile), cacheFile);
          if (!tmpFile.delete()) {
            log.error("Could not delete tmpFile[%s].", tmpFile);
          }
        }
      }

      long endTime = System.currentTimeMillis();
      log.info("Local processing of file[%s] done in %,d millis", cacheFile, endTime - downloadEndTime);

      return cacheFile.getParentFile();
    }
    catch (Exception e) {
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }
    finally {
      S3Utils.closeStreamsQuietly(s3Obj);
      if (tmpFile != null && tmpFile.exists()) {
        log.warn("Deleting tmpFile[%s] in finally block.  Why?", tmpFile);
        tmpFile.delete();
      }
    }
  }

  private String computeCacheFilePath(String s3Bucket, String s3Path)
  {
    return String.format(
        "%s/%s", s3Bucket, s3Path.endsWith("gz") ? s3Path.substring(0, s3Path.length() - ".gz".length()) : s3Path
    );
  }

  @Override
  public boolean cleanSegmentFiles(Map<String, Object> loadSpec) throws StorageAdapterLoadingException
  {
    File cacheFile = new File(
        config.getCacheDirectory(),
        computeCacheFilePath(MapUtils.getString(loadSpec, BUCKET), MapUtils.getString(loadSpec, KEY))
    );

    try {
      final File parentFile = cacheFile.getParentFile();
      log.info("Recursively deleting file[%s]", parentFile);
      FileUtils.deleteDirectory(parentFile);
    }
    catch (IOException e) {
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }

    return true;
  }
}
