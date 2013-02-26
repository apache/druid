package com.metamx.druid.loading;

import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;

import java.util.Collection;
import java.util.Map;

/**
 */
public class S3SegmentKiller implements SegmentKiller
{
  private static final Logger log = new Logger(S3SegmentKiller.class);

  private final RestS3Service s3Client;

  @Inject
  public S3SegmentKiller(
      RestS3Service s3Client
  )
  {
    this.s3Client = s3Client;
  }


  @Override
  public void kill(Collection<DataSegment> segments) throws ServiceException
  {
    for (final DataSegment segment : segments) {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      String s3Bucket = MapUtils.getString(loadSpec, "bucket");
      String s3Path = MapUtils.getString(loadSpec, "key");
      String s3DescriptorPath = s3Path.substring(0, s3Path.lastIndexOf("/")) + "/descriptor.json";

      if (s3Client.isObjectInBucket(s3Bucket, s3Path)) {
        log.info("Removing index file[s3://%s/%s] from s3!", s3Bucket, s3Path);
        s3Client.deleteObject(s3Bucket, s3Path);
      }
      if (s3Client.isObjectInBucket(s3Bucket, s3DescriptorPath)) {
        log.info("Removing descriptor file[s3://%s/%s] from s3!", s3Bucket, s3DescriptorPath);
        s3Client.deleteObject(s3Bucket, s3DescriptorPath);
      }
    }
  }
}
