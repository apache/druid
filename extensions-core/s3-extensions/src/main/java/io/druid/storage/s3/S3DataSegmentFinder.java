package io.druid.storage.s3;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.segment.loading.DataSegmentFinder;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.utils.ServiceUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3DataSegmentFinder implements DataSegmentFinder
{

  private static final Logger log = new Logger(S3DataSegmentFinder.class);

  private final RestS3Service s3Client;
  private final ObjectMapper mapper;

  @Inject
  public S3DataSegmentFinder(RestS3Service s3Client, ObjectMapper mapper)
  {
    this.s3Client = s3Client;
    this.mapper = mapper;
  }

  @Override
  public Set<DataSegment> findSegments(String workingPath, boolean updateDescriptor)
      throws SegmentLoadingException
  {

    final Matcher matcher = Pattern.compile("^s3://(.+?)/(.*)").matcher(workingPath);
    if (!matcher.matches() || matcher.groupCount() < 2) {
      throw new SegmentLoadingException(
          "S3 path [%s] must have following format s3://bucket/optional-path !!!",
          workingPath
      );
    }

    final String s3Bucket = matcher.group(1);
    final String s3Path = matcher.group(2);
    final Set<DataSegment> segments = Sets.newHashSet();

    try {
      List<S3Object> s3Listing = Arrays.asList(s3Client.listObjects(s3Bucket, s3Path, ""));

      if (s3Listing.size() < 1) {
        throw new SegmentLoadingException("Working path [%s] doesn't exist !!!", workingPath);
      }

      for (S3Object s3Obj : s3Listing) {
        if (s3Obj.getKey().endsWith("descriptor.json")) {
          final String descriptorKey = s3Obj.getKey();
          final String indexKey = descriptorKey.replace("descriptor.json", "index.zip");

          if (!s3Client.isObjectInBucket(s3Bucket, indexKey)) {
            throw new SegmentLoadingException(
                "index.zip doesn't exist at [%s] while descripter.json exists!?",
                indexKey
            );
          }

          final String segmentData = ServiceUtils.readInputStreamToString(s3Obj.getDataInputStream(), "UTF-8");
          final DataSegment dataSegment = mapper.readValue(segmentData, DataSegment.class);
          log.info("Found segment [%s] located at [%s]", dataSegment.getIdentifier(), indexKey);

          final Map<String, Object> loadSpec = dataSegment.getLoadSpec();

          if (updateDescriptor) {
            if (!loadSpec.get("type").equals(S3StorageDruidModule.SCHEME) ||
                !loadSpec.get("bucket").equals(s3Bucket) || !loadSpec.get("key").equals(indexKey)) {

              loadSpec.put("type", S3StorageDruidModule.SCHEME);
              loadSpec.put("bucket", s3Bucket);
              loadSpec.put("key", indexKey);
              log.info(
                  "Updating loadSpec in descriptor.json at [%s] with new key [%s]",
                  descriptorKey, indexKey
              );
              byte[] content = mapper.writeValueAsBytes(dataSegment);
              S3Object s3Object = new S3Object(descriptorKey, content);
              s3Client.putObject(s3Bucket, s3Object);
            }
          }
          segments.add(dataSegment);
        }
      }
    }
    catch (Exception e) {
      throw new SegmentLoadingException(e, "Problems interacting with s3 storage[%s].", workingPath);
    }
    return segments;
  }

}