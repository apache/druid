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

package io.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentFinder;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class S3DataSegmentFinder implements DataSegmentFinder
{
  private static final Logger log = new Logger(S3DataSegmentFinder.class);

  private final AmazonS3 s3Client;
  private final ObjectMapper jsonMapper;
  private final S3DataSegmentPusherConfig config;

  @Inject
  public S3DataSegmentFinder(
      AmazonS3 s3Client,
      S3DataSegmentPusherConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.s3Client = s3Client;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Set<DataSegment> findSegments(String workingDirPath, boolean updateDescriptor) throws SegmentLoadingException
  {
    final Map<String, Pair<DataSegment, Long>> timestampedSegments = new HashMap<>();

    try {
      final Iterator<S3ObjectSummary> objectSummaryIterator = S3Utils.objectSummaryIterator(
          s3Client,
          config.getBucket(),
          workingDirPath.length() == 0 ? config.getBaseKey() : workingDirPath,
          config.getMaxListingLength()
      );

      while (objectSummaryIterator.hasNext()) {
        final S3ObjectSummary objectSummary = objectSummaryIterator.next();

        if (S3Utils.toFilename(objectSummary.getKey()).equals("descriptor.json")) {
          final String descriptorJson = objectSummary.getKey();
          String indexZip = S3Utils.indexZipForSegmentPath(descriptorJson);

          if (S3Utils.isObjectInBucketIgnoringPermission(s3Client, config.getBucket(), indexZip)) {
            try (S3Object indexObject = s3Client.getObject(config.getBucket(), descriptorJson);
                 S3ObjectInputStream is = indexObject.getObjectContent()) {
              final ObjectMetadata objectMetadata = indexObject.getObjectMetadata();
              final DataSegment dataSegment = jsonMapper.readValue(is, DataSegment.class);
              log.info("Found segment [%s] located at [%s]", dataSegment.getIdentifier(), indexZip);

              final Map<String, Object> loadSpec = dataSegment.getLoadSpec();
              if (!S3StorageDruidModule.SCHEME.equals(loadSpec.get("type")) ||
                  !indexZip.equals(loadSpec.get("key")) ||
                  !config.getBucket().equals(loadSpec.get("bucket"))) {
                loadSpec.put("type", S3StorageDruidModule.SCHEME);
                loadSpec.put("key", indexZip);
                loadSpec.put("bucket", config.getBucket());
                if (updateDescriptor) {
                  log.info(
                      "Updating loadSpec in descriptor.json at [%s] with new path [%s]",
                      descriptorJson,
                      indexObject
                  );
                  final ByteArrayInputStream bais = new ByteArrayInputStream(
                      StringUtils.toUtf8(jsonMapper.writeValueAsString(dataSegment))
                  );
                  s3Client.putObject(config.getBucket(), descriptorJson, bais, objectMetadata);
                }
              }

              DataSegmentFinder.putInMapRetainingNewest(
                  timestampedSegments,
                  dataSegment,
                  objectMetadata.getLastModified() == null ? 0 : objectMetadata.getLastModified().getTime()
              );
            }
          } else {
            throw new SegmentLoadingException(
                "index.zip didn't exist at [%s] while descriptor.json exists!?",
                indexZip
            );
          }
        }
      }
    }
    catch (AmazonServiceException e) {
      throw new SegmentLoadingException(e, "Problem interacting with S3");
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "IO exception");
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      Throwables.propagate(e);
    }
    return timestampedSegments.values().stream().map(x -> x.lhs).collect(Collectors.toSet());
  }
}
