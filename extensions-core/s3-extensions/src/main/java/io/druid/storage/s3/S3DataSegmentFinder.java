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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentFinder;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class S3DataSegmentFinder implements DataSegmentFinder
{
  private static final Logger log = new Logger(S3DataSegmentFinder.class);

  private final RestS3Service s3Client;
  private final ObjectMapper jsonMapper;
  private final S3DataSegmentPusherConfig config;

  @Inject
  public S3DataSegmentFinder(
      RestS3Service s3Client,
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
    final Set<DataSegment> segments = Sets.newHashSet();

    try {
      Iterator<StorageObject> objectsIterator = S3Utils.storageObjectsIterator(
          s3Client,
          config.getBucket(),
          workingDirPath.length() == 0 ? config.getBaseKey() : workingDirPath,
          config.getMaxListingLength());

      while(objectsIterator.hasNext()) {
        StorageObject storageObject = objectsIterator.next();
        storageObject.closeDataInputStream();

        if (S3Utils.toFilename(storageObject.getKey()).equals("descriptor.json")) {
          final String descriptorJson = storageObject.getKey();
          String indexZip = S3Utils.indexZipForSegmentPath(descriptorJson);

          if (S3Utils.isObjectInBucket(s3Client, config.getBucket(), indexZip)) {
            S3Object indexObject = s3Client.getObject(config.getBucket(), descriptorJson);

            try (InputStream is = indexObject.getDataInputStream()) {
              final DataSegment dataSegment = jsonMapper.readValue(is, DataSegment.class);
              log.info("Found segment [%s] located at [%s]", dataSegment.getIdentifier(), indexZip);

              final Map<String, Object> loadSpec = dataSegment.getLoadSpec();
              if (!loadSpec.get("type").equals(S3StorageDruidModule.SCHEME) || !loadSpec.get("key").equals(indexZip)) {
                loadSpec.put("type", S3StorageDruidModule.SCHEME);
                loadSpec.put("key", indexZip);
                if (updateDescriptor) {
                  log.info(
                      "Updating loadSpec in descriptor.json at [%s] with new path [%s]",
                      descriptorJson,
                      indexObject
                  );
                  S3Object newDescJsonObject = new S3Object(descriptorJson, jsonMapper.writeValueAsString(dataSegment));
                  s3Client.putObject(config.getBucket(), newDescJsonObject);
                }
              }
              segments.add(dataSegment);
            }
          } else {
            throw new SegmentLoadingException(
                "index.zip didn't exist at [%s] while descriptor.json exists!?",
                indexZip
            );
          }
        }
      }
    } catch (ServiceException e) {
      throw new SegmentLoadingException(e, "Problem interacting with S3");
    } catch (IOException e) {
      throw new SegmentLoadingException(e, "IO exception");
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      Throwables.propagate(e);
    }
    return segments;
  }
}
