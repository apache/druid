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

package io.druid.storage.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentFinder;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GoogleDataSegmentFinder implements DataSegmentFinder
{
  private static final Logger LOG = new Logger(GoogleDataSegmentFinder.class);

  private final GoogleStorage storage;
  private final GoogleAccountConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public GoogleDataSegmentFinder(
      final GoogleStorage storage,
      final GoogleAccountConfig config,
      final ObjectMapper jsonMapper
  )
  {
    this.storage = storage;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Set<DataSegment> findSegments(String workingDirPath, boolean updateDescriptor) throws SegmentLoadingException
  {
    final Set<DataSegment> segments = Sets.newHashSet();

    try {
      Storage.Objects.List listObjects = storage.list(config.getBucket());
      listObjects.setPrefix(workingDirPath);
      Objects objects;
      do {
        objects = listObjects.execute();
        List<StorageObject> items = objects.getItems();
        if (items != null) {
          for (StorageObject item : items) {
            if (GoogleUtils.toFilename(item.getName()).equals("descriptor.json")) {
              final String descriptorJson = item.getName();
              final String indexZip = GoogleUtils.indexZipForSegmentPath(descriptorJson);

              if (storage.exists(item.getBucket(), indexZip)) {
                InputStream is = storage.get(item.getBucket(), item.getName());
                final DataSegment dataSegment = jsonMapper.readValue(is, DataSegment.class);

                LOG.info("Found segment [%s] located at [%s]", dataSegment.getIdentifier(), indexZip);

                Map<String, Object> loadSpec = dataSegment.getLoadSpec();

                if (!GoogleStorageDruidModule.SCHEME.equals(loadSpec.get("type")) ||
                    !indexZip.equals(loadSpec.get("path")) ||
                    !config.getBucket().equals(loadSpec.get("bucket"))) {
                  File descriptorFile = null;

                  try {
                    loadSpec.put("type", GoogleStorageDruidModule.SCHEME);
                    loadSpec.put("path", indexZip);
                    loadSpec.put("bucket", config.getBucket());
                    if (updateDescriptor) {
                      LOG.info(
                          "Updating loadSpec in descriptor.json at [%s] with new path [%s]",
                          descriptorJson,
                          indexZip
                      );

                      final byte[] bts = jsonMapper.writeValueAsBytes(dataSegment);
                      InputStreamContent mediaContent = new InputStreamContent("application/json", new ByteArrayInputStream(bts));
                      mediaContent.setLength(bts.length);

                      storage.insert(config.getBucket(), descriptorJson, mediaContent);
                    }
                  }
                  catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                  finally {
                    if (descriptorFile != null) {
                      LOG.info("Deleting file [%s]", descriptorFile);
                      descriptorFile.delete();
                    }
                  }
                }
                segments.add(dataSegment);
              } else {
                throw new SegmentLoadingException(
                    "index.zip didn't exist at [%s] while descriptor.json exists!?",
                    indexZip
                );
              }
            }
          }
        }
      } while (objects.getNextPageToken() != null);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "IO exception");
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, SegmentLoadingException.class);
      Throwables.propagate(e);
    }
    return segments;
  }
}
