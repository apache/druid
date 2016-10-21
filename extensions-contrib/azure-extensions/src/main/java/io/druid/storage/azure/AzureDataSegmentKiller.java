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

package io.druid.storage.azure;

import com.google.inject.Inject;
import com.microsoft.azure.storage.StorageException;

import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

public class AzureDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(AzureDataSegmentKiller.class);

  private final AzureStorage azureStorage;

  @Inject
  public AzureDataSegmentKiller(
      final AzureStorage azureStorage
  )
  {
    this.azureStorage = azureStorage;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    log.info("Killing segment [%s]", segment);

    Map<String, Object> loadSpec = segment.getLoadSpec();
    final String containerName = MapUtils.getString(loadSpec, "containerName");
    final String blobPath = MapUtils.getString(loadSpec, "blobPath");
    final String dirPath = Paths.get(blobPath).getParent().toString();

    try {
      azureStorage.emptyCloudBlobDirectory(containerName, dirPath);
    }
    catch(StorageException e){
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getIdentifier(), e.getExtendedErrorInformation() == null ? null : e.getExtendedErrorInformation().getErrorMessage());
    }
    catch (URISyntaxException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getIdentifier(), e.getReason());
    }
  }

}
