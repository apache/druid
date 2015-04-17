/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.azure;

import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.microsoft.azure.storage.StorageException;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.net.URISyntaxException;
import java.util.Map;

public class AzureDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(AzureDataSegmentKiller.class);

  private final AzureStorageContainer azureStorageContainer;

  @Inject
  public AzureDataSegmentKiller(
      final AzureStorageContainer azureStorageContainer
  )
  {
    this.azureStorageContainer = azureStorageContainer;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    log.info("Killing segment [%s]", segment);

    Map<String, Object> loadSpec = segment.getLoadSpec();
    String storageDir = MapUtils.getString(loadSpec, "storageDir");

    try {
      azureStorageContainer.emptyCloudBlobDirectory(storageDir);
    }
    catch (StorageException | URISyntaxException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]", segment.getIdentifier());
    }
  }

}
