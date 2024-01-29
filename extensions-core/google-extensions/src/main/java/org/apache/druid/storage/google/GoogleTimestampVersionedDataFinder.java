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

package org.apache.druid.storage.google;

import com.google.inject.Inject;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

public class GoogleTimestampVersionedDataFinder extends GoogleDataSegmentPuller
    implements SearchableVersionedDataFinder<URI>
{
  private static final long MAX_LISTING_KEYS = 1000;

  @Inject
  public GoogleTimestampVersionedDataFinder(final GoogleStorage storage)
  {
    super(storage);
  }

  @Override
  public URI getLatestVersion(URI descriptorBase, @Nullable Pattern pattern)
  {
    try {
      long mostRecent = Long.MIN_VALUE;
      URI latest = null;
      final CloudObjectLocation baseLocation = new CloudObjectLocation(descriptorBase);
      final GoogleStorageObjectPage googleStorageObjectPage = storage.list(
          baseLocation.getBucket(),
          baseLocation.getPath(),
          MAX_LISTING_KEYS,
          null
      );
      for (GoogleStorageObjectMetadata objectMetadata : googleStorageObjectPage.getObjectList()) {
        if (GoogleUtils.isDirectoryPlaceholder(objectMetadata)) {
          continue;
        }
        // remove path prefix from file name
        final CloudObjectLocation objectLocation = new CloudObjectLocation(
            objectMetadata.getBucket(),
            objectMetadata.getName()
        );
        final String keyString = StringUtils
            .maybeRemoveLeadingSlash(objectMetadata.getName().substring(baseLocation.getPath().length()));
        if (pattern != null && !pattern.matcher(keyString).matches()) {
          continue;
        }
        final long latestModified = objectMetadata.getLastUpdateTime();
        if (latestModified >= mostRecent) {
          mostRecent = latestModified;
          latest = objectLocation.toUri(GoogleStorageDruidModule.SCHEME_GS);
        }
      }
      return latest;
    }
    catch (IOException e) {
      throw new RuntimeException();
    }
  }
}
