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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObjectSummary;
import com.google.inject.Inject;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Pattern;

public class OssTimestampVersionedDataFinder extends OssDataSegmentPuller implements SearchableVersionedDataFinder<URI>
{
  @Inject
  public OssTimestampVersionedDataFinder(OSS client)
  {
    super(client);
  }

  /**
   * Gets the key with the most recently modified timestamp.
   * `pattern` is evaluated against the entire key AFTER the path given in `uri`.
   * The substring `pattern` is matched against will have a leading `/` removed.
   * For example `oss://some_bucket/some_prefix/some_key` with a URI of `oss://some_bucket/some_prefix` will match against `some_key`.
   * `oss://some_bucket/some_prefixsome_key` with a URI of `oss://some_bucket/some_prefix` will match against `some_key`
   * `oss://some_bucket/some_prefix//some_key` with a URI of `oss://some_bucket/some_prefix` will match against `/some_key`
   *
   * @param uri     The URI of in the form of `oss://some_bucket/some_key`
   * @param pattern The pattern matcher to determine if a *key* is of interest, or `null` to match everything.
   * @return A URI to the most recently modified object which matched the pattern.
   */
  @Override
  public URI getLatestVersion(final URI uri, final @Nullable Pattern pattern)
  {
    try {
      final CloudObjectLocation coords = new CloudObjectLocation(OssUtils.checkURI(uri));
      long mostRecent = Long.MIN_VALUE;
      URI latest = null;
      final Iterator<OSSObjectSummary> objectSummaryIterator = OssUtils.objectSummaryIterator(
          client,
          Collections.singletonList(uri),
          OssUtils.MAX_LISTING_LENGTH
      );
      while (objectSummaryIterator.hasNext()) {
        final OSSObjectSummary objectSummary = objectSummaryIterator.next();
        final CloudObjectLocation objectLocation = OssUtils.summaryToCloudObjectLocation(objectSummary);
        // remove coords path prefix from object path
        String keyString = StringUtils.maybeRemoveLeadingSlash(
            objectLocation.getPath().substring(coords.getPath().length())
        );
        if (pattern != null && !pattern.matcher(keyString).matches()) {
          continue;
        }
        final long latestModified = objectSummary.getLastModified().getTime();
        if (latestModified >= mostRecent) {
          mostRecent = latestModified;
          latest = objectLocation.toUri(OssStorageDruidModule.SCHEME);
        }
      }
      return latest;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
