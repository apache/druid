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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.RetryUtils;
import io.druid.data.SearchableVersionedDataFinder;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;

import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * This is implemented explicitly for URIExtractionNamespaceFunctionFactory
 * If you have a use case for this interface beyond URIExtractionNamespaceFunctionFactory please bring it up in the dev list.
 */
public class S3TimestampVersionedDataFinder extends S3DataSegmentPuller implements SearchableVersionedDataFinder<URI>
{
  @Inject
  public S3TimestampVersionedDataFinder(RestS3Service s3Client)
  {
    super(s3Client);
  }

  /**
   * Gets the latest version using the "parent" of uri as a prefix. The "parent" of uri is evaluated assuming '/'
   * delimited paths. If the uri path ends with '/', the path is assumed to be the parent.
   *
   * @param uri     The URI of interest whose "parent" will be searched as a key prefix for the latest version
   * @param pattern The pattern matcher to determine if a *key* is of interest. This will match against the entire key,
   *                not just the equivalent "filename" like some other implementations. A null value matches everything
   *
   * @return A URI to the most recently modified object which matched the pattern.
   */
  @Override
  public URI getLatestVersion(final URI uri, final Pattern pattern)
  {
    try {
      return RetryUtils.retry(
          new Callable<URI>()
          {
            @Override
            public URI call() throws Exception
            {
              final S3Coords coords = new S3Coords(checkURI(uri));
              long mostRecent = Long.MIN_VALUE;
              URI latest = null;
              String parentPath = coords.path.endsWith("/")
                                  ? coords.path
                                  : Paths.get(coords.path).getParent().toString();
              if (!parentPath.endsWith("/")) {
                parentPath = parentPath + "/";
              }
              S3Object[] objects = s3Client.listObjects(coords.bucket, parentPath, "/");
              if (objects == null) {
                return null;
              }
              for (S3Object storageObject : objects) {
                storageObject.closeDataInputStream();
                String keyString = storageObject.getKey().substring(parentPath.length());
                if (keyString.startsWith("/")) {
                  keyString = keyString.substring(1);
                }
                if (pattern != null && !pattern.matcher(keyString).matches()) {
                  continue;
                }
                final long latestModified = storageObject.getLastModifiedDate().getTime();
                if (latestModified >= mostRecent) {
                  mostRecent = latestModified;
                  latest = new URI(String.format("s3://%s/%s", storageObject.getBucketName(), storageObject.getKey()));
                }
              }
              return latest;
            }
          },
          shouldRetryPredicate(),
          DEFAULT_RETRY_COUNT
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Class<URI> getDataDescriptorClass()
  {
    return URI.class;
  }
}
