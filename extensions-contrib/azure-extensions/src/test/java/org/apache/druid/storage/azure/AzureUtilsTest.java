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

package org.apache.druid.storage.azure;

import org.apache.druid.data.input.azure.AzureInputSource;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;

public class AzureUtilsTest
{
  private static final String CONTAINER_NAME = "container1";
  private static final String BLOB_NAME = "blob1";
  private static final String BLOB_PATH_WITH_LEADING_SLASH = "/" + BLOB_NAME;
  private static final String BLOB_PATH_WITH_LEADING_AZURE_PREFIX = AzureUtils.AZURE_STORAGE_HOST_ADDRESS
                                                                    + "/"
                                                                    + BLOB_NAME;
  private static final URI URI_WITH_PATH_WITH_LEADING_SLASH;

  static {
    try {
      URI_WITH_PATH_WITH_LEADING_SLASH = new URI(AzureInputSource.SCHEME
                                                 + "://"
                                                 + CONTAINER_NAME
                                                 + BLOB_PATH_WITH_LEADING_SLASH);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void test_extractAzureKey_pathHasLeadingSlash_returnsPathWithLeadingSlashRemoved()
  {
    String extractedKey = AzureUtils.extractAzureKey(URI_WITH_PATH_WITH_LEADING_SLASH);
    Assert.assertEquals(BLOB_NAME, extractedKey);
  }

  @Test
  public void test_maybeRemoveAzurePathPrefix_pathHasLeadingAzurePathPrefix_returnsPathWithLeadingAzurePathRemoved()
  {
    String path = AzureUtils.maybeRemoveAzurePathPrefix(BLOB_PATH_WITH_LEADING_AZURE_PREFIX);
    Assert.assertEquals(BLOB_NAME, path);
  }

  @Test
  public void test_maybeRemoveAzurePathPrefix_pathDoesNotHaveAzurePathPrefix__returnsPathWithLeadingAzurePathRemoved()
  {
    String path = AzureUtils.maybeRemoveAzurePathPrefix(BLOB_NAME);
    Assert.assertEquals(BLOB_NAME, path);
  }
}
