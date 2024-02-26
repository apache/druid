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

import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.druid.data.input.azure.AzureInputSource;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeoutException;

@RunWith(EasyMockRunner.class)
public class AzureUtilsTest extends EasyMockSupport
{
  private static final String CONTAINER_NAME = "container1";
  private static final String BLOB_NAME = "blob1";
  private static final String BLOB_PATH_WITH_LEADING_SLASH = "/" + BLOB_NAME;
  private static final String BLOB_PATH_WITH_LEADING_AZURE_PREFIX = AzureUtils.AZURE_STORAGE_HOST_ADDRESS
                                                                    + "/"
                                                                    + BLOB_NAME;
  private static final URI URI_WITH_PATH_WITH_LEADING_SLASH;

  private static final URISyntaxException URI_SYNTAX_EXCEPTION = new URISyntaxException("", "");

  private static final IOException IO_EXCEPTION = new IOException();
  private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException();
  private static final RuntimeException NULL_EXCEPTION_WRAPPED_IN_RUNTIME_EXCEPTION = new RuntimeException("", null);
  private static final RuntimeException IO_EXCEPTION_WRAPPED_IN_RUNTIME_EXCEPTION = new RuntimeException(
      "",
      new IOException()
  );
  private static final RuntimeException RUNTIME_EXCEPTION_WRAPPED_IN_RUNTIME_EXCEPTON = new RuntimeException(
      "",
      new RuntimeException()
  );

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

  @Mock
  private HttpResponse httpResponse;

  @Test
  public void test_extractAzureKey_pathHasLeadingSlash_returnsPathWithLeadingSlashRemoved()
  {
    String extractedKey = AzureUtils.extractAzureKey(URI_WITH_PATH_WITH_LEADING_SLASH);
    Assert.assertEquals(BLOB_NAME, extractedKey);
  }

  @Test
  public void test_maybeRemoveAzurePathPrefix_pathHasLeadingAzurePathPrefix_returnsPathWithLeadingAzurePathRemoved()
  {
    String path = AzureUtils.maybeRemoveAzurePathPrefix(BLOB_PATH_WITH_LEADING_AZURE_PREFIX, AzureUtils.AZURE_STORAGE_HOST_ADDRESS);
    Assert.assertEquals(BLOB_NAME, path);
  }

  @Test
  public void test_maybeRemoveAzurePathPrefix_pathDoesNotHaveAzurePathPrefix__returnsPathWithLeadingAzurePathRemoved()
  {
    String path = AzureUtils.maybeRemoveAzurePathPrefix(BLOB_NAME, AzureUtils.AZURE_STORAGE_HOST_ADDRESS);
    Assert.assertEquals(BLOB_NAME, path);
  }

  @Test
  public void test_azureRetry_URISyntaxException_returnsFalse()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(URI_SYNTAX_EXCEPTION);
    Assert.assertFalse(retry);
  }

  @Test
  public void test_azureRetry_StorageException_500ErrorCode_returnsTrue()
  {
    EasyMock.expect(httpResponse.getStatusCode()).andReturn(500).anyTimes();

    replayAll();
    BlobStorageException blobStorageException = new BlobStorageException("storage exception", httpResponse, null);
    boolean retry = AzureUtils.AZURE_RETRY.apply(blobStorageException);
    verifyAll();
    Assert.assertTrue(retry);
  }

  @Test
  public void test_azureRetry_StorageException_429ErrorCode_returnsTrue()
  {
    EasyMock.expect(httpResponse.getStatusCode()).andReturn(429).anyTimes();

    replayAll();
    BlobStorageException blobStorageException = new BlobStorageException("storage exception", httpResponse, null);
    boolean retry = AzureUtils.AZURE_RETRY.apply(blobStorageException);
    verifyAll();
    Assert.assertTrue(retry);
  }

  @Test
  public void test_azureRetry_StorageException_503ErrorCode_returnsTrue()
  {
    EasyMock.expect(httpResponse.getStatusCode()).andReturn(503).anyTimes();

    replayAll();
    BlobStorageException blobStorageException = new BlobStorageException("storage exception", httpResponse, null);
    boolean retry = AzureUtils.AZURE_RETRY.apply(blobStorageException);
    verifyAll();
    Assert.assertTrue(retry);
  }

  @Test
  public void test_azureRetry_StorageException_400ErrorCode_returnsFalse()
  {
    EasyMock.expect(httpResponse.getStatusCode()).andReturn(400).anyTimes();

    replayAll();
    BlobStorageException blobStorageException = new BlobStorageException("storage exception", httpResponse, null);
    boolean retry = AzureUtils.AZURE_RETRY.apply(blobStorageException);
    verifyAll();
    Assert.assertFalse(retry);
  }

  @Test
  public void test_azureRetry_nestedIOException_returnsTrue()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(new RuntimeException("runtime", new IOException("ioexception")));
    Assert.assertTrue(retry);
  }

  @Test
  public void test_azureRetry_nestedTimeoutException_returnsTrue()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(new RuntimeException("runtime", new TimeoutException("timeout exception")));
    Assert.assertTrue(retry);
  }

  @Test
  public void test_azureRetry_IOException_returnsTrue()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(IO_EXCEPTION);
    Assert.assertTrue(retry);
  }

  @Test
  public void test_azureRetry_nullException_returnsFalse()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(null);
    Assert.assertFalse(retry);
  }

  @Test
  public void test_azureRetry_RunTimeException_returnsFalse()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(RUNTIME_EXCEPTION);
    Assert.assertFalse(retry);
  }

  @Test
  public void test_azureRetry_nullExceptionWrappedInRunTimeException_returnsFalse()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(NULL_EXCEPTION_WRAPPED_IN_RUNTIME_EXCEPTION);
    Assert.assertFalse(retry);
  }

  @Test
  public void test_azureRetry_IOExceptionWrappedInRunTimeException_returnsTrue()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(IO_EXCEPTION_WRAPPED_IN_RUNTIME_EXCEPTION);
    Assert.assertTrue(retry);
  }

  @Test
  public void test_azureRetry_RunTimeExceptionWrappedInRunTimeException_returnsFalse()
  {
    boolean retry = AzureUtils.AZURE_RETRY.apply(RUNTIME_EXCEPTION_WRAPPED_IN_RUNTIME_EXCEPTON);
    Assert.assertFalse(retry);
  }

  @Test
  public void testRemoveAzurePathPrefixDefaultEndpoint()
  {
    String outputBlob = AzureUtils.maybeRemoveAzurePathPrefix("blob.core.windows.net/container/blob", "blob.core.windows.net");
    Assert.assertEquals("container/blob", outputBlob);
  }

  @Test
  public void testRemoveAzurePathPrefixCustomEndpoint()
  {
    String outputBlob = AzureUtils.maybeRemoveAzurePathPrefix("blob.core.usgovcloudapi.net/container/blob", "blob.core.usgovcloudapi.net");
    Assert.assertEquals("container/blob", outputBlob);
  }
}
