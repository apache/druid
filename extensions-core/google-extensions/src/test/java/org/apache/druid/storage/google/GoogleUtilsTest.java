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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GoogleUtilsTest
{
  @Test
  public void test429()
  {
    Assert.assertTrue(
        GoogleUtils.isRetryable(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(429, "ignored", new HttpHeaders()),
                null
            )
        )
    );
    Assert.assertTrue(
        GoogleUtils.isRetryable(
            new HttpResponseException.Builder(429, "ignored", new HttpHeaders()).build()
        )
    );
    Assert.assertTrue(
        GoogleUtils.isRetryable(
            new HttpResponseException.Builder(500, "ignored", new HttpHeaders()).build()
        )
    );
    Assert.assertTrue(
        GoogleUtils.isRetryable(
            new HttpResponseException.Builder(503, "ignored", new HttpHeaders()).build()
        )
    );
    Assert.assertTrue(
        GoogleUtils.isRetryable(
            new HttpResponseException.Builder(599, "ignored", new HttpHeaders()).build()
        )
    );
    Assert.assertFalse(
        GoogleUtils.isRetryable(
            new GoogleJsonResponseException(
                new HttpResponseException.Builder(404, "ignored", new HttpHeaders()),
                null
            )
        )
    );
    Assert.assertFalse(
        GoogleUtils.isRetryable(
            new HttpResponseException.Builder(404, "ignored", new HttpHeaders()).build()
        )
    );
    Assert.assertTrue(
        GoogleUtils.isRetryable(
            new IOException("generic io exception")
        )
    );
  }
}
