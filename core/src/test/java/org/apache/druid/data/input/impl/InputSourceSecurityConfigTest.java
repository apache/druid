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

package org.apache.druid.data.input.impl;

import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;

public class InputSourceSecurityConfigTest
{
  @Test
  public void testEmptyAllowList()
  {
    InputSourceSecurityConfig config = new InputSourceSecurityConfig(Collections.emptyList(), null);
    Assert.assertFalse(config.isURIAllowed(URI.create("gs://my-bucket/abc")));
    Assert.assertFalse(config.isURIAllowed(URI.create("s3://my-bucket/abc")));
  }

  @Test
  public void testNonSpecifiedAllowDenyList()
  {
    InputSourceSecurityConfig config = new InputSourceSecurityConfig(null, null);
    Assert.assertTrue(config.isURIAllowed(URI.create("gs://my-bucket/abc")));
    Assert.assertTrue(config.isURIAllowed(URI.create("s3://my-bucket/abc")));
  }

  @Test
  public void testAllowList()
  {
    InputSourceSecurityConfig config = new InputSourceSecurityConfig(Collections.singletonList(URI.create(
        "gs://allowed-bucket/allowed-path")), null);
    Assert.assertTrue(config.isURIAllowed(URI.create("gs://allowed-bucket/allowed-path")));
    Assert.assertTrue(config.isURIAllowed(URI.create("gs://allowed-bucket/allowed-path/subsirectory")));

    Assert.assertFalse(config.isURIAllowed(URI.create("gs://allowed-bucket/deny-path")));
    Assert.assertFalse(config.isURIAllowed(URI.create("gs://deny-bucket/allow-path")));
    Assert.assertFalse(config.isURIAllowed(URI.create("gs://allow-bucket/allow-path/../deny-path")));

    Assert.assertFalse(config.isURIAllowed(URI.create("s3://allow-bucket/allow-path")));
  }

  @Test
  public void testDenyList()
  {
    InputSourceSecurityConfig config = new InputSourceSecurityConfig(
        null,
        Collections.singletonList(URI.create("gs://deny-bucket/deny-path"))
    );
    Assert.assertTrue(config.isURIAllowed(URI.create("gs://allowed-bucket/allowed-path")));
    Assert.assertTrue(config.isURIAllowed(URI.create("gs://allowed-bucket/allowed-path/subsirectory")));

    Assert.assertFalse(config.isURIAllowed(URI.create("gs://deny-bucket/deny-path")));
    Assert.assertTrue(config.isURIAllowed(URI.create("gs://deny-bucket/allow-path")));
    Assert.assertFalse(config.isURIAllowed(URI.create("gs://deny-bucket/allow-path/../deny-path")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCannotSetBothAllowAndDenyList()
  {
    new InputSourceSecurityConfig(
        Collections.singletonList(URI.create("http://allow.com")),
        Collections.singletonList(URI.create("http://deny.com"))
    );
  }
}
