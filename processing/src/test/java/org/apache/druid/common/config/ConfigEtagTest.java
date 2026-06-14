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

package org.apache.druid.common.config;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class ConfigEtagTest
{
  @Test
  public void testComputeIsDeterministic()
  {
    byte[] bytes = "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8);
    Assert.assertEquals(ConfigEtag.compute(bytes), ConfigEtag.compute(bytes));
  }

  @Test
  public void testComputeDiffersForDifferentInput()
  {
    Assert.assertNotEquals(
        ConfigEtag.compute("a".getBytes(StandardCharsets.UTF_8)),
        ConfigEtag.compute("b".getBytes(StandardCharsets.UTF_8))
    );
  }

  @Test
  public void testComputeIsQuoted()
  {
    String etag = ConfigEtag.compute(new byte[]{1, 2, 3});
    Assert.assertNotNull(etag);
    Assert.assertTrue("ETag must be quoted: " + etag, etag.startsWith("\"") && etag.endsWith("\""));
  }

  @Test
  public void testComputeNullInput()
  {
    Assert.assertNull(ConfigEtag.compute(null));
  }

  @Test
  public void testMatchesNullHeaderAlwaysTrue()
  {
    Assert.assertTrue(ConfigEtag.matches(null, new byte[]{1, 2, 3}));
    Assert.assertTrue(ConfigEtag.matches(null, null));
  }

  @Test
  public void testMatchesWildcard()
  {
    Assert.assertTrue(ConfigEtag.matches("*", new byte[]{1, 2, 3}));
    Assert.assertFalse("wildcard must not match absent value", ConfigEtag.matches("*", null));
  }

  @Test
  public void testMatchesExact()
  {
    byte[] bytes = "payload".getBytes(StandardCharsets.UTF_8);
    String etag = ConfigEtag.compute(bytes);
    Assert.assertTrue(ConfigEtag.matches(etag, bytes));
  }

  @Test
  public void testMatchesMismatch()
  {
    byte[] bytes = "payload".getBytes(StandardCharsets.UTF_8);
    String wrongEtag = ConfigEtag.compute("other".getBytes(StandardCharsets.UTF_8));
    Assert.assertFalse(ConfigEtag.matches(wrongEtag, bytes));
  }

  @Test
  public void testMatchesAgainstNullCurrent()
  {
    String etag = ConfigEtag.compute(new byte[]{1});
    Assert.assertFalse(ConfigEtag.matches(etag, null));
  }

  @Test
  public void testMatchesCommaSeparatedList()
  {
    byte[] bytes = "payload".getBytes(StandardCharsets.UTF_8);
    String correct = ConfigEtag.compute(bytes);
    String header = "\"bogus\", " + correct + ", \"another\"";
    Assert.assertTrue(ConfigEtag.matches(header, bytes));
  }
}
