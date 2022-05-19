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

package org.apache.druid.indexer;


import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

public class TaskLocationTest
{
  @Test
  @SuppressWarnings("HttpUrlsUsage")
  public void testMakeURL() throws MalformedURLException
  {
    Assert.assertEquals(new URL("http://abc:80/foo"), new TaskLocation("abc", 80, 0).makeURL("/foo"));
    Assert.assertEquals(new URL("http://abc:80/foo"), new TaskLocation("abc", 80, -1).makeURL("/foo"));
    Assert.assertEquals(new URL("https://abc:443/foo"), new TaskLocation("abc", 80, 443).makeURL("/foo"));
    Assert.assertThrows(
        "URL that does not start with '/'",
        IllegalArgumentException.class,
        () -> new TaskLocation("abc", 80, 443).makeURL("foo")
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(TaskLocation.class).usingGetClass().verify();
  }
}
