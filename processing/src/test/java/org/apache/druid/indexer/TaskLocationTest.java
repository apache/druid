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


import com.google.common.net.HostAndPort;
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
    Assert.assertEquals(new URL("http://abc:80/foo"), new TaskLocation("abc", 80, 0, null).makeURL("/foo"));
    Assert.assertEquals(new URL("http://abc:80/foo"), new TaskLocation("abc", 80, -1, null).makeURL("/foo"));
    Assert.assertEquals(new URL("https://abc:443/foo"), new TaskLocation("abc", 80, 443, null).makeURL("/foo"));
    Assert.assertThrows(
        "URL that does not start with '/'",
        IllegalArgumentException.class,
        () -> new TaskLocation("abc", 80, 443, null).makeURL("foo")
    );
  }

  @Test
  public void testTlsForPeonJobs()
  {
    TaskLocation noTls = TaskLocation.create("foo", 1, 2, false);
    Assert.assertEquals(-1, noTls.getTlsPort());
    Assert.assertEquals(1, noTls.getPort());
    TaskLocation tls = TaskLocation.create("foo", 1, 2, true);
    Assert.assertEquals(-1, tls.getPort());
    Assert.assertEquals(2, tls.getTlsPort());
  }

  @Test
  public void testDefaultK8sJobName()
  {
    TaskLocation noK8sJobName = TaskLocation.create("foo", 1, 2, false);
    Assert.assertNull(noK8sJobName.getK8sPodName());
    noK8sJobName = TaskLocation.create("foo", 1, 2);
    Assert.assertNull(noK8sJobName.getK8sPodName());
  }

  @Test
  public void testK8sJobNameSet()
  {
    TaskLocation k8sJobName = TaskLocation.create("foo", 1, 2, false, "job-name");
    Assert.assertEquals("job-name", k8sJobName.getK8sPodName());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(TaskLocation.class).usingGetClass().verify();
  }

  @Test
  public void testGetLocationWithK8sPodNameShouldReturnK8sPodName()
  {
    TaskLocation taskLocation = TaskLocation.create("foo", 1, 2, false, "job-name");
    Assert.assertEquals("job-name", taskLocation.getLocation());
  }

  @Test
  public void testGetLocationWithK8sPodNameAndTlsShouldReturnK8sPodName()
  {
    TaskLocation taskLocation = TaskLocation.create("foo", 1, 2, true, "job-name");
    Assert.assertEquals("job-name", taskLocation.getLocation());
  }

  @Test
  public void testGetLocationWithK8sPodNameAndNoHostShouldReturnK8sPodName()
  {
    TaskLocation taskLocation = TaskLocation.create(null, 1, 2, true, "job-name");
    Assert.assertEquals("job-name", taskLocation.getLocation());
  }

  @Test
  public void testGetLocationWithoutK8sPodNameAndHostShouldReturnNull()
  {
    TaskLocation taskLocation = TaskLocation.create(null, 1, 2, false);
    Assert.assertNull(taskLocation.getLocation());
  }

  @Test
  public void testGetLocationWithoutK8sPodNameAndNoTlsPortShouldReturnLocation()
  {
    TaskLocation taskLocation = TaskLocation.create("foo", 1, -1, false);
    Assert.assertEquals(HostAndPort.fromParts("foo", 1).toString(), taskLocation.getLocation());
  }

  @Test
  public void testGetLocationWithoutK8sPodNameAndNonZeroTlsPortShouldReturnLocation()
  {
    TaskLocation taskLocation = TaskLocation.create("foo", 1, 2, true);
    Assert.assertEquals(HostAndPort.fromParts("foo", 2).toString(), taskLocation.getLocation());
  }
}
