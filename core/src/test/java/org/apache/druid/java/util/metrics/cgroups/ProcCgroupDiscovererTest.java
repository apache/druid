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

package org.apache.druid.java.util.metrics.cgroups;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;

public class ProcCgroupDiscovererTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws Exception
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
  }

  @Test
  public void testSimpleProc()
  {
    Assert.assertEquals(
        new File(
            cgroupDir,
            "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
        ).toPath(),
        discoverer.discover("cpu")
    );
  }

  @Test
  public void testParse()
  {
    final ProcCgroupDiscoverer.ProcMountsEntry entry = ProcCgroupDiscoverer.ProcMountsEntry.parse(
        "/dev/md126 /ebs xfs rw,seclabel,noatime,attr2,inode64,sunit=1024,swidth=16384,noquota 0 0"
    );
    Assert.assertEquals("/dev/md126", entry.dev);
    Assert.assertEquals(Paths.get("/ebs"), entry.path);
    Assert.assertEquals("xfs", entry.type);
    Assert.assertEquals(ImmutableSet.of(
        "rw",
        "seclabel",
        "noatime",
        "attr2",
        "inode64",
        "sunit=1024",
        "swidth=16384",
        "noquota"
    ), entry.options);
  }

  @Test
  public void testNullCgroup()
  {
    expectedException.expect(NullPointerException.class);
    Assert.assertNull(new ProcCgroupDiscoverer(procDir.toPath()).discover(null));
  }
}
