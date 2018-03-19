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

package io.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class LookupSnapshotTakerTest
{
  private static final String TIER1 = "tier1";
  private static final String TIER2 = "tier2";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();


  private LookupSnapshotTaker lookupSnapshotTaker;
  private String basePersistDirectory;

  @Before
  public void setUp() throws IOException
  {
    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    basePersistDirectory = temporaryFolder.newFolder().getAbsolutePath();
    lookupSnapshotTaker = new LookupSnapshotTaker(mapper, basePersistDirectory);
  }

  @Test
  public void testTakeSnapshotAndPullExisting()
  {
    LookupBean lookupBean1 = new LookupBean(
        "name1",
        null,
        new LookupExtractorFactoryContainer(
            "v1",
            new MapLookupExtractorFactory(ImmutableMap.of("key", "value"), true)
        )
    );
    LookupBean lookupBean2 = new LookupBean(
        "name2",
        null,
        new LookupExtractorFactoryContainer(
            "v1",
            new MapLookupExtractorFactory(ImmutableMap.of("key", "value"), true)
        )
    );
    List<LookupBean> lookupBeanList1 = Lists.newArrayList(lookupBean1);
    lookupSnapshotTaker.takeSnapshot(TIER1, lookupBeanList1);
    List<LookupBean> lookupBeanList2 = Lists.newArrayList(lookupBean2);
    lookupSnapshotTaker.takeSnapshot(TIER2, lookupBeanList2);
    Assert.assertEquals(lookupBeanList1, lookupSnapshotTaker.pullExistingSnapshot(TIER1));
    Assert.assertEquals(lookupBeanList2, lookupSnapshotTaker.pullExistingSnapshot(TIER2));
  }

  @Test
  public void testIOExceptionDuringLookupPersist() throws IOException
  {
    File directory = temporaryFolder.newFolder();
    LookupSnapshotTaker lookupSnapshotTaker = new LookupSnapshotTaker(mapper, directory.getAbsolutePath());
    File snapshotFile = lookupSnapshotTaker.getPersistFile(TIER1);
    Assert.assertFalse(snapshotFile.exists());
    Assert.assertTrue(snapshotFile.createNewFile());
    Assert.assertTrue(snapshotFile.setReadOnly());
    Assert.assertTrue(snapshotFile.getParentFile().setReadOnly());
    LookupBean lookupBean = new LookupBean(
        "name",
        null,
        new LookupExtractorFactoryContainer(
            "v1",
            new MapLookupExtractorFactory(
                ImmutableMap.of(
                    "key",
                    "value"
                ), true
            )
        )
    );
    List<LookupBean> lookupBeanList = Lists.newArrayList(lookupBean);

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Exception during serialization of lookups");
    lookupSnapshotTaker.takeSnapshot(TIER1, lookupBeanList);
  }

  @Test
  public void tesLookupPullingFromEmptyFile() throws IOException
  {
    File snapshotFile = lookupSnapshotTaker.getPersistFile(TIER1);
    Assert.assertTrue(snapshotFile.createNewFile());
    Assert.assertEquals(Collections.EMPTY_LIST, lookupSnapshotTaker.pullExistingSnapshot(TIER1));
  }

  @Test(expected = ISE.class)
  public void tesLookupPullingFromCorruptFile() throws IOException
  {
    File snapshotFile = lookupSnapshotTaker.getPersistFile(TIER1);
    Assert.assertTrue(snapshotFile.createNewFile());
    byte[] bytes = StringUtils.toUtf8("test corrupt file");
    Files.write(bytes, snapshotFile);
    lookupSnapshotTaker.pullExistingSnapshot(TIER1);
  }

  @Test
  public void testLookupPullingFromNonExistingFile() throws IOException
  {
    File directory = temporaryFolder.newFolder();
    LookupSnapshotTaker lookupSnapshotTaker = new LookupSnapshotTaker(mapper, directory.getAbsolutePath());
    List<LookupBean> actualList = lookupSnapshotTaker.pullExistingSnapshot(TIER1);
    Assert.assertEquals(Collections.EMPTY_LIST, actualList);
  }
}
