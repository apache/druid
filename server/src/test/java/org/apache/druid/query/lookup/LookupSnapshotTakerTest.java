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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LookupSnapshotTakerTest
{
  private static final String TIER1 = "tier1";
  private static final String TIER2 = "tier2";

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = new TemporaryFolderExtension();

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();


  private LookupSnapshotTaker lookupSnapshotTaker;
  private String basePersistDirectory;

  @BeforeEach
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
    List<LookupBean> lookupBeanList1 = Collections.singletonList(lookupBean1);
    lookupSnapshotTaker.takeSnapshot(TIER1, lookupBeanList1);
    List<LookupBean> lookupBeanList2 = Collections.singletonList(lookupBean2);
    lookupSnapshotTaker.takeSnapshot(TIER2, lookupBeanList2);
    Assertions.assertEquals(lookupBeanList1, lookupSnapshotTaker.pullExistingSnapshot(TIER1));
    Assertions.assertEquals(lookupBeanList2, lookupSnapshotTaker.pullExistingSnapshot(TIER2));
  }

  @Test
  public void testIOExceptionDuringLookupPersist() throws IOException
  {
    final File directory = temporaryFolder.newFolder();
    final LookupSnapshotTaker lookupSnapshotTaker = new LookupSnapshotTaker(mapper, directory.getAbsolutePath());
    final File snapshotFile = lookupSnapshotTaker.getPersistFile(TIER1);
    Assertions.assertFalse(snapshotFile.exists());
    Assertions.assertTrue(snapshotFile.createNewFile());
    try {
      Assertions.assertTrue(
          snapshotFile.setReadOnly(),
          "test setup must be able to make the snapshot file read-only"
      );
      final File snapshotDirectory = snapshotFile.getParentFile();
      Assertions.assertTrue(
          snapshotDirectory.setReadOnly(),
          "test setup must be able to make the snapshot directory read-only"
      );
      final LookupBean lookupBean = new LookupBean(
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
      final List<LookupBean> lookupBeanList = Collections.singletonList(lookupBean);
      final Throwable exception = Assertions.assertThrows(
          ISE.class,
          () -> lookupSnapshotTaker.takeSnapshot(TIER1, lookupBeanList)
      );
      Assertions.assertTrue(exception.getMessage().contains("Exception during serialization of lookups"));
    }
    finally {
      Assertions.assertTrue(snapshotFile.setWritable(true), "test teardown must restore file write permission");
      Assertions.assertTrue(
          snapshotFile.getParentFile().setWritable(true),
          "test teardown must restore directory write permission"
      );
    }
  }

  @Test
  public void tesLookupPullingFromEmptyFile() throws IOException
  {
    File snapshotFile = lookupSnapshotTaker.getPersistFile(TIER1);
    Assertions.assertTrue(snapshotFile.createNewFile());
    Assertions.assertEquals(Collections.emptyList(), lookupSnapshotTaker.pullExistingSnapshot(TIER1));
  }

  @Test
  public void tesLookupPullingFromCorruptFile()
  {
    Assertions.assertThrows(ISE.class, () -> {
      File snapshotFile = lookupSnapshotTaker.getPersistFile(TIER1);
      Assertions.assertTrue(snapshotFile.createNewFile());
      byte[] bytes = StringUtils.toUtf8("test corrupt file");
      Files.write(bytes, snapshotFile);
      lookupSnapshotTaker.pullExistingSnapshot(TIER1);
    });
  }

  @Test
  public void testLookupPullingFromNonExistingFile() throws IOException
  {
    File directory = temporaryFolder.newFolder();
    LookupSnapshotTaker lookupSnapshotTaker = new LookupSnapshotTaker(mapper, directory.getAbsolutePath());
    List<LookupBean> actualList = lookupSnapshotTaker.pullExistingSnapshot(TIER1);
    Assertions.assertEquals(Collections.emptyList(), actualList);
  }

}
