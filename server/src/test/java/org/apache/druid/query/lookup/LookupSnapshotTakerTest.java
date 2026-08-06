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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class LookupSnapshotTakerTest
{
  private static final String TIER1 = "tier1";
  private static final String TIER2 = "tier2";

  @TempDir
  public File temporaryFolder;

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();


  private LookupSnapshotTaker lookupSnapshotTaker;
  private String basePersistDirectory;

  @BeforeEach
  public void setUp() throws IOException
  {
    mapper.registerSubtypes(MapLookupExtractorFactory.class);
    basePersistDirectory = newFolder(temporaryFolder, "junit").getAbsolutePath();
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
  public void testIOExceptionDuringLookupPersist()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(ISE.class, () -> {
      File directory = newFolder(temporaryFolder, "junit");
      LookupSnapshotTaker lookupSnapshotTaker = new LookupSnapshotTaker(mapper, directory.getAbsolutePath());
      File snapshotFile = lookupSnapshotTaker.getPersistFile(TIER1);
      Assertions.assertFalse(snapshotFile.exists());
      Assertions.assertTrue(snapshotFile.createNewFile());
      Assertions.assertTrue(snapshotFile.setReadOnly());
      Assertions.assertTrue(snapshotFile.getParentFile().setReadOnly());
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
      List<LookupBean> lookupBeanList = Collections.singletonList(lookupBean);
      lookupSnapshotTaker.takeSnapshot(TIER1, lookupBeanList);
    });
    assertTrue(exception.getMessage().contains("Exception during serialization of lookups"));
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
    org.junit.jupiter.api.Assertions.assertThrows(ISE.class, () -> {
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
    File directory = newFolder(temporaryFolder, "junit");
    LookupSnapshotTaker lookupSnapshotTaker = new LookupSnapshotTaker(mapper, directory.getAbsolutePath());
    List<LookupBean> actualList = lookupSnapshotTaker.pullExistingSnapshot(TIER1);
    Assertions.assertEquals(Collections.emptyList(), actualList);
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    if (subDirs.length == 0 || (subDirs.length == 1 && "junit".equals(subDirs[0]))) {
      return java.nio.file.Files.createTempDirectory(root.toPath(), "junit").toFile();
    }
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
