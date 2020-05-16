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

package org.apache.druid.data.input.parquet;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Objects;

public class ParquetReaderResourceLeakTest extends BaseParquetReaderTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testFetchOnReadCleanupAfterExhaustingIterator() throws IOException
  {
    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("timestamp", "iso", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(ImmutableList.of("page", "language", "user", "unpatrolled"))
        ),
        Collections.emptyList()
    );
    FetchingFileEntity entity = new FetchingFileEntity(new File("example/wiki/wiki.parquet"));
    ParquetInputFormat parquet = new ParquetInputFormat(JSONPathSpec.DEFAULT, false, new Configuration());
    File tempDir = temporaryFolder.newFolder();
    InputEntityReader reader = parquet.createReader(schema, entity, tempDir);
    Assert.assertEquals(0, Objects.requireNonNull(tempDir.list()).length);
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(Objects.requireNonNull(tempDir.list()).length > 0);
      while (iterator.hasNext()) {
        iterator.next();
      }
    }
    Assert.assertEquals(0, Objects.requireNonNull(tempDir.list()).length);
  }

  private static class FetchingFileEntity extends FileEntity
  {
    private FetchingFileEntity(File file)
    {
      super(file);
    }

    @Override
    public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer)
    {
      // Copied from InputEntity
      try {
        final File tempFile = File.createTempFile("druid-input-entity", ".tmp", temporaryDirectory);
        try (InputStream is = open()) {
          FileUtils.copyLarge(
              is,
              tempFile,
              fetchBuffer,
              getRetryCondition(),
              DEFAULT_MAX_NUM_FETCH_TRIES,
              StringUtils.format("Failed to fetch into [%s]", tempFile.getAbsolutePath())
          );
        }

        return new CleanableFile()
        {
          @Override
          public File file()
          {
            return tempFile;
          }

          @Override
          public void close()
          {
            if (!tempFile.delete()) {
              LOG.warn("Failed to remove file[%s]", tempFile.getAbsolutePath());
            }
          }
        };
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
