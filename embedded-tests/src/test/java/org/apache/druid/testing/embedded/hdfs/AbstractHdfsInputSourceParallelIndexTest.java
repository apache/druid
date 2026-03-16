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

package org.apache.druid.testing.embedded.hdfs;

import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexer.AbstractITBatchIndexTest;
import org.junit.jupiter.api.BeforeAll;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Abstract base class for embedded parallel index tests where HDFS is the input source.
 * Subclasses supply their own {@link HdfsStorageResource} and register whatever deep-storage
 * resource they need (e.g. another {@link HdfsStorageResource} in deep-storage mode, or an
 * Azure / GCS resource).
 *
 * <p>Test data ({@code tiny_wiki_*.json}) is uploaded to HDFS before any test runs and deleted
 * after all tests complete.
 */
public abstract class AbstractHdfsInputSourceParallelIndexTest extends AbstractITBatchIndexTest
{
  private static final Logger LOG = new Logger(AbstractHdfsInputSourceParallelIndexTest.class);
  private static final String HDFS_INDEX_TASK = "/indexer/wikipedia_cloud_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String HDFS_DATA_PATH = "/data/json";

  private static final String DATA_1 = "tiny_wiki_1.json";
  private static final String DATA_2 = "tiny_wiki_2.json";
  private static final String DATA_3 = "tiny_wiki_3.json";

  /**
   * Placeholder replaced with the actual {@code hdfs://localhost:PORT} URL at test time.
   */
  private static final String HDFS_URL_PLACEHOLDER = "%%HDFS_URL%%";

  private HdfsTestUtil hdfs;

  /**
   * Returns the {@link HdfsStorageResource} that holds the HDFS input data.
   * This instance must have been registered with the embedded cluster in
   * {@link #addResources}.
   */
  protected abstract HdfsStorageResource getHdfsResource();

  /**
   * Data provider: returns (inputSourcePropertyKey, inputSourcePropertyValue) pairs that exercise
   * different forms of the HDFS input source spec.
   * <ul>
   *   <li>A single directory path (HDFS reads all files in the directory)</li>
   *   <li>A list of explicit file paths</li>
   * </ul>
   * The {@value HDFS_URL_PLACEHOLDER} token is replaced with the real NameNode URL when the
   * test task is submitted.
   */
  public static Object[][] resources()
  {
    final String dataBase = HDFS_URL_PLACEHOLDER + HDFS_DATA_PATH;
    return new Object[][]{
        {new Pair<>("paths", dataBase)},
        {new Pair<>("paths", List.of(
            dataBase + "/" + DATA_1,
            dataBase + "/" + DATA_2,
            dataBase + "/" + DATA_3
        ))}
    };
  }

  public static List<String> fileList()
  {
    return Arrays.asList(DATA_1, DATA_2, DATA_3);
  }

  @BeforeAll
  public void setupHdfs() throws Exception
  {
    LOG.info("Uploading test data files to HDFS");
    hdfs = new HdfsTestUtil(getHdfsResource().getFileSystem(), HDFS_DATA_PATH);
    hdfs.uploadDataFilesToHdfs(List.of(
        "data/json/" + DATA_1,
        "data/json/" + DATA_2,
        "data/json/" + DATA_3
    ));
  }

  /**
   * Runs a parallel-index test reading from HDFS.
   *
   * @param hdfsInputSource {@code lhs} is the input-source property key ({@code "paths"}),
   *                        {@code rhs} is the value (String or {@link List})
   *                        with {@value HDFS_URL_PLACEHOLDER} placeholders
   * @param segmentAvailabilityConfirmationPair segment-availability check flags
   */
  protected String doHdfsTest(
      Pair<String, Object> hdfsInputSource,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair
  ) throws Exception
  {
    final String indexDatasource = dataSource;
    try (final Closeable ignored = unloader(indexDatasource)) {
      final String hdfsUrl = getHdfsResource().getHdfsUrl();

      final Function<String, String> transform = spec -> {
        try {
          // Replace the HDFS URL placeholder in the serialised input-source value.
          String inputSourceValue = jsonMapper.writeValueAsString(hdfsInputSource.rhs);
          inputSourceValue = StringUtils.replace(inputSourceValue, HDFS_URL_PLACEHOLDER, hdfsUrl);

          spec = StringUtils.replace(
              spec,
              "%%INPUT_FORMAT_TYPE%%",
              InputFormatDetails.JSON.getInputFormatType()
          );
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(spec, "%%INPUT_SOURCE_TYPE%%", "hdfs");
          // sharing index_task.json with cloud which has this one
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTIES%%",
              "null"
          );
          spec = StringUtils.replace(spec, "%%INPUT_SOURCE_PROPERTY_KEY%%", hdfsInputSource.lhs);
          spec = StringUtils.replace(spec, "%%INPUT_SOURCE_PROPERTY_VALUE%%", inputSourceValue);
          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          HDFS_INDEX_TASK,
          transform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          segmentAvailabilityConfirmationPair
      );
      return indexDatasource;
    }
  }
}
