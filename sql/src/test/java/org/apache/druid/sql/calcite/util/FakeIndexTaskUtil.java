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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.quidem.ProjectPathUtils;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IOConfig;
import org.apache.druid.segment.indexing.IngestionSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.sql.calcite.util.datasets.InputSourceBasedTestDataset;
import org.apache.druid.sql.calcite.util.datasets.TestDataSet;

import java.io.File;
import java.io.IOException;

/**
 * Utility class to create {@link TestDataSet} from fake indexing tasks.
 *
 * Goal is to let the users utilize the ingestion api to create test data.
 */
public class FakeIndexTaskUtil
{
  public static TestDataSet makeDS(ObjectMapper objectMapper, File src)
  {
    try {
      ObjectMapper om = objectMapper.copy();
      om.registerSubtypes(new NamedType(MyIOConfigType.class, "index_parallel"));
      FakeIndexTask indexTask = om.readValue(src, FakeIndexTask.class);
      FakeIngestionSpec spec = indexTask.spec;
      InputSource inputSource = relativizeLocalInputSource(
          spec.getIOConfig().inputSource, ProjectPathUtils.PROJECT_ROOT
      );
      TestDataSet dataset = new InputSourceBasedTestDataset(
          spec.getDataSchema(),
          spec.getIOConfig().inputFormat,
          inputSource
      );
      return dataset;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static InputSource relativizeLocalInputSource(InputSource inputSource, File projectRoot)
  {
    if (!(inputSource instanceof LocalInputSource)) {
      return inputSource;
    }
    LocalInputSource localInputSource = (LocalInputSource) inputSource;
    if (localInputSource.getBaseDir().isAbsolute()) {
      return inputSource;
    }
    File newBaseDir = projectRoot.toPath().resolve(localInputSource.getBaseDir().toPath()).toFile();
    return new LocalInputSource(
        newBaseDir,
        localInputSource.getFilter(),
        localInputSource.getFiles(),
        localInputSource.getSystemFields()
    );
  }

  static class FakeIndexTask
  {
    @JsonProperty
    public FakeIngestionSpec spec;
  }

  static class FakeIngestionSpec extends IngestionSpec<MyIOConfigType, TuningConfig>
  {
    @JsonCreator
    public FakeIngestionSpec(
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("ioConfig") MyIOConfigType ioConfig)
    {
      super(dataSchema, ioConfig, null);
    }
  }

  static class MyIOConfigType implements IOConfig
  {
    @JsonProperty
    public InputSource inputSource;
    @JsonProperty
    public InputFormat inputFormat;
  }
}
