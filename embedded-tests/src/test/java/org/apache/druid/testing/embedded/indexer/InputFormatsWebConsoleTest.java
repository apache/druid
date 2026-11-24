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

package org.apache.druid.testing.embedded.indexer;

import org.apache.druid.data.input.avro.AvroExtensionsModule;
import org.apache.druid.data.input.orc.OrcExtensionsModule;
import org.apache.druid.data.input.parquet.ParquetExtensionsModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.console.WebConsoleTestBase;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

/**
 * Loads the data files of {@link ITLocalInputSourceAllInputFormatTest} (CSV, TSV, Parquet, ORC and Avro OCF) through
 * the data loader of the web console, checking the input format it picks for each.
 */
public class InputFormatsWebConsoleTest extends WebConsoleTestBase
{
  @Override
  protected void configureCluster(EmbeddedDruidCluster cluster)
  {
    cluster.addExtensions(AvroExtensionsModule.class, ParquetExtensionsModule.class, OrcExtensionsModule.class);
  }

  @Test
  public void testInputFormats() throws Exception
  {
    runSpec(
        "input-formats.spec.ts",
        Map.of("DRUID_E2E_TEST_DATA_DIR", new File("src/test/resources/data").getCanonicalPath())
    );
  }
}
