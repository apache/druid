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

package io.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 */
public class ExtensionsConfigTest
{
  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String json = "{}";
    ObjectMapper mapper = TestHelper.getJsonMapper();

    ExtensionsConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(json, ExtensionsConfig.class)
        ),
        ExtensionsConfig.class
    );

    Assert.assertTrue(config.searchCurrentClassloader());
    Assert.assertEquals("extensions", config.getDirectory());
    Assert.assertEquals("hadoop-dependencies", config.getHadoopDependenciesDir());
    Assert.assertNull(config.getHadoopContainerDruidClasspath());
    Assert.assertNull(config.getLoadList());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String json = "{\n"
                  + "  \"searchCurrentClassloader\": false,\n"
                  + "  \"directory\": \"testExtensions\",\n"
                  + "  \"hadoopDependenciesDir\": \"testHadoopDependenciesDir\",\n"
                  + "  \"hadoopContainerDruidClasspath\": \"testHadoopContainerClasspath\",\n"
                  + "  \"loadList\": [\"b\",\"a\"]\n"
                  + "}";
    ObjectMapper mapper = TestHelper.getJsonMapper();

    ExtensionsConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(json, ExtensionsConfig.class)
        ),
        ExtensionsConfig.class
    );

    Assert.assertFalse(config.searchCurrentClassloader());
    Assert.assertEquals("testExtensions", config.getDirectory());
    Assert.assertEquals("testHadoopDependenciesDir", config.getHadoopDependenciesDir());
    Assert.assertEquals("testHadoopContainerClasspath", config.getHadoopContainerDruidClasspath());
    Assert.assertEquals(
        ImmutableList.of(
            "b", "a"
        ),
            new ArrayList<>(config.getLoadList())
    );
  }
  @Test
  public void testLoadList() throws Exception
  {
    String json = "{\n"
            + "  \"searchCurrentClassloader\": false,\n"
            + "  \"directory\": \"testExtensions\",\n"
            + "  \"hadoopDependenciesDir\": \"testHadoopDependenciesDir\",\n"
            + "  \"hadoopContainerDruidClasspath\": \"testHadoopContainerClasspath\",\n"
            + "  \"loadList\": [\"b\",\"b\",\"a\",\"c\",\"d\",\"a\"]\n"
            + "}";
    ObjectMapper mapper = TestHelper.getJsonMapper();

    ExtensionsConfig config = mapper.readValue(
            mapper.writeValueAsString(
                    mapper.readValue(json, ExtensionsConfig.class)
            ),
            ExtensionsConfig.class
    );

    Assert.assertEquals(
            ImmutableList.of("b","a","c","d"),
            new ArrayList<>(config.getLoadList())
    );
  }
}

