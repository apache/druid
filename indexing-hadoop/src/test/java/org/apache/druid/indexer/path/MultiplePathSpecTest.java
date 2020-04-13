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

package org.apache.druid.indexer.path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MultiplePathSpecTest
{
  @Test
  public void testSerde() throws Exception
  {
    PathSpec expected = new MultiplePathSpec(
        Lists.newArrayList(
            new StaticPathSpec("/tmp/path1", null),
            new StaticPathSpec("/tmp/path2", TextInputFormat.class)
        )
    );

    ObjectMapper jsonMapper = new DefaultObjectMapper();

    PathSpec actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAddInputPaths() throws Exception
  {
    PathSpec ps1 = EasyMock.createMock(PathSpec.class);
    EasyMock.expect(ps1.addInputPaths(null, null)).andReturn(null);

    PathSpec ps2 = EasyMock.createMock(PathSpec.class);
    EasyMock.expect(ps2.addInputPaths(null, null)).andReturn(null);

    EasyMock.replay(ps1, ps2);

    new MultiplePathSpec(Lists.newArrayList(ps1, ps2)).addInputPaths(null, null);

    EasyMock.verify(ps1, ps2);
  }
}
