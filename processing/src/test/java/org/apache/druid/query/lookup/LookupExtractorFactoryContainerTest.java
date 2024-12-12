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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 */
public class LookupExtractorFactoryContainerTest
{
  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{\n"
                     + "\"version\": \"v1\",\n"
                     + "\"lookupExtractorFactory\": {\n"
                     + "   \"type\": \"test\"\n"
                     + " }\n"
                     + "}\n";

    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(TestLookupExtractorFactory.class);

    LookupExtractorFactoryContainer actual = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(jsonStr, LookupExtractorFactoryContainer.class)
        ),
        LookupExtractorFactoryContainer.class
    );

    Assert.assertEquals(
        new LookupExtractorFactoryContainer(
            "v1",
            new TestLookupExtractorFactory()
        ),
        actual
    );
  }

  @Test
  public void testReplaces()
  {
    LookupExtractorFactoryContainer l0 = new LookupExtractorFactoryContainer(null, new TestLookupExtractorFactory());
    LookupExtractorFactoryContainer l1 = new LookupExtractorFactoryContainer(null, new TestLookupExtractorFactory());
    LookupExtractorFactoryContainer l2 = new LookupExtractorFactoryContainer("v0", new TestLookupExtractorFactory());
    LookupExtractorFactoryContainer l3 = new LookupExtractorFactoryContainer("v1", new TestLookupExtractorFactory());

    Assert.assertTrue(l0.replaces(l1));
    Assert.assertFalse(l1.replaces(l2));
    Assert.assertTrue(l2.replaces(l1));
    Assert.assertFalse(l2.replaces(l3));
    Assert.assertTrue(l3.replaces(l2));
  }

  @JsonTypeName("test")
  static class TestLookupExtractorFactory implements LookupExtractorFactory
  {

    @Override
    public boolean start()
    {
      return false;
    }

    @Override
    public boolean close()
    {
      return false;
    }

    @Override
    public boolean replaces(@Nullable LookupExtractorFactory other)
    {
      return true;
    }

    @Nullable
    @Override
    public LookupIntrospectHandler getIntrospectHandler()
    {
      return null;
    }

    @Override
    public void awaitInitialization()
    {
    }

    @Override
    public boolean isInitialized()
    {
      return true;
    }

    @Override
    public LookupExtractor get()
    {
      return null;
    }

    @Override
    @SuppressWarnings("EqualsHashCode")
    public boolean equals(Object other)
    {
      return other instanceof TestLookupExtractorFactory;
    }
  }
}
