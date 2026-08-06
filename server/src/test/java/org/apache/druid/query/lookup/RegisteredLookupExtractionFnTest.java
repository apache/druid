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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RegisteredLookupExtractionFnTest extends InitializedNullHandlingTest
{
  private static Map<String, String> MAP = ImmutableMap.of(
      "foo", "bar",
      "bat", "baz"
  );
  private static final LookupExtractor LOOKUP_EXTRACTOR = new MapLookupExtractor(MAP, true);
  private static final String LOOKUP_NAME = "some lookup";

  @Test
  public void testSimpleDelegation()
  {
    final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);
    managerReturnsMap(manager);
    EasyMock.replay(manager);
    final RegisteredLookupExtractionFn fn = new RegisteredLookupExtractionFn(
        manager,
        LOOKUP_NAME,
        true,
        null,
        false,
        false
    );
    EasyMock.verify(manager);

    Assertions.assertSame(LOOKUP_EXTRACTOR, fn.getDelegate().getLookup());

    Assertions.assertEquals(false, fn.isInjective());
    Assertions.assertFalse(fn.getDelegate().isInjective());

    Assertions.assertEquals(ExtractionFn.ExtractionType.MANY_TO_ONE, fn.getExtractionType());
    Assertions.assertEquals(ExtractionFn.ExtractionType.MANY_TO_ONE, fn.getDelegate().getExtractionType());

    for (String orig : Arrays.asList(null, "foo", "bat")) {
      Assertions.assertEquals(LOOKUP_EXTRACTOR.apply(orig), fn.apply(orig));
    }
    Assertions.assertEquals("not in the map", fn.apply("not in the map"));
  }

  @Test
  public void testInheritInjective()
  {
    final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);
    managerReturnsMap(manager);
    EasyMock.replay(manager);
    final RegisteredLookupExtractionFn fn = new RegisteredLookupExtractionFn(
        manager,
        LOOKUP_NAME,
        true,
        null,
        null,
        false
    );
    EasyMock.verify(manager);

    Assertions.assertNull(fn.isInjective());
    Assertions.assertEquals(ExtractionFn.ExtractionType.ONE_TO_ONE, fn.getExtractionType());
  }

  @Test
  public void testMissingDelegation()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
      final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);
      EasyMock.expect(manager.get(EasyMock.eq(LOOKUP_NAME))).andReturn(Optional.empty()).once();
      EasyMock.replay(manager);
      try {
        new RegisteredLookupExtractionFn(
            manager,
            LOOKUP_NAME,
            true,
            null,
            true,
            false
        ).apply("foo");
      }
      finally {
        EasyMock.verify(manager);
      }
    });
    assertTrue(exception.getMessage().contains("Lookup [some lookup] not found"));
  }

  @Test
  public void testNullLookup()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
      new RegisteredLookupExtractionFn(
          null,
          null,
          true,
          null,
          true,
          false
      );
    });
    assertTrue(exception.getMessage().contains("`lookup` required"));
  }

  @Test
  public void testSerDe() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();

    final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);
    managerReturnsMap(manager);
    EasyMock.replay(manager);
    final RegisteredLookupExtractionFn fn = new RegisteredLookupExtractionFn(
        manager,
        LOOKUP_NAME,
        true,
        null,
        true,
        false
    );
    EasyMock.verify(manager);

    final Map<String, Object> result = mapper.readValue(
        mapper.writeValueAsString(fn),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assertions.assertEquals(mapper.convertValue(fn, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT), result);
    Assertions.assertEquals(LOOKUP_NAME, result.get("lookup"));
    Assertions.assertEquals(true, result.get("retainMissingValue"));
    Assertions.assertEquals(true, result.get("injective"));
    Assertions.assertNull(result.get("replaceMissingValueWith"));
    Assertions.assertEquals(false, result.get("optimize"));
  }

  @Test
  public void testEquals()
  {
    final LookupExtractorFactoryContainerProvider manager = EasyMock.createStrictMock(LookupReferencesManager.class);
    managerReturnsMap(manager);
    EasyMock.replay(manager);
    final RegisteredLookupExtractionFn fn = new RegisteredLookupExtractionFn(
        manager,
        LOOKUP_NAME,
        false,
        "something",
        true,
        false
    );
    Assertions.assertEquals(
        fn,
        new RegisteredLookupExtractionFn(
            manager,
            LOOKUP_NAME,
            false,
            "something",
            true,
            false
        )
    );
    Assertions.assertNotEquals(
        fn,
        new RegisteredLookupExtractionFn(
            manager,
            LOOKUP_NAME,
            true,
            null,
            true,
            false
        )
    );

    Assertions.assertNotEquals(
        fn,
        new RegisteredLookupExtractionFn(
            manager,
            LOOKUP_NAME,
            false,
            "something else",
            true,
            false
        )
    );


    Assertions.assertNotEquals(
        fn,
        new RegisteredLookupExtractionFn(
            manager,
            LOOKUP_NAME,
            false,
            "something",
            false,
            false
        )
    );

    Assertions.assertNotEquals(
        fn,
        new RegisteredLookupExtractionFn(
            manager,
            LOOKUP_NAME,
            false,
            "something",
            true,
            true
        )
    );


    Assertions.assertNotEquals(
        fn,
        new RegisteredLookupExtractionFn(
            manager,
            LOOKUP_NAME,
            false,
            null,
            true,
            false
        )
    );
    EasyMock.verify(manager);
  }

  private void managerReturnsMap(LookupExtractorFactoryContainerProvider manager)
  {
    EasyMock.expect(manager.get(EasyMock.eq(LOOKUP_NAME))).andReturn(
        Optional.of(
            new LookupExtractorFactoryContainer(
                "v0",
                new LookupExtractorFactory()
                {
                  @Override
                  public boolean start()
                  {
                    return false;
                  }

                  @Override
                  public boolean replaces(@Nullable LookupExtractorFactory other)
                  {
                    return false;
                  }

                  @Override
                  public boolean close()
                  {
                    return false;
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
                    return LOOKUP_EXTRACTOR;
                  }
                }
            )
        )
    ).anyTimes();
  }
}
