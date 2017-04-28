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

package io.druid.query.dimension;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.lookup.LookupExtractor;
import io.druid.query.lookup.LookupExtractorFactoryContainer;
import io.druid.query.lookup.LookupReferencesManager;
import io.druid.query.lookup.MapLookupExtractorFactory;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

@RunWith(JUnitParamsRunner.class)
public class LookupDimensionSpecTest
{
  private static final Map<String, String> STRING_MAP = ImmutableMap.of("key", "value", "key2", "value2");
  private static LookupExtractor MAP_LOOKUP_EXTRACTOR = new MapLookupExtractor(
      STRING_MAP, true);

  private static final LookupReferencesManager LOOKUP_REF_MANAGER = EasyMock.createMock(LookupReferencesManager.class);

  static {
    EasyMock.expect(LOOKUP_REF_MANAGER.get(EasyMock.eq("lookupName"))).andReturn(
        new LookupExtractorFactoryContainer(
            "v0",
            new MapLookupExtractorFactory(STRING_MAP, false)
        )
    ).anyTimes();
    EasyMock.replay(LOOKUP_REF_MANAGER);
  }

  private final DimensionSpec lookupDimSpec = new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, null,
                                                                      true
  );


  @Parameters
  @Test
  public void testSerDesr(DimensionSpec lookupDimSpec) throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    InjectableValues injectableValues = new InjectableValues.Std().addValue(
        LookupReferencesManager.class,
        LOOKUP_REF_MANAGER
    );
    String serLookup = mapper.writeValueAsString(lookupDimSpec);
    Assert.assertEquals(lookupDimSpec, mapper.reader(DimensionSpec.class).with(injectableValues).readValue(serLookup));
  }

  private Object[] parametersForTestSerDesr()
  {
    return new Object[]{
        new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, null, null, true),
        new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, "Missing_value", null, null, true),
        new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, null, true),
        new LookupDimensionSpec("dimName", "outputName", null, false, null, "name", LOOKUP_REF_MANAGER, true)
    };
  }

  @Test(expected = Exception.class)
  public void testExceptionWhenNameAndLookupNotNull()
  {
    new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, "replace", "name", null, true);
  }

  @Test(expected = Exception.class)
  public void testExceptionWhenNameAndLookupNull()
  {
    new LookupDimensionSpec("dimName", "outputName", null, false, "replace", "", null, true);
  }

  @Test
  public void testGetDimension()
  {
    Assert.assertEquals("dimName", lookupDimSpec.getDimension());
  }

  @Test
  public void testGetOutputName()
  {
    Assert.assertEquals("outputName", lookupDimSpec.getOutputName());
  }

  public Object[] parametersForTestApply()
  {
    return new Object[]{
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, true, null, "lookupName", LOOKUP_REF_MANAGER, true),
            STRING_MAP
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, null, null, true),
            STRING_MAP
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, null, true),
            ImmutableMap.of("not there", "")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, false, null, "lookupName", LOOKUP_REF_MANAGER, true),
            ImmutableMap.of("not there", "")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, "Missing_value", null, null,
                                    true
            ),
            ImmutableMap.of("not there", "Missing_value")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, false, "Missing_value", "lookupName", LOOKUP_REF_MANAGER,
                                    true
            ),
            ImmutableMap.of("not there", "Missing_value")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, true, null, "lookupName", LOOKUP_REF_MANAGER, true),
            ImmutableMap.of("not there", "not there")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, "", null, true),
            ImmutableMap.of("not there", "not there")
        }

    };
  }

  @Test
  @Parameters
  public void testApply(DimensionSpec dimensionSpec, Map<String, String> map)
  {
    for (Map.Entry<String, String> entry : map.entrySet()
        ) {
      Assert.assertEquals(Strings.emptyToNull(entry.getValue()), dimensionSpec.getExtractionFn().apply(entry.getKey()));
    }
  }

  public Object[] parametersForTestGetCacheKey()
  {
    return new Object[]{
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, null, null, true),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, "Missing_value", null, null,
                                    true
            ),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName2", MAP_LOOKUP_EXTRACTOR, false, null, null, null, true),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName2", "outputName2", MAP_LOOKUP_EXTRACTOR, false, null, null, null, true),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, null, true),
            true
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, false, null, "name", LOOKUP_REF_MANAGER, true),
            false
        }
    };
  }

  @Test
  @Parameters
  public void testGetCacheKey(DimensionSpec dimensionSpec, boolean expectedResult)
  {
    Assert.assertEquals(expectedResult, Arrays.equals(lookupDimSpec.getCacheKey(), dimensionSpec.getCacheKey()));
  }

  @Test
  public void testPreservesOrdering()
  {
    Assert.assertFalse(lookupDimSpec.preservesOrdering());
  }

  @Test
  public void testIsOneToOne()
  {
    Assert.assertEquals(lookupDimSpec.getExtractionFn().getExtractionType(), ExtractionFn.ExtractionType.ONE_TO_ONE);
  }
}
