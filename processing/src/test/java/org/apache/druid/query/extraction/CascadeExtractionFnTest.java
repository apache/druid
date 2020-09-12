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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class CascadeExtractionFnTest extends InitializedNullHandlingTest
{
  private static final String[] PATHS = {
      "/druid/prod/historical",
      "/druid/prod/broker",
      "/druid/prod/coordinator",
      "/druid/demo/historical",
      "/druid/demo/broker",
      "/druid/demo/coordinator",
      "/dash/aloe",
      "/dash/baloo"
  };

  private final String regex = "/([^/]+)/";
  private final String function = "function(str) { return \"the \".concat(str) }";
  private final RegexDimExtractionFn regexDimExtractionFn = new RegexDimExtractionFn(regex, false, null);
  private final JavaScriptExtractionFn javascriptExtractionFn = new JavaScriptExtractionFn(
      function,
      true,
      JavaScriptConfig.getEnabledInstance()
  );
  private final SubstringDimExtractionFn substringDimExtractionFn = new SubstringDimExtractionFn(0, 7);
  private final String regexDimExtractionFnJson = "{ \"type\" : \"regex\", \"expr\" : \"/([^/]+)/\" , " +
                                                  "\"replaceMissingValue\": false, \"replaceMissingValueWith\": null}";
  private final String javascriptExtractionFnJson =
      "{ \"type\" : \"javascript\", \"function\" : \"function(str) { return \\\"the \\\".concat(str) }\" }";
  private final String substringDimExtractionFnJson = "{ \"type\" : \"substring\", \"index\" : 0, \"length\" : 7 }";

  @Test
  public void testRegexAndJavascriptAndSubstring()
  {
    ExtractionFn[] fns = new ExtractionFn[3];
    fns[0] = regexDimExtractionFn;
    fns[1] = javascriptExtractionFn;
    fns[2] = substringDimExtractionFn;

    CascadeExtractionFn cascadeExtractionFn = new CascadeExtractionFn(fns);

    Set<String> extracted = new LinkedHashSet<>();
    for (String path : PATHS) {
      extracted.add(cascadeExtractionFn.apply(path));
    }

    Set<String> expected = Sets.newLinkedHashSet(ImmutableList.of("the dru", "the das"));
    Assert.assertEquals(expected, extracted);
  }

  @Test
  public void testGetCacheKey()
  {
    ExtractionFn[] fns1 = new ExtractionFn[2];
    fns1[0] = javascriptExtractionFn;
    fns1[1] = regexDimExtractionFn;
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(fns1);

    ExtractionFn[] fns2 = new ExtractionFn[2];
    fns2[0] = regexDimExtractionFn;
    fns2[1] = javascriptExtractionFn;
    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(fns2);

    ExtractionFn[] fns3 = new ExtractionFn[3];
    fns3[0] = regexDimExtractionFn;
    fns3[1] = javascriptExtractionFn;
    fns3[2] = substringDimExtractionFn;
    CascadeExtractionFn cascadeExtractionFn3 = new CascadeExtractionFn(fns3);


    Assert.assertFalse(Arrays.equals(cascadeExtractionFn1.getCacheKey(), cascadeExtractionFn2.getCacheKey()));
    Assert.assertFalse(Arrays.equals(cascadeExtractionFn1.getCacheKey(), cascadeExtractionFn3.getCacheKey()));
    Assert.assertFalse(Arrays.equals(cascadeExtractionFn2.getCacheKey(), cascadeExtractionFn3.getCacheKey()));
  }

  @Test
  public void testHashCode()
  {
    ExtractionFn[] fns1 = new ExtractionFn[2];
    fns1[0] = javascriptExtractionFn;
    fns1[1] = regexDimExtractionFn;
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(fns1);

    ExtractionFn[] fns2 = new ExtractionFn[2];
    fns2[0] = regexDimExtractionFn;
    fns2[1] = javascriptExtractionFn;
    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(fns2);

    ExtractionFn[] fns3 = new ExtractionFn[3];
    fns3[0] = regexDimExtractionFn;
    fns3[1] = javascriptExtractionFn;
    fns3[2] = substringDimExtractionFn;
    CascadeExtractionFn cascadeExtractionFn3 = new CascadeExtractionFn(fns3);

    Assert.assertNotEquals(cascadeExtractionFn1.hashCode(), cascadeExtractionFn2.hashCode());
    Assert.assertNotEquals(cascadeExtractionFn1.hashCode(), cascadeExtractionFn3.hashCode());
    Assert.assertNotEquals(cascadeExtractionFn2.hashCode(), cascadeExtractionFn3.hashCode());
  }

  @Test
  public void testPreservesOrdering()
  {
    ExtractionFn[] fns1 = new ExtractionFn[1];
    fns1[0] = substringDimExtractionFn;
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(fns1);

    ExtractionFn[] fns2 = new ExtractionFn[2];
    fns2[0] = regexDimExtractionFn;
    fns2[1] = substringDimExtractionFn;
    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(fns2);

    ExtractionFn[] fns3 = new ExtractionFn[2];
    fns3[0] = substringDimExtractionFn;
    fns3[1] = javascriptExtractionFn;
    CascadeExtractionFn cascadeExtractionFn3 = new CascadeExtractionFn(fns3);

    Assert.assertTrue(cascadeExtractionFn1.preservesOrdering());
    Assert.assertFalse(cascadeExtractionFn2.preservesOrdering());
    Assert.assertFalse(cascadeExtractionFn3.preservesOrdering());
  }

  @Test
  public void testGetExtractionType()
  {
    ExtractionFn[] fns1 = new ExtractionFn[1];
    fns1[0] = javascriptExtractionFn;
    CascadeExtractionFn cascadeExtractionFn1 = new CascadeExtractionFn(fns1);

    ExtractionFn[] fns2 = new ExtractionFn[2];
    fns2[0] = regexDimExtractionFn;
    fns2[1] = javascriptExtractionFn;
    CascadeExtractionFn cascadeExtractionFn2 = new CascadeExtractionFn(fns2);

    Assert.assertTrue(cascadeExtractionFn1.getExtractionType() == ExtractionFn.ExtractionType.ONE_TO_ONE);
    Assert.assertTrue(cascadeExtractionFn2.getExtractionType() == ExtractionFn.ExtractionType.MANY_TO_ONE);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            JavaScriptConfig.class,
            JavaScriptConfig.getEnabledInstance()
        )
    );
    final String json = "{\"type\" : \"cascade\", \"extractionFns\": ["
                        +
                        regexDimExtractionFnJson
                        + ","
                        + javascriptExtractionFnJson
                        + ","
                        + substringDimExtractionFnJson
                        + "]}";

    CascadeExtractionFn cascadeExtractionFn = (CascadeExtractionFn) objectMapper.readValue(json, ExtractionFn.class);
    RegexDimExtractionFn regexDimExtractionFn =
        (RegexDimExtractionFn) objectMapper.readValue(regexDimExtractionFnJson, ExtractionFn.class);
    JavaScriptExtractionFn javascriptExtractionFn =
        (JavaScriptExtractionFn) objectMapper.readValue(javascriptExtractionFnJson, ExtractionFn.class);
    SubstringDimExtractionFn substringDimExtractionFn =
        (SubstringDimExtractionFn) objectMapper.readValue(substringDimExtractionFnJson, ExtractionFn.class);

    Assert.assertEquals(regexDimExtractionFn, cascadeExtractionFn.getExtractionFns()[0]);
    Assert.assertEquals(javascriptExtractionFn, cascadeExtractionFn.getExtractionFns()[1]);
    Assert.assertEquals(substringDimExtractionFn, cascadeExtractionFn.getExtractionFns()[2]);

    Assert.assertEquals(
        cascadeExtractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(cascadeExtractionFn),
            ExtractionFn.class
        )
    );
  }

  @Test
  public void testEqualsContract()
  {
    EqualsVerifier.forClass(CascadeExtractionFn.class)
                  .withPrefabValues(
                      CascadeExtractionFn.ChainedExtractionFn.class,
                      CascadeExtractionFn.DEFAULT_CHAINED_EXTRACTION_FN,
                      new CascadeExtractionFn.ChainedExtractionFn(
                          StrlenExtractionFn.instance(),
                          CascadeExtractionFn.DEFAULT_CHAINED_EXTRACTION_FN
                      )
                  )
                  .withNonnullFields("chainedExtractionFn")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testEqualsContractForChainedExtractionFn()
  {
    EqualsVerifier.forClass(CascadeExtractionFn.ChainedExtractionFn.class)
                  .withPrefabValues(
                      CascadeExtractionFn.ChainedExtractionFn.class,
                      CascadeExtractionFn.DEFAULT_CHAINED_EXTRACTION_FN,
                      new CascadeExtractionFn.ChainedExtractionFn(
                          StrlenExtractionFn.instance(),
                          CascadeExtractionFn.DEFAULT_CHAINED_EXTRACTION_FN
                      )
                  )
                  .withNonnullFields("fn")
                  .usingGetClass()
                  .verify();
  }
}
