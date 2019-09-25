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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.js.JavaScriptConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Iterator;

public class JavaScriptExtractionFnTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final String[] TEST_STRINGS = {
      "Quito",
      "Calgary",
      "Tokyo",
      "Stockholm",
      "Vancouver",
      "Pretoria",
      "Wellington",
      "Ontario"
  };

  @Test
  public void testJavascriptSubstring()
  {
    String function = "function(str) { return str.substring(0,3); }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(function, false, JavaScriptConfig.getEnabledInstance());

    for (String str : TEST_STRINGS) {
      String res = extractionFn.apply(str);
      Assert.assertEquals(str.substring(0, 3), res);
    }
  }

  @Test
  public void testJavascriptNotAllowed()
  {
    String function = "function(str) { return str.substring(0,3); }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(function, false, new JavaScriptConfig(false));

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("JavaScript is disabled");
    extractionFn.apply("hey");
    Assert.assertTrue(false);
  }

  @Test
  public void testTimeExample()
  {
    String utcHour = "function(t) {\nreturn 'Second ' + Math.floor((t % 60000) / 1000);\n}";
    final long millis = DateTimes.of("2015-01-02T13:00:59.999Z").getMillis();
    Assert.assertEquals("Second 59", new JavaScriptExtractionFn(utcHour, false, JavaScriptConfig.getEnabledInstance()).apply(millis));
  }

  @Test
  public void testLongs()
  {
    String typeOf = "function(x) {\nreturn typeof x\n}";
    Assert.assertEquals("number", new JavaScriptExtractionFn(typeOf, false, JavaScriptConfig.getEnabledInstance()).apply(1234L));
  }

  @Test
  public void testFloats()
  {
    String typeOf = "function(x) {\nreturn typeof x\n}";
    Assert.assertEquals("number", new JavaScriptExtractionFn(typeOf, false, JavaScriptConfig.getEnabledInstance()).apply(1234.0));
  }

  @Test
  public void testCastingAndNull()
  {
    String function = "function(x) {\n  x = Number(x);\n  if (isNaN(x)) return null;\n  return Math.floor(x / 5) * 5;\n}";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(function, false, JavaScriptConfig.getEnabledInstance());

    Iterator<String> it = Iterators.forArray("0", "5", "5", "10", null);

    for (String str : Lists.newArrayList("1", "5", "6", "10", "CA")) {
      String res = extractionFn.apply(str);
      String expected = it.next();
      Assert.assertEquals(expected, res);
    }
  }

  @Test
  public void testJavascriptRegex()
  {
    String function = "function(str) { return str.replace(/[aeiou]/g, ''); }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(function, false, JavaScriptConfig.getEnabledInstance());

    Iterator it = Iterators.forArray("Qt", "Clgry", "Tky", "Stckhlm", "Vncvr", "Prtr", "Wllngtn", "Ontr");
    for (String str : TEST_STRINGS) {
      String res = extractionFn.apply(str);
      Assert.assertEquals(it.next(), res);
    }
  }

  @Test
  public void testJavascriptIsNull()
  {
    String function = "function(x) { if (x == null) { return 'yes'; } else { return 'no' } }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(function, false, JavaScriptConfig.getEnabledInstance());

    Assert.assertEquals("yes", extractionFn.apply((String) null));
    Assert.assertEquals("yes", extractionFn.apply((Object) null));
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals("yes", extractionFn.apply(""));
    } else {
      Assert.assertEquals("no", extractionFn.apply(""));
    }
    Assert.assertEquals("no", extractionFn.apply("abc"));
    Assert.assertEquals("no", extractionFn.apply(new Object()));
    Assert.assertEquals("no", extractionFn.apply(1));
  }

  @Test
  public void testJavaScriptPorterStemmer()
  {
    // JavaScript porter stemmer adapted from
    // https://github.com/kristopolous/Porter-Stemmer/blob/e990a8d456510571d1ef9ef923d2a30a94679e13/PorterStemmer1980.js
    String function = "function(w) {"
                      + "var step2list = {\n"
                      + "      \"ational\" : \"ate\",\n"
                      + "      \"tional\" : \"tion\",\n"
                      + "      \"enci\" : \"ence\",\n"
                      + "      \"anci\" : \"ance\",\n"
                      + "      \"izer\" : \"ize\",\n"
                      + "      \"bli\" : \"ble\",\n"
                      + "      \"alli\" : \"al\",\n"
                      + "      \"entli\" : \"ent\",\n"
                      + "      \"eli\" : \"e\",\n"
                      + "      \"ousli\" : \"ous\",\n"
                      + "      \"ization\" : \"ize\",\n"
                      + "      \"ation\" : \"ate\",\n"
                      + "      \"ator\" : \"ate\",\n"
                      + "      \"alism\" : \"al\",\n"
                      + "      \"iveness\" : \"ive\",\n"
                      + "      \"fulness\" : \"ful\",\n"
                      + "      \"ousness\" : \"ous\",\n"
                      + "      \"aliti\" : \"al\",\n"
                      + "      \"iviti\" : \"ive\",\n"
                      + "      \"biliti\" : \"ble\",\n"
                      + "      \"logi\" : \"log\"\n"
                      + "    },\n"
                      + "\n"
                      + "    step3list = {\n"
                      + "      \"icate\" : \"ic\",\n"
                      + "      \"ative\" : \"\",\n"
                      + "      \"alize\" : \"al\",\n"
                      + "      \"iciti\" : \"ic\",\n"
                      + "      \"ical\" : \"ic\",\n"
                      + "      \"ful\" : \"\",\n"
                      + "      \"ness\" : \"\"\n"
                      + "    },\n"
                      + "\n"
                      + "    c = \"[^aeiou]\",          // consonant\n"
                      + "    v = \"[aeiouy]\",          // vowel\n"
                      + "    C = c + \"[^aeiouy]*\",    // consonant sequence\n"
                      + "    V = v + \"[aeiou]*\",      // vowel sequence\n"
                      + "\n"
                      + "    mgr0 = \"^(\" + C + \")?\" + V + C,               // [C]VC... is m>0\n"
                      + "    meq1 = \"^(\" + C + \")?\" + V + C + \"(\" + V + \")?$\",  // [C]VC[V] is m=1\n"
                      + "    mgr1 = \"^(\" + C + \")?\" + V + C + V + C,       // [C]VCVC... is m>1\n"
                      + "    s_v = \"^(\" + C + \")?\" + v;     "
                      + ""
                      + "var\n"
                      + "      stem,\n"
                      + "      suffix,\n"
                      + "      firstch,\n"
                      + "      re,\n"
                      + "      re2,\n"
                      + "      re3,\n"
                      + "      re4,\n"
                      + "      debugFunction,\n"
                      + "      origword = w;\n"
                      + "\n"
                      + "\n"
                      + "    if (w.length < 3) { return w; }\n"
                      + "\n"
                      + "    firstch = w.substr(0,1);\n"
                      + "    if (firstch == \"y\") {\n"
                      + "      w = firstch.toUpperCase() + w.substr(1);\n"
                      + "    }\n"
                      + "\n"
                      + "    // Step 1a\n"
                      + "    re = /^(.+?)(ss|i)es$/;\n"
                      + "    re2 = /^(.+?)([^s])s$/;\n"
                      + "\n"
                      + "    if (re.test(w)) { \n"
                      + "      w = w.replace(re,\"$1$2\"); \n"
                      + "\n"
                      + "    } else if (re2.test(w)) {\n"
                      + "      w = w.replace(re2,\"$1$2\"); \n"
                      + "    }\n"
                      + "\n"
                      + "    // Step 1b\n"
                      + "    re = /^(.+?)eed$/;\n"
                      + "    re2 = /^(.+?)(ed|ing)$/;\n"
                      + "    if (re.test(w)) {\n"
                      + "      var fp = re.exec(w);\n"
                      + "      re = new RegExp(mgr0);\n"
                      + "      if (re.test(fp[1])) {\n"
                      + "        re = /.$/;\n"
                      + "        w = w.replace(re,\"\");\n"
                      + "      }\n"
                      + "    } else if (re2.test(w)) {\n"
                      + "      var fp = re2.exec(w);\n"
                      + "      stem = fp[1];\n"
                      + "      re2 = new RegExp(s_v);\n"
                      + "      if (re2.test(stem)) {\n"
                      + "        w = stem;\n"
                      + "\n"
                      + "        re2 = /(at|bl|iz)$/;\n"
                      + "        re3 = new RegExp(\"([^aeiouylsz])\\\\1$\");\n"
                      + "        re4 = new RegExp(\"^\" + C + v + \"[^aeiouwxy]$\");\n"
                      + "\n"
                      + "        if (re2.test(w)) { \n"
                      + "          w = w + \"e\"; \n"
                      + "\n"
                      + "        } else if (re3.test(w)) { \n"
                      + "          re = /.$/; \n"
                      + "          w = w.replace(re,\"\"); \n"
                      + "\n"
                      + "        } else if (re4.test(w)) { \n"
                      + "          w = w + \"e\"; \n"
                      + "        }\n"
                      + "      }\n"
                      + "    }\n"
                      + "\n"
                      + "    // Step 1c\n"
                      + "    re = new RegExp(\"^(.*\" + v + \".*)y$\");\n"
                      + "    if (re.test(w)) {\n"
                      + "      var fp = re.exec(w);\n"
                      + "      stem = fp[1];\n"
                      + "      w = stem + \"i\";\n"
                      + "    }\n"
                      + "\n"
                      + "    // Step 2\n"
                      + "    re = /^(.+?)(ational|tional|enci|anci|izer|bli|alli|entli|eli|ousli|ization|ation|ator|alism|iveness|fulness|ousness|aliti|iviti|biliti|logi)$/;\n"
                      + "    if (re.test(w)) {\n"
                      + "      var fp = re.exec(w);\n"
                      + "      stem = fp[1];\n"
                      + "      suffix = fp[2];\n"
                      + "      re = new RegExp(mgr0);\n"
                      + "      if (re.test(stem)) {\n"
                      + "        w = stem + step2list[suffix];\n"
                      + "      }\n"
                      + "    }\n"
                      + "\n"
                      + "    // Step 3\n"
                      + "    re = /^(.+?)(icate|ative|alize|iciti|ical|ful|ness)$/;\n"
                      + "    if (re.test(w)) {\n"
                      + "      var fp = re.exec(w);\n"
                      + "      stem = fp[1];\n"
                      + "      suffix = fp[2];\n"
                      + "      re = new RegExp(mgr0);\n"
                      + "      if (re.test(stem)) {\n"
                      + "        w = stem + step3list[suffix];\n"
                      + "      }\n"
                      + "    }\n"
                      + "\n"
                      + "    // Step 4\n"
                      + "    re = /^(.+?)(al|ance|ence|er|ic|able|ible|ant|ement|ment|ent|ou|ism|ate|iti|ous|ive|ize)$/;\n"
                      + "    re2 = /^(.+?)(s|t)(ion)$/;\n"
                      + "    if (re.test(w)) {\n"
                      + "      var fp = re.exec(w);\n"
                      + "      stem = fp[1];\n"
                      + "      re = new RegExp(mgr1);\n"
                      + "      if (re.test(stem)) {\n"
                      + "        w = stem;\n"
                      + "      }\n"
                      + "    } else if (re2.test(w)) {\n"
                      + "      var fp = re2.exec(w);\n"
                      + "      stem = fp[1] + fp[2];\n"
                      + "      re2 = new RegExp(mgr1);\n"
                      + "      if (re2.test(stem)) {\n"
                      + "        w = stem;\n"
                      + "      }\n"
                      + "    }\n"
                      + "\n"
                      + "    // Step 5\n"
                      + "    re = /^(.+?)e$/;\n"
                      + "    if (re.test(w)) {\n"
                      + "      var fp = re.exec(w);\n"
                      + "      stem = fp[1];\n"
                      + "      re = new RegExp(mgr1);\n"
                      + "      re2 = new RegExp(meq1);\n"
                      + "      re3 = new RegExp(\"^\" + C + v + \"[^aeiouwxy]$\");\n"
                      + "      if (re.test(stem) || (re2.test(stem) && !(re3.test(stem)))) {\n"
                      + "        w = stem;\n"
                      + "      }\n"
                      + "    }\n"
                      + "\n"
                      + "    re = /ll$/;\n"
                      + "    re2 = new RegExp(mgr1);\n"
                      + "    if (re.test(w) && re2.test(w)) {\n"
                      + "      re = /.$/;\n"
                      + "      w = w.replace(re,\"\");\n"
                      + "    }\n"
                      + "\n"
                      + "    // and turn initial Y back to y\n"
                      + "    if (firstch == \"y\") {\n"
                      + "      w = firstch.toLowerCase() + w.substr(1);\n"
                      + "    }\n"
                      + "\n"
                      + "\n"
                      + "    return w;"
                      + ""
                      + "}";

    ExtractionFn extractionFn = new JavaScriptExtractionFn(function, false, JavaScriptConfig.getEnabledInstance());

    Iterator<String> inputs = Iterators.forArray("introducing", "exploratory", "analytics", "on", "large", "datasets");
    Iterator<String> it = Iterators.forArray("introduc", "exploratori", "analyt", "on", "larg", "dataset");

    while (inputs.hasNext()) {
      String res = extractionFn.apply(inputs.next());
      Assert.assertEquals(it.next(), res);
    }
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

    final String json = "{ \"type\" : \"javascript\", \"function\" : \"function(str) { return str.substring(0,3); }\" }";
    JavaScriptExtractionFn extractionFn = (JavaScriptExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assert.assertEquals("function(str) { return str.substring(0,3); }", extractionFn.getFunction());

    // round trip
    Assert.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }

  @Test
  public void testInjective()
  {
    Assert.assertEquals(ExtractionFn.ExtractionType.MANY_TO_ONE, new JavaScriptExtractionFn("function(str) { return str; }", false, JavaScriptConfig.getEnabledInstance()).getExtractionType());
    Assert.assertEquals(ExtractionFn.ExtractionType.ONE_TO_ONE, new JavaScriptExtractionFn("function(str) { return str; }", true, JavaScriptConfig.getEnabledInstance()).getExtractionType());
  }
}
