/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.extraction.extraction;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.JavascriptDimExtractionFn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class JavascriptDimExtractionFnTest
{
  private static final String[] testStrings = {
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
    DimExtractionFn dimExtractionFn = new JavascriptDimExtractionFn(function);

    for (String str : testStrings) {
      String res = dimExtractionFn.apply(str);
      Assert.assertEquals(str.substring(0, 3), res);
    }
  }

  @Test
  public void testCastingAndNull()
  {
    String function = "function(x) {\n  x = Number(x);\n  if(isNaN(x)) return null;\n  return Math.floor(x / 5) * 5;\n}";
    DimExtractionFn dimExtractionFn = new JavascriptDimExtractionFn(function);

    Iterator<String> it = Iterators.forArray("0", "5", "5", "10", null);

    for(String str : Lists.newArrayList("1", "5", "6", "10", "CA")) {
      String res = dimExtractionFn.apply(str);
      String expected = it.next();
      Assert.assertEquals(expected, res);
    }
  }

  @Test
  public void testJavascriptRegex()
  {
    String function = "function(str) { return str.replace(/[aeiou]/g, ''); }";
    DimExtractionFn dimExtractionFn = new JavascriptDimExtractionFn(function);

    Iterator it = Iterators.forArray("Qt", "Clgry", "Tky", "Stckhlm", "Vncvr", "Prtr", "Wllngtn", "Ontr");
    for (String str : testStrings) {
      String res = dimExtractionFn.apply(str);
      Assert.assertEquals(it.next(), res);
    }
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

    DimExtractionFn dimExtractionFn = new JavascriptDimExtractionFn(function);

    Iterator<String> inputs = Iterators.forArray("introducing", "exploratory", "analytics", "on", "large", "datasets");
    Iterator<String> it = Iterators.forArray("introduc", "exploratori", "analyt", "on", "larg", "dataset");

    while(inputs.hasNext()) {
      String res = dimExtractionFn.apply(inputs.next());
      Assert.assertEquals(it.next(), res);
    }
  }
}
