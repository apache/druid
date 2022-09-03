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

package org.apache.druid.segment.nested;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

public class NestedPathFinderTest
{

  private static final Map<String, Object> NESTER = ImmutableMap.of(
      "x", ImmutableList.of("a", "b", "c"),
      "y", ImmutableMap.of("a", "hello", "b", "world"),
      "z", "foo",
      "[sneaky]", "bar",
      "[also_sneaky]", ImmutableList.of(ImmutableMap.of("a", "x"), ImmutableMap.of("b", "y", "c", "z"))
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParseJqPath()
  {
    List<NestedPathPart> pathParts;

    pathParts = NestedPathFinder.parseJqPath(".");
    Assert.assertEquals(0, pathParts.size());

    // { "z" : "hello" }
    pathParts = NestedPathFinder.parseJqPath(".z");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("z", pathParts.get(0).getPartIdentifier());
    Assert.assertEquals(".\"z\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "z" : "hello" }
    pathParts = NestedPathFinder.parseJqPath(".\"z\"");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("z", pathParts.get(0).getPartIdentifier());
    Assert.assertEquals(".\"z\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "z" : "hello" }
    pathParts = NestedPathFinder.parseJqPath(".[\"z\"]");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("z", pathParts.get(0).getPartIdentifier());
    Assert.assertEquals(".\"z\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = NestedPathFinder.parseJqPath(".x[1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1]", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = NestedPathFinder.parseJqPath(".\"x\"[1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1]", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = NestedPathFinder.parseJqPath(".[\"x\"][1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1]", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "x" : { "1" : "hello" }}
    pathParts = NestedPathFinder.parseJqPath(".[\"x\"][\"1\"]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathField);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\".\"1\"", NestedPathFinder.toNormalizedJqPath(pathParts));


    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = NestedPathFinder.parseJqPath(".x[1].foo.bar");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = NestedPathFinder.parseJqPath(".x[1].\"foo\".bar");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = NestedPathFinder.parseJqPath(".[\"x\"][1].\"foo\"[\"bar\"]");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // make sure we chomp question marks
    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = NestedPathFinder.parseJqPath(".[\"x\"]?[1]?.foo?.\"bar\"?");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // { "x" : { "1" : { "foo" : { "bar" : "hello" }}}}
    pathParts = NestedPathFinder.parseJqPath(".\"x\"[\"1\"].\"foo\".\"bar\"");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathField);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\".\"1\".\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));

    // stress out the parser
    // { "x.y.z]?[\\\"]][]\" : { "13234.12[]][23" : { "f?o.o" : { ".b?.a.r.": "hello" }}}}
    pathParts = NestedPathFinder.parseJqPath(".[\"x.y.z]?[\\\"]][]\"]?[\"13234.12[]][23\"].\"f?o.o\"?[\".b?.a.r.\"]");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x.y.z]?[\\\"]][]", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathField);
    Assert.assertEquals("13234.12[]][23", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("f?o.o", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals(".b?.a.r.", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x.y.z]?[\\\"]][]\".\"13234.12[]][23\".\"f?o.o\".\".b?.a.r.\"", NestedPathFinder.toNormalizedJqPath(pathParts));
  }

  @Test
  public void testParseJsonPath()
  {
    List<NestedPathPart> pathParts;

    pathParts = NestedPathFinder.parseJsonPath("$.");
    Assert.assertEquals(0, pathParts.size());

    pathParts = NestedPathFinder.parseJsonPath("$");
    Assert.assertEquals(0, pathParts.size());

    // { "z" : "hello" }
    pathParts = NestedPathFinder.parseJsonPath("$.z");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("z", pathParts.get(0).getPartIdentifier());
    Assert.assertEquals(".\"z\"", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.z", NestedPathFinder.toNormalizedJsonPath(pathParts));

    // { "z" : "hello" }
    pathParts = NestedPathFinder.parseJsonPath("$['z']");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("z", pathParts.get(0).getPartIdentifier());
    Assert.assertEquals(".\"z\"", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.z", NestedPathFinder.toNormalizedJsonPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = NestedPathFinder.parseJsonPath("$.x[1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1]", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.x[1]", NestedPathFinder.toNormalizedJsonPath(pathParts));


    // { "x" : ["a", "b"]}
    pathParts = NestedPathFinder.parseJsonPath("$.x[-1]");
    Assert.assertEquals(2, pathParts.size());

    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("-1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\"[-1]", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.x[-1]", NestedPathFinder.toNormalizedJsonPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = NestedPathFinder.parseJsonPath("$['x'][1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1]", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.x[1]", NestedPathFinder.toNormalizedJsonPath(pathParts));


    // { "x" : { "1" : "hello" }}
    pathParts = NestedPathFinder.parseJsonPath("$['x']['1']");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathField);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertEquals(".\"x\".\"1\"", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.x.1", NestedPathFinder.toNormalizedJsonPath(pathParts));


    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = NestedPathFinder.parseJsonPath("$.x[1].foo.bar");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.x[1].foo.bar", NestedPathFinder.toNormalizedJsonPath(pathParts));

    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = NestedPathFinder.parseJsonPath("$.x[1]['foo'].bar");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.x[1].foo.bar", NestedPathFinder.toNormalizedJsonPath(pathParts));

    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = NestedPathFinder.parseJsonPath("$['x'][1].foo['bar']");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("foo", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals("bar", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", NestedPathFinder.toNormalizedJqPath(pathParts));
    Assert.assertEquals("$.x[1].foo.bar", NestedPathFinder.toNormalizedJsonPath(pathParts));

    // stress out the parser
    // { "x.y.z]?[\\\"]][]\" : { "13234.12[]][23" : { "f?o.o" : { ".b?.a.r.": "hello" }}}}
    pathParts = NestedPathFinder.parseJsonPath("$['x.y.z][\\']][]']['13234.12[]][23']['fo.o']['.b.a.r.']");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof NestedPathField);
    Assert.assertEquals("x.y.z][\\']][]", pathParts.get(0).getPartIdentifier());
    Assert.assertTrue(pathParts.get(1) instanceof NestedPathField);
    Assert.assertEquals("13234.12[]][23", pathParts.get(1).getPartIdentifier());
    Assert.assertTrue(pathParts.get(2) instanceof NestedPathField);
    Assert.assertEquals("fo.o", pathParts.get(2).getPartIdentifier());
    Assert.assertTrue(pathParts.get(3) instanceof NestedPathField);
    Assert.assertEquals(".b.a.r.", pathParts.get(3).getPartIdentifier());
    Assert.assertEquals(
        ".\"x.y.z][\\']][]\".\"13234.12[]][23\".\"fo.o\".\".b.a.r.\"",
        NestedPathFinder.toNormalizedJqPath(pathParts)
    );
    Assert.assertEquals(
        "$['x.y.z][\\']][]']['13234.12[]][23']['fo.o']['.b.a.r.']",
        NestedPathFinder.toNormalizedJsonPath(pathParts)
    );

    pathParts = NestedPathFinder.parseJsonPath("$['hell'o']");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertEquals("hell'o", pathParts.get(0).getPartIdentifier());
    Assert.assertEquals("$['hell'o']", NestedPathFinder.toNormalizedJsonPath(pathParts));
  }

  @Test
  public void testBadFormatMustStartWithDot()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, 'x.y' is not a valid 'jq' path: must start with '.'");
    NestedPathFinder.parseJqPath("x.y");
  }

  @Test
  public void testBadFormatNoDot()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage(".\"x\"\"y\"' is not a valid 'jq' path: path parts must be separated with '.'");
    NestedPathFinder.parseJqPath(".\"x\"\"y\"");
  }

  @Test
  public void testBadFormatWithDot2()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '..\"x\"' is not a valid 'jq' path: path parts separated by '.' must not be empty");
    NestedPathFinder.parseJqPath("..\"x\"");
  }

  @Test
  public void testBadFormatWithDot3()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x.[1]' is not a valid 'jq' path: invalid position 3 for '[', must not follow '.' or must be contained with '\"'");
    NestedPathFinder.parseJqPath(".x.[1]");
  }

  @Test
  public void testBadFormatWithDot4()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[1].[2]' is not a valid 'jq' path: invalid position 6 for '[', must not follow '.' or must be contained with '\"'");
    NestedPathFinder.parseJqPath(".x[1].[2]");
  }

  @Test
  public void testBadFormatNotANumber()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[.1]' is not a valid 'jq' path: expected number for array specifier got .1 instead. Use \"\" if this value was meant to be a field name");
    NestedPathFinder.parseJqPath(".x[.1]");
  }

  @Test
  public void testBadFormatUnclosedArray()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[1' is not a valid 'jq' path: unterminated '['");
    NestedPathFinder.parseJqPath(".x[1");
  }

  @Test
  public void testBadFormatUnclosedArray2()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[\"1\"' is not a valid 'jq' path: unterminated '['");
    NestedPathFinder.parseJqPath(".x[\"1\"");
  }

  @Test
  public void testBadFormatUnclosedQuote()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x.\"1' is not a valid 'jq' path: unterminated '\"'");
    NestedPathFinder.parseJqPath(".x.\"1");
  }

  @Test
  public void testBadFormatUnclosedQuote2()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[\"1]' is not a valid 'jq' path: unterminated '\"'");
    NestedPathFinder.parseJqPath(".x[\"1]");
  }



  @Test
  public void testPathSplitter()
  {
    List<NestedPathPart> pathParts;

    pathParts = NestedPathFinder.parseJqPath(".");
    Assert.assertEquals(NESTER, NestedPathFinder.find(NESTER, pathParts));
    Assert.assertNull(NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".z");
    Assert.assertEquals("foo", NestedPathFinder.find(NESTER, pathParts));
    Assert.assertEquals("foo", NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".x");
    Assert.assertEquals(NESTER.get("x"), NestedPathFinder.find(NESTER, pathParts));
    Assert.assertNull(NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".x[1]");
    Assert.assertEquals("b", NestedPathFinder.find(NESTER, pathParts));
    Assert.assertEquals("b", NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".x[-1]");
    Assert.assertEquals("c", NestedPathFinder.find(NESTER, pathParts));
    Assert.assertEquals("c", NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".x[-2]");
    Assert.assertEquals("b", NestedPathFinder.find(NESTER, pathParts));
    Assert.assertEquals("b", NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".x[-4]");
    Assert.assertNull(NestedPathFinder.find(NESTER, pathParts));
    Assert.assertNull(NestedPathFinder.findStringLiteral(NESTER, pathParts));

    // nonexistent
    pathParts = NestedPathFinder.parseJqPath(".x[1].y.z");
    Assert.assertNull(NestedPathFinder.find(NESTER, pathParts));
    Assert.assertNull(NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".y.a");
    Assert.assertEquals("hello", NestedPathFinder.find(NESTER, pathParts));
    Assert.assertEquals("hello", NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".y[1]");
    Assert.assertNull(NestedPathFinder.find(NESTER, pathParts));
    Assert.assertNull(NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".\"[sneaky]\"");
    Assert.assertEquals("bar", NestedPathFinder.find(NESTER, pathParts));
    Assert.assertEquals("bar", NestedPathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = NestedPathFinder.parseJqPath(".\"[also_sneaky]\"[1].c");
    Assert.assertEquals("z", NestedPathFinder.find(NESTER, pathParts));
    Assert.assertEquals("z", NestedPathFinder.findStringLiteral(NESTER, pathParts));
  }
}
