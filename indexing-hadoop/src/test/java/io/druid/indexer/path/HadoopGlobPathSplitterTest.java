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

package io.druid.indexer.path;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class HadoopGlobPathSplitterTest
{
  @Test
  public void testGlobSplitting() throws Exception
  {
    String path = "/a/b/c";
    List<String> expected = ImmutableList.of(
        "/a/b/c"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "/a/b/c,/d/e";
    expected = ImmutableList.of(
        "/a/b/c",
        "/d/e"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "/a/b/*.c,/d/*.e";
    expected = ImmutableList.of(
        "/a/b/*.c",
        "/d/*.e"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "/a/b/c,/d/e,/f/g";
    expected = ImmutableList.of(
        "/a/b/c",
        "/d/e",
        "/f/g"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "/a/b/{c,d}";
    expected = ImmutableList.of(
        "/a/b/c",
        "/a/b/d"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "/a/b/{c,d}/e";
    expected = ImmutableList.of(
        "/a/b/c/e",
        "/a/b/d/e"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "{c,d}";
    expected = ImmutableList.of(
        "c",
        "d"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "{c,d}/e";
    expected = ImmutableList.of(
        "c/e",
        "d/e"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "/a/b/{c,d},/a/b/{c,d}/e,{c,d},{c,d}/e";
    expected = ImmutableList.of(
        "/a/b/c",
        "/a/b/d",
        "/a/b/c/e",
        "/a/b/d/e",
        "c",
        "d",
        "c/e",
        "d/e"
    );
    Assert.assertEquals(expected, splitGlob(path));

    path = "/a/b/{c/{d,e/{f,g},h},i}/{j,k}/l";
    expected = ImmutableList.of(
        "/a/b/c/d/j/l",
        "/a/b/c/d/k/l",
        "/a/b/c/e/f/j/l",
        "/a/b/c/e/f/k/l",
        "/a/b/c/e/g/j/l",
        "/a/b/c/e/g/k/l",
        "/a/b/c/h/j/l",
        "/a/b/c/h/k/l",
        "/a/b/i/j/l",
        "/a/b/i/k/l"
    );
    Assert.assertEquals(expected, splitGlob(path));


    path = "";
    expected = ImmutableList.of("");
    Assert.assertEquals(expected, splitGlob(path));

    path = "{}";
    expected = ImmutableList.of("");
    Assert.assertEquals(expected, splitGlob(path));
  }

  private static List<String> splitGlob(String path)
  {
    return Lists.newArrayList(HadoopGlobPathSplitter.splitGlob(path));
  }
}
