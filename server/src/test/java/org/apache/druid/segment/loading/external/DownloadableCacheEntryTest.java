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

package org.apache.druid.segment.loading.external;

import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DownloadableCacheEntryTest
{
  @Test
  public void test_simpleName_splitInHalf()
  {
    final String path = "foo";
    final String expected = "f-" + hash(path) + "-oo";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_singleExtension_suffixIsExtension()
  {
    final String path = "foo.gz";
    final String expected = "foo-" + hash(path) + "-.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_chainedExtensionsShortBasename_suffixIsFullExtensionChain()
  {
    final String path = "a.log.gz";
    final String expected = "a-" + hash(path) + "-.log.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_chainedExtensionsLongBasename_suffixIncludesFullExtensionChain()
  {
    final String path = "container.log.gz";
    final String expected = "containe-" + hash(path) + "-r.log.gz";
    final String result = DownloadableCacheEntry.sanitizePath(path);
    Assertions.assertEquals(expected, result);
    Assertions.assertTrue(result.endsWith(".log.gz"));
  }

  @Test
  public void test_uriWithExtensions_slashesReplacedAndSuffixEndsWithExtensionChain()
  {
    final String path = "file:/var/folders/raw/container.log.gz";
    final String expected = "file__var_folders_r-" + hash(path) + "-aw_container.log.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_nonAlnumChar_breaksExtensionChain_onlyTrailingAlnumExtensionsCount()
  {
    final String path = "foo.my_thing.gz";
    final String expected = "foo.my_-" + hash(path) + "-thing.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_underscoreInBasename_noTrailingExtension_splitInHalf()
  {
    final String path = "foo.bar_baz";
    final String expected = "foo.b-" + hash(path) + "-ar_baz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_dotsInDirectoryComponents_ignored_basenameHasNoExtension()
  {
    final String path = "foo.bar/baz";
    final String expected = "foo.b-" + hash(path) + "-ar_baz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_dotsInDirectoryAndBasename_onlyBasenameExtensionsConsidered()
  {
    final String path = "foo.bar/baz.qux";
    final String expected = "foo.bar-" + hash(path) + "-_baz.qux";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_trailingDot_notTreatedAsExtension()
  {
    final String path = "foo.bar.";
    final String expected = "foo.-" + hash(path) + "-bar.";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_dotfile_leadingDotNotTreatedAsExtension()
  {
    final String path = "foo/.gitignore";
    final String expected = "foo_.gi-" + hash(path) + "-tignore";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_dotfileWithExtension_leadingDotIgnoredOnlyTrailingExtensionUsed()
  {
    final String path = "foo/.hidden.gz";
    final String expected = "foo_.hi-" + hash(path) + "-dden.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_basenameIsBareDotfile_noExtension()
  {
    final String path = ".gitignore";
    final String expected = ".giti-" + hash(path) + "-gnore";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_backslashesNormalizedToSlashes()
  {
    final String path = "foo\\bar\\baz.gz";
    final String expected = "foo_bar-" + hash(path) + "-_baz.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_leadingSlash_stripped()
  {
    final String path = "/foo/bar.gz";
    final String expected = "foo_b-" + hash(path) + "-ar.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_trailingSlash_dropped()
  {
    final String path = "foo/bar/";
    final String expected = "foo-" + hash(path) + "-_bar";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_longPath_truncatedToFirst50AndLast50_suffixEndsWithExtension()
  {
    final StringBuilder pathBuilder = new StringBuilder("/aaaa");
    while (pathBuilder.length() < 200) {
      pathBuilder.append("/bbbb");
    }
    pathBuilder.append("/finalfile.gz");
    final String path = pathBuilder.toString();

    final String result = DownloadableCacheEntry.sanitizePath(path);

    Assertions.assertTrue(
        result.endsWith(".gz"),
        "truncated output should still end with the original extension, got: " + result
    );
    Assertions.assertTrue(
        result.contains("-" + hash(path) + "-"),
        "hashCode should be embedded between '-' delimiters between the prefix and suffix, got: " + result
    );
    Assertions.assertEquals(
        100 + 2 + hash(path).length(),
        result.length(),
        "kept portion should be exactly 100 chars (50 each side) plus delimiters and hashcode"
    );
  }

  @Test
  public void test_disallowedChars_replacedWithUnderscores()
  {
    final String path = "foo bar*baz?.gz";
    final String expected = "foo_bar-" + hash(path) + "-_baz_.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_hyphenPreserved()
  {
    final String path = "foo-bar.gz";
    final String expected = "foo-b-" + hash(path) + "-ar.gz";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_emptyPath_resultIsJustHashWithDelimiters()
  {
    final String path = "";
    final String expected = "-" + hash(path) + "-";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  @Test
  public void test_slashOnly_resultIsJustHashWithDelimiters()
  {
    final String path = "/";
    final String expected = "-" + hash(path) + "-";
    Assertions.assertEquals(expected, DownloadableCacheEntry.sanitizePath(path));
  }

  /**
   * Computes the disambiguating hash the way {@link DownloadableCacheEntry#sanitizePath(String)} does, so the tests
   * below can assert on the prefix/suffix splitting (the actual behavior under test) without hardcoding hash values.
   */
  private static String hash(final String path)
  {
    return Hashing.sha512().hashUnencodedChars(path).toString().substring(0, 16);
  }
}
