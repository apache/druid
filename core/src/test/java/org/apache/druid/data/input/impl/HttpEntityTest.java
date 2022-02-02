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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.PasswordProvider;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HttpEntityTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testOpenInputStream() throws IOException, URISyntaxException
  {
    URI url = new URI("https://druid.apache.org/data/wikipedia.json.gz");
    PasswordProvider passwordProvider = new DefaultPasswordProvider("");
    final InputStream inputStream = HttpEntity.openInputStream(url, "", passwordProvider, 0);
    File file = new File("resources/wikipedia_fetched.json.gz");
    File file1 = new File("resources/wikipedia.json.gz");
    FileUtils.copyInputStreamToFile(inputStream, file);
    Assert.assertTrue(FileUtils.contentEquals(file, file1));
  }
  @Test
  public void testPartialOpenInputStream() throws IOException, URISyntaxException
  {
    URI url = new URI("https://druid.apache.org/data/wikipedia.json.gz");
    PasswordProvider passwordProvider = new DefaultPasswordProvider("");
    final InputStream inputStream = HttpEntity.openInputStream(url, "", passwordProvider, 15);
    File file = new File("resources/wikipedia_fetched.json.gz");
    File file1 = new File("resources/wikipedia.json.gz");
    FileUtils.copyInputStreamToFile(inputStream, file);
    Assert.assertFalse(FileUtils.contentEquals(file, file1));
  }

  long offset = 15;
  URI uri = Mockito.mock(URI.class);
  URL url = Mockito.mock(URL.class);
  URLConnection urlConnection = Mockito.mock(URLConnection.class);
  InputStream inputStreamMock = Mockito.mock(InputStream.class);
  String contentRange = StringUtils.format("bytes %d-%d/%d", offset, 1000, 1000);

  @Test
  public void testWithServerAcceptRanges() throws IOException
  {
    Mockito.when(uri.toURL()).thenReturn(url);
    Mockito.when(url.openConnection()).thenReturn(urlConnection);
    Mockito.when(urlConnection.getHeaderField(HttpHeaders.CONTENT_RANGE)).thenReturn("posts 12-22/22");
    Mockito.when(urlConnection.getInputStream()).thenReturn(inputStreamMock);
    Mockito.when(inputStreamMock.skip(offset)).thenReturn(offset);
    HttpEntity.openInputStream(uri, "", null, offset);
    Mockito.verify(inputStreamMock, Mockito.times(1)).skip(offset);
  }
  @Test
  public void testWithoutServerAcceptRanges() throws IOException
  {
    Mockito.when(uri.toURL()).thenReturn(url);
    Mockito.when(url.openConnection()).thenReturn(urlConnection);
    Mockito.when(urlConnection.getHeaderField(HttpHeaders.CONTENT_RANGE)).thenReturn(contentRange);
    Mockito.when(urlConnection.getInputStream()).thenReturn(inputStreamMock);
    Mockito.when(inputStreamMock.skip(offset)).thenReturn(offset);
    HttpEntity.openInputStream(uri, "", null, offset);
    Mockito.verify(inputStreamMock, Mockito.times(0)).skip(offset);
  }
}
