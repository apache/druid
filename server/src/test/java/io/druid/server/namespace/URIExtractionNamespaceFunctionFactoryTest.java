/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespaceTest;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Collection;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 *
 */
@RunWith(Parameterized.class)
public class URIExtractionNamespaceFunctionFactoryTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
        new Object[]{
            ".gz",
            new Function<File, OutputStream>()
            {

              @Nullable
              @Override
              public OutputStream apply(@Nullable File outFile)
              {
                try {
                  final FileOutputStream fos = new FileOutputStream(outFile);
                  return new GZIPOutputStream(fos)
                  {
                    @Override
                    public void close() throws IOException
                    {
                      try {
                        super.close();
                      }
                      finally {
                        fos.close();
                      }
                    }
                  };
                }
                catch (IOException ex) {
                  throw Throwables.propagate(ex);
                }
              }
            }
        },
        new Object[]{
            "",
            new Function<File, OutputStream>()
            {

              @Nullable
              @Override
              public OutputStream apply(@Nullable File outFile)
              {
                try {
                  return new FileOutputStream(outFile);
                }
                catch (IOException ex) {
                  throw Throwables.propagate(ex);
                }
              }
            }
        }
    );
  }

  public URIExtractionNamespaceFunctionFactoryTest(
      String suffix,
      Function<File, OutputStream> outStreamSupplier
  )
  {
    this.suffix = suffix;
    this.outStreamSupplier = outStreamSupplier;
  }

  private final String suffix;
  private final Function<File, OutputStream> outStreamSupplier;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager manager;
  private File tmpFile;
  private URIExtractionNamespaceFunctionFactory factory;
  private URIExtractionNamespace namespace;

  @Before
  public void setUp() throws IOException
  {
    lifecycle = new Lifecycle();
    manager = new OnHeapNamespaceExtractionCacheManager(lifecycle);
    tmpFile = Files.createTempFile("druidTestURIExtractionNS", suffix).toFile();
    tmpFile.deleteOnExit();
    final ObjectMapper mapper = new DefaultObjectMapper();
    try (OutputStream ostream = outStreamSupplier.apply(tmpFile)) {
      try (OutputStreamWriter out = new OutputStreamWriter(ostream)) {
        out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of("foo", "bar")));
      }
    }
    factory = new URIExtractionNamespaceFunctionFactory(
        manager,
        new DefaultObjectMapper(),
        new DefaultObjectMapper(),
        ImmutableMap.<String, DataSegmentPuller>of("file", new LocalDataSegmentPuller())
    );
    namespace = new URIExtractionNamespace(
        "ns",
        tmpFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        )
    );
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
    tmpFile.delete();
  }

  @Test
  public void simpleTest() throws IOException
  {
    final Function<String, String> fn = factory.build(namespace);
    Assert.assertEquals(null, fn.apply("foo"));
    Assert.assertEquals(null, fn.apply("baz"));
    factory.getCachePopulator(namespace).run();
    Assert.assertEquals("bar", fn.apply("foo"));
    Assert.assertEquals(null, fn.apply("baz"));
  }

  @Test
  public void testLoadOnlyOnce() throws NoSuchFieldException, IllegalAccessException
  {

    final Function<String, String> fn = factory.build(namespace);
    Assert.assertEquals(null, fn.apply("foo"));
    Assert.assertEquals(null, fn.apply("baz"));

    Runnable populator = factory.getCachePopulator(namespace);

    final Field field = populator.getClass().getDeclaredField("lastCached");
    field.setAccessible(true);

    final long lastChecked = field.getLong(populator);

    populator.run();
    Assert.assertEquals("bar", fn.apply("foo"));
    Assert.assertEquals(null, fn.apply("baz"));
    final long postFirst = field.getLong(populator);
    Assert.assertNotEquals(lastChecked, postFirst);

    populator.run();
    Assert.assertEquals("bar", fn.apply("foo"));
    Assert.assertEquals(null, fn.apply("baz"));
    final long postSecond = field.getLong(populator);
    Assert.assertNotEquals(lastChecked, postSecond);
    Assert.assertEquals(postFirst, postSecond);
  }
}
