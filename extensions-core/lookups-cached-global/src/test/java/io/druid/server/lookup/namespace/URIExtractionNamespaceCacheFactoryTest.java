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

package io.druid.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.query.lookup.namespace.URIExtractionNamespaceTest;
import io.druid.segment.loading.LocalFileTimestampVersionFinder;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManagersTest;
import io.druid.server.lookup.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import io.druid.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import io.druid.server.metrics.NoopServiceEmitter;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 *
 */
@RunWith(Parameterized.class)
public class URIExtractionNamespaceCacheFactoryTest
{
  private static final String FAKE_SCHEME = "wabblywoo";
  private static final Map<String, SearchableVersionedDataFinder> FINDERS = ImmutableMap.<String, SearchableVersionedDataFinder>of(
      "file",
      new LocalFileTimestampVersionFinder(),
      FAKE_SCHEME,
      new LocalFileTimestampVersionFinder()
      {
        URI fixURI(URI uri)
        {
          final URI newURI;
          try {
            newURI = new URI(
                "file",
                uri.getUserInfo(),
                uri.getHost(),
                uri.getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment()
            );
          }
          catch (URISyntaxException e) {
            throw Throwables.propagate(e);
          }
          return newURI;
        }

        @Override
        public String getVersion(URI uri)
        {
          return super.getVersion(fixURI(uri));
        }

        @Override
        public InputStream getInputStream(URI uri) throws IOException
        {
          return super.getInputStream(fixURI(uri));
        }
      }
  );

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> getParameters() throws NoSuchMethodException
  {
    final List<Object[]> compressionParams = ImmutableList.of(
        new Object[]{
            ".dat",
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
        },
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
        }
    );

    final List<Constructor<? extends NamespaceExtractionCacheManager>> cacheConstructors = ImmutableList.<Constructor<? extends NamespaceExtractionCacheManager>>of(
        OnHeapNamespaceExtractionCacheManager.class.getConstructor(
            Lifecycle.class,
            ServiceEmitter.class,
            Map.class
        ),
        OffHeapNamespaceExtractionCacheManager.class.getConstructor(
            Lifecycle.class,
            ServiceEmitter.class,
            Map.class
        )
    );
    return new Iterable<Object[]>()
    {
      @Override
      public Iterator<Object[]> iterator()
      {
        return new Iterator<Object[]>()
        {
          Iterator<Object[]> compressionIt = compressionParams.iterator();
          Iterator<Constructor<? extends NamespaceExtractionCacheManager>> cacheConstructorIt = cacheConstructors.iterator();
          Object[] compressions = compressionIt.next();

          @Override
          public boolean hasNext()
          {
            return compressionIt.hasNext() || cacheConstructorIt.hasNext();
          }

          @Override
          public Object[] next()
          {
            if (cacheConstructorIt.hasNext()) {
              Constructor<? extends NamespaceExtractionCacheManager> constructor = cacheConstructorIt.next();
              final NamespaceExtractionCacheManager manager;
              try {
                manager = constructor.newInstance(
                    new Lifecycle(),
                    new NoopServiceEmitter(),
                    new HashMap<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>>()
                );
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
              ConcurrentHashMap<String, Function<String, String>> fnCache = new ConcurrentHashMap<String, Function<String, String>>();
              try {
                return new Object[]{
                    String.format(
                        "[%s]:[%s]",
                        compressions[0],
                        manager.getClass().getCanonicalName()
                    ), compressions[0], compressions[1], constructor
                };
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            } else {
              cacheConstructorIt = cacheConstructors.iterator();
              compressions = compressionIt.next();
              return next();
            }
          }

          @Override
          public void remove()
          {
            throw new UOE("Cannot remove");
          }
        };
      }
    };
  }

  public URIExtractionNamespaceCacheFactoryTest(
      String friendlyName,
      String suffix,
      Function<File, OutputStream> outStreamSupplier,
      Constructor<? extends NamespaceExtractionCacheManager> cacheManagerConstructor
  ) throws IllegalAccessException, InvocationTargetException, InstantiationException
  {
    final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> namespaceFunctionFactoryMap = new HashMap<>();
    this.suffix = suffix;
    this.outStreamSupplier = outStreamSupplier;
    this.lifecycle = new Lifecycle();
    this.manager = cacheManagerConstructor.newInstance(
        lifecycle,
        new NoopServiceEmitter(),
        namespaceFunctionFactoryMap
    );
    namespaceFunctionFactoryMap.put(
        URIExtractionNamespace.class,

        new URIExtractionNamespaceCacheFactory(FINDERS)
    );
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final String suffix;
  private final Function<File, OutputStream> outStreamSupplier;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager manager;
  private File tmpFile;
  private File tmpFileParent;
  private URIExtractionNamespaceCacheFactory factory;
  private URIExtractionNamespace namespace;
  private String id;

  @Before
  public void setUp() throws Exception
  {
    lifecycle.start();
    tmpFileParent = new File(temporaryFolder.newFolder(), "☃");
    Assert.assertTrue(tmpFileParent.mkdir());
    Assert.assertTrue(tmpFileParent.isDirectory());
    tmpFile = Files.createTempFile(tmpFileParent.toPath(), "druidTestURIExtractionNS", suffix).toFile();
    final ObjectMapper mapper = new DefaultObjectMapper();
    try (OutputStream ostream = outStreamSupplier.apply(tmpFile)) {
      try (OutputStreamWriter out = new OutputStreamWriter(ostream)) {
        out.write(mapper.writeValueAsString(ImmutableMap.<String, String>of(
            "boo",
            "bar",
            "foo",
            "bar",
            "",
            "MissingValue",
            "emptyString",
            ""
        )));
      }
    }
    factory = new URIExtractionNamespaceCacheFactory(FINDERS);
    namespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null
    );
    id = "ns";
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
  }

  @Test
  public void simpleTest() throws IOException, ExecutionException, InterruptedException
  {
    Assert.assertTrue(manager.getKnownIDs().isEmpty());
    NamespaceExtractionCacheManagersTest.waitFor(manager.schedule(id, namespace));
    Map<String, String> map = manager.getCacheMap(id);
    Assert.assertEquals("bar", map.get("foo"));
    Assert.assertEquals(null, map.get("baz"));
  }

  @Test
  public void simpleTestRegex() throws IOException, ExecutionException, InterruptedException
  {
    String regexID = "regex";
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        null,
        Paths.get(this.namespace.getUri()).getParent().toUri(),
        Pattern.quote(Paths.get(this.namespace.getUri()).getFileName().toString()),
        this.namespace.getNamespaceParseSpec(),
        Period.millis((int) this.namespace.getPollMs()),
        null
    );
    Assert.assertTrue(!manager.getKnownIDs().contains(regexID));
    NamespaceExtractionCacheManagersTest.waitFor(manager.schedule(regexID, namespace));
    Map<String, String> map = manager.getCacheMap(regexID);
    Assert.assertNotNull(map);
    Assert.assertEquals("bar", map.get("foo"));
    Assert.assertEquals(null, map.get("baz"));
  }

  @Test
  public void simplePileONamespacesTest() throws InterruptedException
  {
    final int size = 128;
    List<String> ids = new ArrayList<>(size);
    for (int i = 0; i < size; ++i) {
      String id = String.format("%d-ns-%d", i << 10, i);
      ids.add(id);
      URIExtractionNamespace namespace = new URIExtractionNamespace(
          tmpFile.toURI(),
          null, null,
          new URIExtractionNamespace.ObjectMapperFlatDataParser(
              URIExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(0),
          null
      );

      Assert.assertFalse(manager.getKnownIDs().contains(id));
      NamespaceExtractionCacheManagersTest.waitFor(manager.schedule(id, namespace));
    }

    for (String id : ids) {
      final Map<String, String> map = manager.getCacheMap(id);
      Assert.assertEquals("bar", map.get("foo"));
      Assert.assertEquals(null, map.get("baz"));
      manager.delete(id);
    }
    Assert.assertTrue(manager.getKnownIDs().isEmpty());
  }

  @Test
  public void testLoadOnlyOnce() throws Exception
  {
    Assert.assertTrue(manager.getKnownIDs().isEmpty());

    ConcurrentMap<String, String> map = new ConcurrentHashMap<>();
    Callable<String> populator = factory.getCachePopulator(id, namespace, null, map);

    String v = populator.call();
    Assert.assertEquals("bar", map.get("foo"));
    Assert.assertEquals(null, map.get("baz"));
    Assert.assertNotNull(v);

    populator = factory.getCachePopulator(id, namespace, v, map);
    String v2 = populator.call();
    Assert.assertEquals(v, v2);
    Assert.assertEquals("bar", map.get("foo"));
    Assert.assertEquals(null, map.get("baz"));
  }

  @Test
  public void testMissing() throws Exception
  {
    URIExtractionNamespace badNamespace = new URIExtractionNamespace(
        namespace.getUri(),
        null, null,
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        null
    );
    Assert.assertTrue(new File(namespace.getUri()).delete());
    ConcurrentMap<String, String> map = new ConcurrentHashMap<>();
    try {
      factory.getCachePopulator(id, badNamespace, null, map).call();
    }
    catch (RuntimeException e) {
      Assert.assertNotNull(e.getCause());
      Assert.assertEquals(FileNotFoundException.class, e.getCause().getClass());
      return;
    }
    Assert.fail("Did not have exception");
  }

  @Test
  public void testMissingRegex() throws Exception
  {
    String badId = "bad";
    URIExtractionNamespace badNamespace = new URIExtractionNamespace(
        null,
        Paths.get(namespace.getUri()).getParent().toUri(),
        Pattern.quote(Paths.get(namespace.getUri()).getFileName().toString()),
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        null
    );
    Assert.assertTrue(new File(namespace.getUri()).delete());
    ConcurrentMap<String, String> map = new ConcurrentHashMap<>();
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public void describeTo(Description description)
      {

      }

      @Override
      public boolean matches(Object o)
      {
        if (!(o instanceof Throwable)) {
          return false;
        }
        final Throwable t = (Throwable) o;
        return t.getCause() != null && t.getCause() instanceof FileNotFoundException;
      }
    });
    factory.getCachePopulator(badId, badNamespace, null, map).call();
  }

  @Test(expected = IAE.class)
  public void testExceptionalCreationDoubleURI()
  {
    new URIExtractionNamespace(
        namespace.getUri(),
        namespace.getUri(),
        null,
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        null
    );
  }

  @Test(expected = IAE.class)
  public void testExceptionalCreationURIWithPattern()
  {
    new URIExtractionNamespace(
        namespace.getUri(),
        null,
        "",
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        null
    );
  }

  @Test(expected = IAE.class)
  public void testExceptionalCreationURIWithLegacyPattern()
  {
    new URIExtractionNamespace(
        namespace.getUri(),
        null,
        null,
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        ""
    );
  }

  @Test(expected = IAE.class)
  public void testLegacyMix()
  {
    new URIExtractionNamespace(
        null,
        namespace.getUri(),
        "",
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        ""
    );
  }


  @Test(expected = IAE.class)
  public void testBadPattern()
  {
    new URIExtractionNamespace(
        null,
        namespace.getUri(),
        "[",
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        null
    );
  }

  @Test
  public void testWeirdSchemaOnExactURI() throws Exception
  {
    final URIExtractionNamespace extractionNamespace = new URIExtractionNamespace(
        new URI(
            FAKE_SCHEME,
            namespace.getUri().getUserInfo(),
            namespace.getUri().getHost(),
            namespace.getUri().getPort(),
            namespace.getUri().getPath(),
            namespace.getUri().getQuery(),
            namespace.getUri().getFragment()
        ),
        null,
        null,
        namespace.getNamespaceParseSpec(),
        Period.millis((int) namespace.getPollMs()),
        null
    );
    final Map<String, String> map = new HashMap<>();
    Assert.assertNotNull(factory.getCachePopulator(id, extractionNamespace, null, map).call());
  }
}
