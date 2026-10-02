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

package org.apache.druid.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespace;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespaceTest;
import org.apache.druid.segment.loading.LocalFileTimestampVersionFinder;
import org.apache.druid.server.lookup.namespace.cache.CacheHandler;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;
import org.apache.druid.server.lookup.namespace.cache.CacheSchedulerTest;
import org.apache.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.druid.server.lookup.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import org.apache.druid.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
 */
public class UriCacheGeneratorTest
{
  private static final String FAKE_SCHEME = "wabblywoo";
  private static final Map<String, SearchableVersionedDataFinder> FINDERS = ImmutableMap.of(
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
            throw new RuntimeException(e);
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

  public static Iterable<Object[]> getParameters()
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
                  throw new RuntimeException(ex);
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
                  throw new RuntimeException(ex);
                }
              }
            }
        }
    );

    final List<Function<Lifecycle, NamespaceExtractionCacheManager>> cacheManagerCreators = ImmutableList.of(
        lifecycle -> new OnHeapNamespaceExtractionCacheManager(
            lifecycle,
            new NoopServiceEmitter(),
            new NamespaceExtractionConfig()
        ),
        lifecycle -> new OffHeapNamespaceExtractionCacheManager(
            lifecycle,
            new NoopServiceEmitter(),
            new NamespaceExtractionConfig()
        )
    );
    return () -> new Iterator<>()
    {
      Iterator<Object[]> compressionIt = compressionParams.iterator();
      Iterator<Function<Lifecycle, NamespaceExtractionCacheManager>> cacheManagerCreatorsIt =
          cacheManagerCreators.iterator();
      Object[] compressions = compressionIt.next();

      @Override
      public boolean hasNext()
      {
        return compressionIt.hasNext() || cacheManagerCreatorsIt.hasNext();
      }

      @Override
      public Object[] next()
      {
        if (cacheManagerCreatorsIt.hasNext()) {
          Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator = cacheManagerCreatorsIt.next();
          return new Object[]{compressions[0], compressions[1], cacheManagerCreator};
        } else {
          cacheManagerCreatorsIt = cacheManagerCreators.iterator();
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

  @TempDir
  public File temporaryFolder;

  private String suffix;

  private Function<File, OutputStream> outStreamSupplier;
  private Lifecycle lifecycle;
  private NamespaceExtractionCacheManager cacheManager;
  private CacheScheduler scheduler;
  private File tmpFile;
  private UriCacheGenerator generator;
  private UriExtractionNamespace namespace;

  public void initUriCacheGeneratorTest(
      String suffix,
      Function<File, OutputStream> outStreamSupplier,
      Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator
  )
  {
    this.suffix = suffix;
    this.outStreamSupplier = outStreamSupplier;
    this.cacheManager = cacheManagerCreator.apply(lifecycle);
    this.scheduler = new CacheScheduler(
        new NoopServiceEmitter(),
        ImmutableMap.of(UriExtractionNamespace.class, new UriCacheGenerator(FINDERS, JvmUtils.getRuntimeInfo())),
        cacheManager
    );
    try {
      initializeUriCacheGeneratorTest();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  public void setUp() throws Exception
  {
    lifecycle = new Lifecycle();
    lifecycle.start();
  }

  private void initializeUriCacheGeneratorTest() throws Exception
  {
    File tmpFileParent = new File(newFolder(temporaryFolder, "junit"), "☃");
    Assertions.assertTrue(tmpFileParent.mkdir());
    Assertions.assertTrue(tmpFileParent.isDirectory());
    tmpFile = Files.createTempFile(tmpFileParent.toPath(), "druidTestURIExtractionNS", suffix).toFile();
    final ObjectMapper mapper = new DefaultObjectMapper();
    try (OutputStream ostream = outStreamSupplier.apply(tmpFile);
         OutputStreamWriter out = new OutputStreamWriter(ostream, StandardCharsets.UTF_8)) {
      out.write(mapper.writeValueAsString(ImmutableMap.of(
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
    generator = new UriCacheGenerator(FINDERS, JvmUtils.getRuntimeInfo());
    namespace = new UriExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new UriExtractionNamespace.ObjectMapperFlatDataParser(
            UriExtractionNamespaceTest.registerTypes(new ObjectMapper())
        ),
        new Period(0),
        null,
        null
    );
  }

  @AfterEach
  public void tearDown()
  {
    lifecycle.stop();
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void simpleTest(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator) throws InterruptedException
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    Assertions.assertEquals(0, scheduler.getActiveEntries());
    CacheScheduler.Entry entry = scheduler.schedule(namespace);
    CacheSchedulerTest.waitFor(entry);
    Map<String, String> map = entry.getCache();
    Assertions.assertEquals("bar", map.get("foo"));
    Assertions.assertEquals(null, map.get("baz"));
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void simpleTestRegex(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator) throws InterruptedException
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    final UriExtractionNamespace namespace = new UriExtractionNamespace(
        null,
        Paths.get(this.namespace.getUri()).getParent().toUri(),
        Pattern.quote(Paths.get(this.namespace.getUri()).getFileName().toString()),
        this.namespace.getNamespaceParseSpec(),
        Period.millis((int) this.namespace.getPollMs()),
        null,
        null
    );
    CacheScheduler.Entry entry = scheduler.schedule(namespace);
    CacheSchedulerTest.waitFor(entry);
    Map<String, String> map = entry.getCache();
    Assertions.assertNotNull(map);
    Assertions.assertEquals("bar", map.get("foo"));
    Assertions.assertEquals(null, map.get("baz"));
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void simplePileONamespacesTest(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator) throws InterruptedException
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    final int size = 128;
    List<CacheScheduler.Entry> entries = new ArrayList<>(size);
    for (int i = 0; i < size; ++i) {
      UriExtractionNamespace namespace = new UriExtractionNamespace(
          tmpFile.toURI(),
          null, null,
          new UriExtractionNamespace.ObjectMapperFlatDataParser(
              UriExtractionNamespaceTest.registerTypes(new ObjectMapper())
          ),
          new Period(0),
          null,
          null
      );

      CacheScheduler.Entry entry = scheduler.schedule(namespace);
      entries.add(entry);
      CacheSchedulerTest.waitFor(entry);
    }

    for (CacheScheduler.Entry entry : entries) {
      final Map<String, String> map = entry.getCache();
      Assertions.assertEquals("bar", map.get("foo"));
      Assertions.assertEquals(null, map.get("baz"));
      entry.close();
    }
    Assertions.assertEquals(0, scheduler.getActiveEntries());
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testLoadOnlyOnce(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator) throws Exception
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    Assertions.assertEquals(0, scheduler.getActiveEntries());

    CacheHandler cache = cacheManager.allocateCache();
    String newVersion = generator.generateCache(namespace, null, null, cache);
    Assertions.assertNotNull(newVersion);
    Map<String, String> map = cache.getCache();
    Assertions.assertEquals("bar", map.get("foo"));
    Assertions.assertEquals(null, map.get("baz"));
    String version = newVersion;
    Assertions.assertNotNull(version);

    Assertions.assertNull(generator.generateCache(namespace, null, version, cacheManager.allocateCache()));
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testMissing(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator)
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    assertThrows(FileNotFoundException.class, () -> {
      UriExtractionNamespace badNamespace = new UriExtractionNamespace(
          namespace.getUri(),
          null, null,
          namespace.getNamespaceParseSpec(),
          Period.millis((int) namespace.getPollMs()),
          null,
          null
      );
      Assertions.assertTrue(new File(namespace.getUri()).delete());
      generator.generateCache(badNamespace, null, null, cacheManager.allocateCache());
    });
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testMissingRegex(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator)
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    assertThrows(FileNotFoundException.class, () -> {
      UriExtractionNamespace badNamespace = new UriExtractionNamespace(
          null,
          Paths.get(namespace.getUri()).getParent().toUri(),
          Pattern.quote(Paths.get(namespace.getUri()).getFileName().toString()),
          namespace.getNamespaceParseSpec(),
          Period.millis((int) namespace.getPollMs()),
          null,
          null
      );
      Assertions.assertTrue(new File(namespace.getUri()).delete());
      generator.generateCache(badNamespace, null, null, cacheManager.allocateCache());
    });
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testExceptionalCreationDoubleURI(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator)
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    assertThrows(IAE.class, () -> {
      new UriExtractionNamespace(
          namespace.getUri(),
          namespace.getUri(),
          null,
          namespace.getNamespaceParseSpec(),
          Period.millis((int) namespace.getPollMs()),
          null,
          null
      );
    });
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testExceptionalCreationURIWithPattern(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator)
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    assertThrows(IAE.class, () -> {
      new UriExtractionNamespace(
          namespace.getUri(),
          null,
          "",
          namespace.getNamespaceParseSpec(),
          Period.millis((int) namespace.getPollMs()),
          null,
          null
      );
    });
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testExceptionalCreationURIWithLegacyPattern(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator)
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    assertThrows(IAE.class, () -> {
      new UriExtractionNamespace(
          namespace.getUri(),
          null,
          null,
          namespace.getNamespaceParseSpec(),
          Period.millis((int) namespace.getPollMs()),
          "",
          null
      );
    });
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testLegacyMix(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator)
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    assertThrows(IAE.class, () -> {
      new UriExtractionNamespace(
          null,
          namespace.getUri(),
          "",
          namespace.getNamespaceParseSpec(),
          Period.millis((int) namespace.getPollMs()),
          "",
          null
      );
    });
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testBadPattern(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator)
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    assertThrows(IAE.class, () -> {
      new UriExtractionNamespace(
          null,
          namespace.getUri(),
          "[",
          namespace.getNamespaceParseSpec(),
          Period.millis((int) namespace.getPollMs()),
          null,
          null
      );
    });
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  public void testWeirdSchemaOnExactURI(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator) throws Exception
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    final UriExtractionNamespace extractionNamespace = new UriExtractionNamespace(
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
        null,
        null
    );
    Assertions.assertNotNull(generator.generateCache(extractionNamespace, null, null, cacheManager.allocateCache()));
  }

  @MethodSource("getParameters")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testDeleteOnScheduleFail(String suffix, Function<File, OutputStream> outStreamSupplier, Function<Lifecycle, NamespaceExtractionCacheManager> cacheManagerCreator) throws Exception
  {
    initUriCacheGeneratorTest(suffix, outStreamSupplier, cacheManagerCreator);
    Assertions.assertNull(scheduler.scheduleAndWait(
        new UriExtractionNamespace(
            new URI("file://tmp/I_DONT_REALLY_EXIST" + UUID.randomUUID()),
            null,
            null,
            new UriExtractionNamespace.JSONFlatDataParser(
                new DefaultObjectMapper(),
                "key",
                "val"
            ),
            Period.millis(10000),
            null,
            null
        ),
        500
    ));
    Assertions.assertEquals(0, scheduler.getActiveEntries());
  }

  private static File newFolder(File root, String... subDirs)
  {
    return FileUtils.createTempDirInLocation(root.toPath(), String.join("-", subDirs));
  }
}
