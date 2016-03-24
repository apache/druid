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

package io.druid.server.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.dimension.LookupDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.NamespacedExtractor;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespace;
import io.druid.query.extraction.namespace.URIExtractionNamespaceTest;
import io.druid.query.lookup.*;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class NamespaceExtractionClusterTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public final TemporaryFolder temporaryFolder2 = new TemporaryFolder();

  private ObjectMapper mapper;
  private LookupReferencesManager lookupReferencesManager;
  private Injector injector;

  @Before
  public void setUp()
  {
    System.setProperty("druid.extensions.searchCurrentClassloader", "false");

    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjectorWithModules(
            ImmutableList.<Module>of()
        ),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              }
            },
            new LookupModule(),
            new NamespacedExtractionModule()
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);
    lookupReferencesManager = injector.getInstance(LookupReferencesManager.class);
    lookupReferencesManager.start();
  }

  @Test(timeout = 60_000)
  public void testSimpleJson() throws IOException, InterruptedException
  {
    final File tempFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tempFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        tempFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );

    String namespaceString = mapper.writeValueAsString(namespace);
    String json = String.format("{\"type\":\"namespace\", \"extractionNamespace\":%s}", namespaceString);
    LookupExtractorFactory factory = mapper.readValue(json, LookupExtractorFactory.class);

    Assert.assertNotNull(factory);

    Assert.assertTrue(factory.start());

    LookupExtractor extractor = factory.get();

    Assert.assertTrue(extractor instanceof NamespacedExtractor);

    NamespacedExtractor namespacedExtractor = (NamespacedExtractor)extractor;

    Assert.assertEquals("bar", namespacedExtractor.apply("foo"));

    Assert.assertTrue(factory.close());
  }

  @Test(timeout = 60_000)
  public void testTwoFactories() throws IOException, InterruptedException
  {
    final File tempFile1 = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tempFile1)) {
      out.write(mapper.writeValueAsString(ImmutableMap.of("foo", "bar")));
    }
    final URIExtractionNamespace namespace1 = new URIExtractionNamespace(
        tempFile1.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    String namespaceString1 = mapper.writeValueAsString(namespace1);
    String json1 = String.format("{\"type\":\"namespace\", \"extractionNamespace\":%s}", namespaceString1);
    LookupExtractorFactory factory1 = mapper.readValue(json1, LookupExtractorFactory.class);

    final File tempFile2 = temporaryFolder2.newFile();
    try (OutputStreamWriter out = new FileWriter(tempFile2)) {
      out.write(mapper.writeValueAsString(ImmutableMap.of("foo", "bad")));
    }
    final URIExtractionNamespace namespace2 = new URIExtractionNamespace(
        tempFile2.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );
    String namespaceString2 = mapper.writeValueAsString(namespace2);
    String json2 = String.format("{\"type\":\"namespace\", \"extractionNamespace\":%s}", namespaceString2);
    LookupExtractorFactory factory2 = mapper.readValue(json2, LookupExtractorFactory.class);

    Assert.assertNotNull(factory1);
    Assert.assertNotNull(factory2);

    Assert.assertTrue(factory1.start());
    Assert.assertTrue(factory2.start());

    LookupExtractor extractor1 = factory1.get();
    Assert.assertTrue(extractor1 instanceof NamespacedExtractor);

    NamespacedExtractor namespacedExtractor1 = (NamespacedExtractor)extractor1;
    Assert.assertEquals("bar", namespacedExtractor1.apply("foo"));

    LookupExtractor extractor2 = factory2.get();
    Assert.assertTrue(extractor2 instanceof NamespacedExtractor);

    NamespacedExtractor namespacedExtractor2 = (NamespacedExtractor)extractor2;
    Assert.assertEquals("bad", namespacedExtractor2.apply("foo"));

    Assert.assertTrue(factory1.close());
    Assert.assertTrue(factory2.close());
  }

  @Test(timeout = 60_000)
  public void testReferenceManagerIntegration() throws IOException, InterruptedException
  {
    final File tempFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tempFile)) {
      out.write(mapper.writeValueAsString(ImmutableMap.of("foo", "bar")));
    }
    final URIExtractionNamespace namespace = new URIExtractionNamespace(
        tempFile.toURI(),
        new URIExtractionNamespace.ObjectMapperFlatDataParser(
            URIExtractionNamespaceTest.registerTypes(new DefaultObjectMapper())
        ),
        new Period(0),
        null
    );

    String namespaceString = mapper.writeValueAsString(namespace);
    String json = String.format("{\"type\":\"namespace\", \"extractionNamespace\":%s}", namespaceString);
    LookupExtractorFactory factory = mapper.readValue(json, LookupExtractorFactory.class);

    lookupReferencesManager.updateIfNew("refTest", factory);

    LookupDimensionSpec lookupDimensionSpec = new LookupDimensionSpec(
        "col",
        "out",
        null,
        false,
        null,
        "refTest",
        lookupReferencesManager,
        false
    );

    ExtractionFn extractionFn = lookupDimensionSpec.getExtractionFn();
    Assert.assertEquals("bar", extractionFn.apply("foo"));
  }
}
