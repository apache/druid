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

import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.loading.LeastBytesUsedStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.StorageLoadingThreadPool;
import org.apache.druid.segment.loading.StorageLocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageLocationVirtualStorageManagerTest
{
  @TempDir
  public File tempFolder;

  private StorageLocation location;
  private StorageLocationVirtualStorageManager manager;

  @BeforeEach
  public void setup() throws IOException
  {
    File locationPath = new File(tempFolder, "storage");
    Files.createDirectories(locationPath.toPath());
    location = new StorageLocation(locationPath, 10_000_000L, null);
    manager = new StorageLocationVirtualStorageManager(
        Collections.singletonList(location),
        new LeastBytesUsedStorageLocationSelectorStrategy(Collections.singletonList(location)),
        StorageLoadingThreadPool.none()
    );
  }

  @AfterEach
  public void teardown()
  {
    location.reset();
  }

  /**
   * Helper method to run test logic with an ExecutorService that is guaranteed to be shut down.
   */
  private void withExecutor(ExecutorConsumer testLogic) throws Exception
  {
    ExecutorService executorService = Execs.multiThreaded(10, "test-thread-%d");
    try {
      testLogic.accept(executorService);
    }
    finally {
      executorService.shutdownNow();
    }
  }

  @FunctionalInterface
  private interface ExecutorConsumer
  {
    void accept(ExecutorService executor) throws Exception;
  }

  @Test
  public void testGetReturnsNullWhenNotExists()
  {
    CachedFile result = manager.get("nonexistent");
    Assertions.assertNull(result);
  }

  @Test
  public void testReserveAndPopulateCreatesFile() throws Exception
  {
    String identifier = "test-file";
    String content = "Hello, World!";

    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(0, location.getWeakEntryCount());

    try (CachedFile cachedFile = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      Assertions.assertNotNull(cachedFile);
      Assertions.assertEquals(identifier, cachedFile.getIdentifier());

      // Validate there's an active hold while the file is open
      Assertions.assertEquals(1, location.getWeakStats().getHoldCount());
      Assertions.assertEquals(1, location.getWeakEntryCount());

      // Validate that both load begin and load complete were tracked
      Assertions.assertEquals(1, location.getWeakStats().getLoadBeginCount());
      Assertions.assertEquals(content.length(), location.getWeakStats().getLoadBeginBytes());
      Assertions.assertEquals(1, location.getWeakStats().getLoadCount());
      Assertions.assertEquals(content.length(), location.getWeakStats().getLoadBytes());

      File file = cachedFile.getFile();
      Assertions.assertNotNull(file);
      Assertions.assertTrue(file.exists());

      String readContent = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
      Assertions.assertEquals(content, readContent);
    }

    // After close, hold should be released
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    // Entry remains in cache, just not held
    Assertions.assertEquals(1, location.getWeakEntryCount());
  }

  @Test
  public void testGetReturnsFileAfterPopulate() throws Exception
  {
    String identifier = "test-file";
    String content = "Test content";

    // First populate
    try (CachedFile cachedFile = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      Assertions.assertNotNull(cachedFile);
      Assertions.assertEquals(1, location.getWeakStats().getHoldCount());
    }

    // After close, hold should be released but entry remains
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());

    // Initial populate counts as a completed load
    Assertions.assertEquals(1, location.getWeakStats().getLoadCount());
    Assertions.assertEquals(content.length(), location.getWeakStats().getLoadBytes());

    // Then get
    try (CachedFile cachedFile = manager.get(identifier)) {
      Assertions.assertNotNull(cachedFile);
      Assertions.assertEquals(identifier, cachedFile.getIdentifier());

      // Hold should be re-acquired
      Assertions.assertEquals(1, location.getWeakStats().getHoldCount());

      // A hit does not count as another load
      Assertions.assertEquals(1, location.getWeakStats().getLoadCount());
      Assertions.assertEquals(content.length(), location.getWeakStats().getLoadBytes());
      Assertions.assertEquals(1, location.getWeakStats().getHitCount());

      String readContent = new String(
          Files.readAllBytes(cachedFile.getFile().toPath()),
          StandardCharsets.UTF_8
      );
      Assertions.assertEquals(content, readContent);
    }

    // After second close, hold should be released again
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());
  }

  @Test
  public void testGetReturnsNullAfterEphemeralEviction() throws Exception
  {
    location.setAreWeakEntriesEphemeral(true);

    String identifier = "test-file";
    String content = "Hello, World!";

    try (CachedFile cachedFile = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      try (CachedFile fromGet = manager.get(identifier)) {
        Assertions.assertNotNull(fromGet);
      }
    }
    // Last hold released → ephemeral eviction → entry unmounted

    Assertions.assertNull(manager.get(identifier));
    Assertions.assertEquals(0, location.getWeakEntryCount());
  }

  @Test
  public void testPopulationLockClearedOnEphemeralEviction() throws Exception
  {
    location.setAreWeakEntriesEphemeral(true);

    String identifier = "test-file";
    AtomicInteger firstSupplierCalls = new AtomicInteger();
    AtomicInteger secondSupplierCalls = new AtomicInteger();
    AtomicInteger firstPopulatorCalls = new AtomicInteger();
    AtomicInteger secondPopulatorCalls = new AtomicInteger();

    try (CachedFile firstFile = manager.reserveAndPopulate(
        identifier,
        () -> {
          firstSupplierCalls.incrementAndGet();
          return 4L;
        },
        file -> {
          firstPopulatorCalls.incrementAndGet();
          Files.write(file.toPath(), "v1".getBytes(StandardCharsets.UTF_8));
        }
    )) {
      Assertions.assertNotNull(firstFile);
    }
    // Hold released → ephemeral eviction → unmount → populationLocks.remove(id, lock)

    try (CachedFile secondFile = manager.reserveAndPopulate(
        identifier,
        () -> {
          secondSupplierCalls.incrementAndGet();
          return 4L;
        },
        file -> {
          secondPopulatorCalls.incrementAndGet();
          Files.write(file.toPath(), "v2".getBytes(StandardCharsets.UTF_8));
        }
    )) {
      Assertions.assertNotNull(secondFile);
      String readContent = new String(
          Files.readAllBytes(secondFile.getFile().toPath()),
          StandardCharsets.UTF_8
      );
      Assertions.assertEquals("v2", readContent);
    }

    Assertions.assertEquals(1, firstSupplierCalls.get(), "first reserve must be a miss");
    Assertions.assertEquals(1, firstPopulatorCalls.get(), "first populator must run");
    Assertions.assertEquals(
        1,
        secondSupplierCalls.get(),
        "second reserve must also be a miss — proving the lock was cleared on unmount"
    );
    Assertions.assertEquals(1, secondPopulatorCalls.get(), "second populator must run");
  }

  @Test
  public void testPopulationLockClearedOnCapacityEviction() throws Exception
  {
    long capacity = location.availableSizeBytes();
    long smallSize = capacity / 4;
    long bigSize = capacity - 100;     // forces reclaim of the small entry

    String smallId = "small";
    String bigId = "big";

    AtomicInteger populatorCalls = new AtomicInteger();
    try (CachedFile smallFile = manager.reserveAndPopulate(
        smallId,
        () -> smallSize,
        file -> {
          populatorCalls.incrementAndGet();
          Files.write(file.toPath(), new byte[]{1, 2, 3});
        }
    )) {
      Assertions.assertNotNull(smallFile);
    }
    // Small entry is now weakly cached, eligible for eviction.

    try (CachedFile bigFile = manager.reserveAndPopulate(
        bigId,
        () -> bigSize,
        file -> {
          populatorCalls.incrementAndGet();
          Files.write(file.toPath(), new byte[]{4, 5, 6});
        }
    )) {
      Assertions.assertNotNull(bigFile);
    }
    // Reserving `big` should have forced eviction of `small`, invoking unmount → cleanup.

    AtomicInteger supplierCalls = new AtomicInteger();
    try (CachedFile reborn = manager.reserveAndPopulate(
        smallId,
        () -> {
          supplierCalls.incrementAndGet();
          return smallSize;
        },
        file -> {
          populatorCalls.incrementAndGet();
          Files.write(file.toPath(), new byte[]{7, 8, 9});
        }
    )) {
      Assertions.assertNotNull(reborn);
    }

    Assertions.assertEquals(
        1,
        supplierCalls.get(),
        "re-reserving an evicted id must take the miss path"
    );
    Assertions.assertEquals(3, populatorCalls.get());
  }

  @Test
  public void testPopulationLockClearedOnLocationReset() throws Exception
  {
    // location.reset() unmounts every weakly-held entry; the cleanup hook must fire for each.
    String identifier = "test-file";
    String content = "Hello, World!";

    try (CachedFile cachedFile = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      Assertions.assertNotNull(cachedFile);
    }
    Assertions.assertEquals(1, location.getWeakEntryCount());

    location.reset();
    Assertions.assertEquals(0, location.getWeakEntryCount());

    AtomicInteger supplierCalls = new AtomicInteger();
    AtomicInteger populatorCalls = new AtomicInteger();
    try (CachedFile reborn = manager.reserveAndPopulate(
        identifier,
        () -> {
          supplierCalls.incrementAndGet();
          return (long) content.length();
        },
        file -> {
          populatorCalls.incrementAndGet();
          Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
        }
    )) {
      Assertions.assertNotNull(reborn);
    }

    Assertions.assertEquals(
        1,
        supplierCalls.get(),
        "re-reserving after location.reset() must take the miss path"
    );
    Assertions.assertEquals(1, populatorCalls.get());
  }

  @Test
  public void testMultipleCallsToReserveAndPopulateWithSameIdentifier() throws Exception
  {
    String identifier = "test-file";
    String content1 = "First content";
    String content2 = "Second content";

    // First call
    try (CachedFile cachedFile1 = manager.reserveAndPopulate(
        identifier,
        content1::length,
        file -> Files.write(file.toPath(), content1.getBytes(StandardCharsets.UTF_8))
    )) {
      Assertions.assertNotNull(cachedFile1);
      Assertions.assertEquals(1, location.getWeakStats().getHoldCount());

      // Second call with same identifier should return existing file
      try (CachedFile cachedFile2 = manager.reserveAndPopulate(
          identifier,
          content2::length,
          file -> {
            // This populator should NOT be called
            throw new RuntimeException("Populator should not be called for existing file");
          }
      )) {
        Assertions.assertNotNull(cachedFile2);

        Assertions.assertEquals(2, location.getWeakStats().getHoldCount());

        // Only the first call counts as a load
        Assertions.assertEquals(1, location.getWeakStats().getLoadCount());
        Assertions.assertEquals(content1.length(), location.getWeakStats().getLoadBytes());

        // Should have first content, not second
        String readContent = new String(
            Files.readAllBytes(cachedFile2.getFile().toPath()),
            StandardCharsets.UTF_8
        );
        Assertions.assertEquals(content1, readContent);
      }

      // Inner hold released, outer hold still active
      Assertions.assertEquals(1, location.getWeakStats().getHoldCount());
    }

    // All holds released
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());
  }

  @Test
  public void testSizeSupplierIsNotCalledOnCacheHit()
  {
    String identifier = "test-file";
    String content = "Hello, World!";
    AtomicInteger missSupplierCalls = new AtomicInteger();
    AtomicInteger hitSupplierCalls = new AtomicInteger();

    try (CachedFile first = manager.reserveAndPopulate(
        identifier,
        () -> {
          missSupplierCalls.incrementAndGet();
          return content.length();
        },
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      Assertions.assertNotNull(first);
      Assertions.assertEquals(1, missSupplierCalls.get(), "supplier should be invoked on cache miss");

      // Hit path: supplier must not be invoked.
      try (CachedFile second = manager.reserveAndPopulate(
          identifier,
          () -> {
            hitSupplierCalls.incrementAndGet();
            return content.length();
          },
          ignored -> {
            throw new RuntimeException("Populator should not be called for existing file");
          }
      )) {
        Assertions.assertNotNull(second);
      }
    }

    Assertions.assertEquals(
        0,
        hitSupplierCalls.get(),
        "size supplier must not be invoked when the cached entry already exists"
    );
  }

  @Test
  public void testSizeSupplierIsCalledExactlyOnceOnCacheMiss()
  {
    // Companion to the cache-hit case: a successful miss path should evaluate the
    // supplier exactly once, even though the implementation iterates storage locations
    // looking for one with capacity.
    String identifier = "test-file";
    String content = "Hello, World!";
    AtomicInteger supplierCalls = new AtomicInteger();

    try (CachedFile cachedFile = manager.reserveAndPopulate(
        identifier,
        () -> {
          supplierCalls.incrementAndGet();
          return content.length();
        },
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      Assertions.assertNotNull(cachedFile);
    }

    Assertions.assertEquals(1, supplierCalls.get(), "size supplier must be invoked exactly once on cache miss");
  }

  @Test
  public void testPopulatorThrowsException()
  {
    String identifier = "test-file";

    new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.RUNTIME_FAILURE,
        "general"
    )
        .expectMessageContains("populate")
        .assertThrowsAndMatches(() -> manager.reserveAndPopulate(
            identifier,
            () -> 100,
            file -> {
              throw new IOException("Populator failed");
            }
        ));

    // Note: The entry may still be in the cache but in an unmounted state.
    // StorageLocation will eventually evict it. The important thing is that
    // the exception was properly thrown and the file was not successfully created.

    // A failed populate counts as a load begin, but not as a completed load.
    Assertions.assertEquals(1, location.getWeakStats().getLoadBeginCount());
    Assertions.assertEquals(0, location.getWeakStats().getLoadCount());
    Assertions.assertEquals(0, location.getWeakStats().getLoadBytes());
  }

  @Test
  public void testInsufficientSpace()
  {
    String identifier = "huge-file";
    long hugeSize = location.availableSizeBytes() + 1;

    new DruidExceptionMatcher(
        DruidException.Persona.OPERATOR,
        DruidException.Category.CAPACITY_EXCEEDED,
        "general"
    )
        .expectMessageContains("No space available")
        .assertThrowsAndMatches(() -> manager.reserveAndPopulate(
            identifier,
            () -> hugeSize,
            file -> Files.write(file.toPath(), new byte[1000])
        ));
  }

  @Test
  public void testReserveAndPopulateAsyncRecoversFromInsufficientStorage()
  {
    // Verify something similar to what ExternalInputSliceReader does: fall back to recovery logic if the
    // storage location cannot hold a file.
    final long tooLargeFileSize = location.availableSizeBytes() + 1;

    final AsyncResource<String> recovered = AsyncResources.recover(
        AsyncResources.transform(
            manager.reserveAndPopulateAsync(
                "huge-file",
                () -> tooLargeFileSize,
                file -> Files.write(file.toPath(), new byte[]{1, 2, 3})
            ),
            cachedFile -> "fetched"
        ),
        e -> VirtualStorageManager.isInsufficientStorage(e) ? "recovery" : null
    );

    try (recovered) {
      Assertions.assertTrue(recovered.isReady());
      Assertions.assertEquals("recovery", recovered.get());
    }
  }

  @Test
  public void testReserveAndPopulateAsyncDoesNotRecoverFromOtherErrors()
  {
    // A populate failure that is not an insufficient-storage error must be propagated, not recovered: the recovery
    // function returns null, so the original error surfaces from get().
    final AsyncResource<String> recovered = AsyncResources.recover(
        AsyncResources.transform(
            manager.reserveAndPopulateAsync(
                "boom-file",
                () -> 100L,
                file -> {
                  throw new IOException("populate failed");
                }
            ),
            cachedFile -> "fetched"
        ),
        e -> VirtualStorageManager.isInsufficientStorage(e) ? "streamed" : null
    );

    try (recovered) {
      Assertions.assertTrue(recovered.isReady());
      final DruidException e = Assertions.assertThrows(DruidException.class, recovered::get);
      Assertions.assertEquals(DruidException.Category.RUNTIME_FAILURE, e.getCategory());
    }
  }

  @Test
  public void testFilePathSanitization()
  {
    String identifier = "path/with/slashes/and-special_chars.txt";
    String content = "Test content";

    try (CachedFile cachedFile = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      File file = cachedFile.getFile();
      Assertions.assertNotNull(file);
      Assertions.assertTrue(file.exists());

      // Verify file is in a subdirectory structure
      Assertions.assertTrue(file.getAbsolutePath().contains("path"));
      Assertions.assertTrue(file.getAbsolutePath().contains("slashes"));
    }
  }

  // ========== LIFECYCLE TESTS ==========

  @Test
  public void testLifecycleWithMultipleFiles()
  {
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(0, location.getWeakEntryCount());

    // Create multiple files
    CachedFile file1 = manager.reserveAndPopulate(
        "file1",
        () -> 5,
        file -> Files.write(file.toPath(), "File1".getBytes(StandardCharsets.UTF_8))
    );
    Assertions.assertEquals(1, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());

    CachedFile file2 = manager.reserveAndPopulate(
        "file2",
        () -> 5,
        file -> Files.write(file.toPath(), "File2".getBytes(StandardCharsets.UTF_8))
    );
    Assertions.assertEquals(2, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(2, location.getWeakEntryCount());

    CachedFile file3 = manager.reserveAndPopulate(
        "file3",
        () -> 5,
        file -> Files.write(file.toPath(), "File3".getBytes(StandardCharsets.UTF_8))
    );
    Assertions.assertEquals(3, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(3, location.getWeakEntryCount());

    // Close one file
    file1.close();
    Assertions.assertEquals(2, location.getWeakStats().getHoldCount());
    // Entry remains
    Assertions.assertEquals(3, location.getWeakEntryCount());

    // Close another
    file2.close();
    Assertions.assertEquals(1, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(3, location.getWeakEntryCount());

    // Close last
    file3.close();
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(3, location.getWeakEntryCount());
  }

  @Test
  public void testLifecycleWithMultipleHoldsOnSameFile()
  {
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(0, location.getWeakEntryCount());

    String identifier = "shared-file";
    String content = "Shared content";

    // Create first hold
    CachedFile hold1 = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    );
    Assertions.assertEquals(1, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());

    // Get second hold on same file
    CachedFile hold2 = manager.get(identifier);
    Assertions.assertNotNull(hold2);
    Assertions.assertEquals(2, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());

    // Get third hold
    CachedFile hold3 = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> {
          throw new RuntimeException("Should not populate again");
        }
    );
    Assertions.assertEquals(3, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());

    // Release holds one by one - entry stays held until all are released
    hold1.close();
    // Entry is still held by other handles
    Assertions.assertEquals(2, location.getWeakStats().getHoldCount());

    hold2.close();
    Assertions.assertEquals(1, location.getWeakStats().getHoldCount());

    // After last close, no holds remain
    hold3.close();
    Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
    Assertions.assertEquals(1, location.getWeakEntryCount());
  }

  // ========== THREAD-SAFETY TESTS ==========

  @Test
  public void testConcurrentReserveAndPopulateSameIdentifier() throws Exception
  {
    withExecutor(executorService -> {
      String identifier = "concurrent-file";
      String content = "Test content";
      int numThreads = 10;

      Assertions.assertEquals(0, location.getWeakStats().getHoldCount());

      AtomicInteger populatorCallCount = new AtomicInteger(0);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(numThreads);

      List<Future<CachedFile>> futures = new ArrayList<>();

      // Launch multiple threads that all try to populate the same identifier
      for (int i = 0; i < numThreads; i++) {
        Future<CachedFile> future = executorService.submit(() -> {
          try {
            // Wait for all threads to be ready
            startLatch.await();

            CachedFile result = manager.reserveAndPopulate(
                identifier,
                content::length,
                file -> {
                  populatorCallCount.incrementAndGet();
                  // Add a small sleep to increase chance of race conditions if locking is broken
                  try {
                    Thread.sleep(10);
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
                }
            );

            return result;
          }
          finally {
            completionLatch.countDown();
          }
        });
        futures.add(future);
      }

      // Start all threads at once
      startLatch.countDown();

      // Wait for all to complete
      Assertions.assertTrue(completionLatch.await(10, TimeUnit.SECONDS));

      // Verify populator was called exactly once
      Assertions.assertEquals(
          1,
          populatorCallCount.get(),
          "Populator should only be called once despite concurrent access"
      );

      Assertions.assertEquals(numThreads, location.getWeakStats().getHoldCount());

      // Verify all threads got a valid CachedFile and close them
      for (Future<CachedFile> future : futures) {
        CachedFile cachedFile = future.get();
        Assertions.assertNotNull(cachedFile);
        cachedFile.close();
      }

      // After all closes, no active holds
      Assertions.assertEquals(0, location.getWeakStats().getHoldCount());
      Assertions.assertEquals(1, location.getWeakEntryCount());
    });
  }

  @Test
  public void testConcurrentGetWhilePopulating() throws Exception
  {
    withExecutor(executorService -> {
      String identifier = "concurrent-get-file";
      String content = "Test content";
      int numGetters = 5;

      CountDownLatch populateStarted = new CountDownLatch(1);
      CountDownLatch populateCanComplete = new CountDownLatch(1);
      CountDownLatch allComplete = new CountDownLatch(numGetters + 1);

      AtomicInteger nullGetCount = new AtomicInteger(0);
      AtomicInteger nonNullGetCount = new AtomicInteger(0);

      // Start populator thread
      Future<CachedFile> populateFuture = executorService.submit(() -> {
        try {
          CachedFile result = manager.reserveAndPopulate(
              identifier,
              content::length,
              file -> {
                populateStarted.countDown();
                // Wait before completing population
                try {
                  populateCanComplete.await();
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
                Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
              }
          );
          return result;
        }
        finally {
          allComplete.countDown();
        }
      });

      // Wait for population to start
      Assertions.assertTrue(populateStarted.await(5, TimeUnit.SECONDS));

      // Launch getter threads while population is in progress
      List<Future<CachedFile>> getterFutures = new ArrayList<>();
      for (int i = 0; i < numGetters; i++) {
        Future<CachedFile> future = executorService.submit(() -> {
          try {
            CachedFile result = manager.get(identifier);
            if (result == null) {
              nullGetCount.incrementAndGet();
            } else {
              nonNullGetCount.incrementAndGet();
            }
            return result;
          }
          finally {
            allComplete.countDown();
          }
        });
        getterFutures.add(future);
      }

      // Let population complete
      populateCanComplete.countDown();

      // Wait for all threads
      Assertions.assertTrue(allComplete.await(10, TimeUnit.SECONDS));

      // Clean up
      CachedFile populated = populateFuture.get();
      Assertions.assertNotNull(populated);
      populated.close();

      for (Future<CachedFile> future : getterFutures) {
        CachedFile result = future.get();
        if (result != null) {
          result.close();
        }
      }

      // Some gets might return null (before population), some might succeed (after)
      // Both are valid outcomes, we just verify no exceptions occurred
      Assertions.assertEquals(nullGetCount.get() + nonNullGetCount.get(), numGetters);
    });
  }

  @Test
  public void testConcurrentPopulationOfDifferentIdentifiers() throws Exception
  {
    withExecutor(executorService -> {
      int numIdentifiers = 10;
      String contentPrefix = "Content for file ";

      CyclicBarrier startBarrier = new CyclicBarrier(numIdentifiers);
      CountDownLatch completionLatch = new CountDownLatch(numIdentifiers);
      AtomicInteger successCount = new AtomicInteger(0);

      List<Future<CachedFile>> futures = new ArrayList<>();

      for (int i = 0; i < numIdentifiers; i++) {
        final int fileNum = i;
        Future<CachedFile> future = executorService.submit(() -> {
          try {
            // Wait for all threads to be ready
            startBarrier.await();

            String identifier = "file-" + fileNum;
            String content = contentPrefix + fileNum;

            CachedFile result = manager.reserveAndPopulate(
                identifier,
                content::length,
                file -> {
                  // Simulate some work
                  try {
                    Thread.sleep(5);
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                  Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8));
                }
            );

            successCount.incrementAndGet();
            return result;
          }
          finally {
            completionLatch.countDown();
          }
        });
        futures.add(future);
      }

      // Wait for all to complete
      Assertions.assertTrue(completionLatch.await(10, TimeUnit.SECONDS));

      // Verify all succeeded
      Assertions.assertEquals(numIdentifiers, successCount.get());

      // Verify all files are correct
      for (int i = 0; i < numIdentifiers; i++) {
        try (CachedFile cachedFile = futures.get(i).get()) {
          Assertions.assertNotNull(cachedFile);

          String expectedContent = contentPrefix + i;
          String actualContent = new String(
              Files.readAllBytes(cachedFile.getFile().toPath()),
              StandardCharsets.UTF_8
          );
          Assertions.assertEquals(expectedContent, actualContent);
        }
      }
    });
  }

  @Test
  public void testResolvedLocationIsReused()
  {
    String identifier = "resolved-location-file";
    String content = "Test content";

    // First reservation
    try (CachedFile cachedFile1 = manager.reserveAndPopulate(
        identifier,
        content::length,
        file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
    )) {
      Assertions.assertNotNull(cachedFile1);

      // Second reservation should use the cached resolved location
      // and find the existing file without trying other locations
      try (CachedFile cachedFile2 = manager.reserveAndPopulate(
          identifier,
          content::length,
          file -> {
            throw new RuntimeException("Should not populate again");
          }
      )) {
        Assertions.assertNotNull(cachedFile2);

        // Both should point to same underlying file
        Assertions.assertEquals(
            cachedFile1.getFile().getAbsolutePath(),
            cachedFile2.getFile().getAbsolutePath()
        );
      }
    }
  }

  @Test
  public void testConcurrentResolvedLocationLookup() throws Exception
  {
    withExecutor(executorService -> {
      String identifier = "concurrent-resolved-file";
      String content = "Test content";
      int numThreads = 10;

      // First, populate the file
      try (CachedFile initialFile = manager.reserveAndPopulate(
          identifier,
          content::length,
          file -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8))
      )) {
        Assertions.assertNotNull(initialFile);
      }

      // Now have multiple threads try to reserve the same identifier
      CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
      CountDownLatch completionLatch = new CountDownLatch(numThreads);
      AtomicInteger populatorCallCount = new AtomicInteger(0);

      List<Future<CachedFile>> futures = new ArrayList<>();

      for (int i = 0; i < numThreads; i++) {
        Future<CachedFile> future = executorService.submit(() -> {
          try {
            startBarrier.await();

            return manager.reserveAndPopulate(
                identifier,
                content::length,
                file -> {
                  populatorCallCount.incrementAndGet();
                  throw new RuntimeException("Should not be called");
                }
            );
          }
          finally {
            completionLatch.countDown();
          }
        });
        futures.add(future);
      }

      Assertions.assertTrue(completionLatch.await(10, TimeUnit.SECONDS));

      // Verify populator was never called (file already exists)
      Assertions.assertEquals(0, populatorCallCount.get());

      // Verify all got valid results
      for (Future<CachedFile> future : futures) {
        CachedFile cachedFile = future.get();
        Assertions.assertNotNull(cachedFile);
        cachedFile.close();
      }
    });
  }
}
