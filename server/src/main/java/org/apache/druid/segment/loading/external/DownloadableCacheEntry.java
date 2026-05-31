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
import org.apache.druid.error.DruidException;
import org.apache.druid.io.FilePopulator;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.CacheEntry;
import org.apache.druid.segment.loading.CacheEntryIdentifier;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * CacheEntry implementation that calls a FilePopulator lambda when mounted.
 */
class DownloadableCacheEntry implements CacheEntry
{
  private static final Logger log = new Logger(DownloadableCacheEntry.class);

  private static final int MAX_PREFIX_SUFFIX_LENGTH = 50;

  private final StringCacheEntryIdentifier identifier;
  private final long sizeBytes;
  private final FilePopulator populator;
  private final File locationPath;

  private volatile boolean mounted = false;
  private volatile File mountedFile = null;

  /**
   * Map for keeping track of functionality added by {@link #extend}.
   */
  private final ConcurrentHashMap<Class<? extends Closeable>, Closeable> extendMap = new ConcurrentHashMap<>();

  public DownloadableCacheEntry(
      StringCacheEntryIdentifier identifier,
      long sizeBytes,
      FilePopulator populator,
      File locationPath
  )
  {
    this.identifier = identifier;
    this.sizeBytes = sizeBytes;
    this.populator = populator;
    this.locationPath = locationPath;
  }

  @Override
  public CacheEntryIdentifier getId()
  {
    return identifier;
  }

  @Override
  public long getSize()
  {
    return sizeBytes;
  }

  @Override
  public boolean isMounted()
  {
    return mounted;
  }

  @Override
  public void mount(StorageLocation location)
  {
    if (mounted) {
      return; // Already mounted
    }

    // Determine file path - delegate to the same sanitization logic
    File file = getFileForIdentifier(locationPath, identifier.value());

    // Ensure parent directory exists
    File parentDir = file.getParentFile();
    if (!parentDir.exists()) {
      try {
        FileUtils.mkdirp(parentDir);
      }
      catch (IOException e) {
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                             .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                             .build(e, "Failed to create parent directory [%s]", parentDir);
      }
    }

    if (!(file.exists() && file.length() == sizeBytes)) {
      // file doesn't exist (or is the wrong length), so we need populate it

      if (file.exists()) {
        // It wasn't the right length, let's delete it to avoid confusion for the populator.
        if (!file.delete()) {
          log.info("Problem deleting file [%s] before populating for VSM", file);
        }
      }

      try {
        populator.populate(file);
      }
      catch (Throwable e) {
        // Clean up partial file on failure
        if (file.exists()) {
          file.delete();
        }
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                            .build(e, "Failed to populate file [%s]", file);
      }

      // Verify the file was created
      if (!file.exists()) {
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                            .build("Populator did not create file [%s]", file);
      }
    }

    this.mountedFile = file;
    this.mounted = true;
  }

  @Override
  public void unmount()
  {
    final Closer extendCloser = CloseableUtils.forIterable(extendMap.values());
    extendMap.clear();
    CloseableUtils.closeAndSuppressExceptions(extendCloser, e -> log.warn(e, "Failed to close extended functionality"));

    // StorageLocation will call this when it needs to reclaim space
    if (mountedFile != null && mountedFile.exists()) {
      if (!mountedFile.delete()) {
        log.warn("Failed to delete file[%s] on unmount(). Leaving it.", mountedFile);
      }
    }
    mountedFile = null;
    mounted = false;
  }

  /**
   * Get the mounted file.
   *
   * @return The file, or null if not mounted
   */
  @Nullable
  File getFile()
  {
    return mountedFile;
  }

  /**
   * Extend this cache entry with additional functionality. The lifecycle of the extended functionality is
   * tracked along with the lifecycle of the cache entry. The provided supplier is called immediately unless
   * there is already a mapping for the class. Either way, the class is returned.
   */
  @SuppressWarnings("unchecked")
  public <T extends Closeable> T extend(Class<T> clazz, Supplier<T> supplier)
  {
    return (T) extendMap.computeIfAbsent(clazz, k -> supplier.get());
  }

  private File getFileForIdentifier(File locationPath, String identifier)
  {
    String sanitized = sanitizePath(identifier);
    return new File(locationPath, sanitized);
  }

  static String sanitizePath(final String originalPath)
  {
    String path = originalPath;
    path = path.replace('\\', '/');

    int startIndex = 0;
    if (path.startsWith("/")) {
      startIndex = 1;
    }
    int endIndex = path.endsWith("/") ? path.length() - 1 : path.length();

    // Split the kept portion into a prefix and a suffix so that file extensions (e.g. ".gz") are
    // preserved at the end of the sanitized name. The hashcode disambiguates collisions when
    // multiple paths share the same prefix+suffix.
    final int prefixEnd;
    final int suffixStart;
    if (endIndex - startIndex > MAX_PREFIX_SUFFIX_LENGTH * 2) {
      prefixEnd = startIndex + MAX_PREFIX_SUFFIX_LENGTH;
      suffixStart = endIndex - MAX_PREFIX_SUFFIX_LENGTH;
    } else {
      final int mid = startIndex + (endIndex - startIndex) / 2;
      final int extensionPos = indexOfExtension(path, startIndex, endIndex);
      final int split;
      if (extensionPos >= 0 && extensionPos < mid && endIndex - extensionPos < MAX_PREFIX_SUFFIX_LENGTH) {
        // Adjust the split point so the suffix contains the entire extension.
        split = extensionPos;
      } else {
        split = mid;
      }
      prefixEnd = split;
      suffixStart = split;
    }

    final StringBuilder sanitized = new StringBuilder();
    appendSanitized(sanitized, path, startIndex, prefixEnd);
    sanitized.append('-')
             .append(Hashing.sha512().hashUnencodedChars(originalPath).toString(), 0, 16)
             .append('-');
    appendSanitized(sanitized, path, suffixStart, endIndex);

    final String result = sanitized.toString();
    if (result.isEmpty()) {
      throw new IllegalArgumentException("Identifier resulted in empty path after sanitization");
    }

    return result;
  }

  private static void appendSanitized(StringBuilder dest, String path, int start, int end)
  {
    for (int i = start; i < end; i++) {
      char c = path.charAt(i);
      if (Character.isLetterOrDigit(c) || c == '-' || c == '_' || c == '.') {
        dest.append(c);
      } else {
        dest.append('_');
      }
    }
  }

  /**
   * Returns the position in {@code path} where the extension begins, or -1 if there is no extension.
   * Searches from startIndex (inclusive) to endIndex (exclusive). Only alphanumeric strings are
   * considered possible extensions.
   */
  private static int indexOfExtension(String path, int startIndex, int endIndex)
  {
    // Find the filename.
    int basenameStart = startIndex;
    for (int i = endIndex - 1; i >= startIndex; i--) {
      if (path.charAt(i) == '/') {
        basenameStart = i + 1;
        break;
      }
    }

    // Find the extension of the filename.
    int pos = endIndex;
    int extensionPos = -1;
    while (pos > basenameStart) {
      final int initialPos = pos;
      while (pos > basenameStart && isAlphaNumeric(path.charAt(pos - 1))) {
        pos--;
      }
      if (pos == initialPos) {
        // Saw some non-alphanumeric characters.
        break;
      }
      // Check if there's a dot. Use basenameStart + 1 because we don't want to consider ".dotfile" as an extension.
      if (pos > basenameStart + 1 && path.charAt(pos - 1) == '.') {
        // Found an extension. Remember the position and keep searching (for "xyz.log.gz" we want to capture ".log.gz").
        pos--;
        extensionPos = pos;
      } else {
        break;
      }
    }

    return extensionPos;
  }

  private static boolean isAlphaNumeric(char c)
  {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
  }
}
