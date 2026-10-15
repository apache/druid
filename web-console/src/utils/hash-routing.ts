/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// The console routes on the URL hash without a leading slash: #view or #view/param

export interface HashRoute {
  /** The first path segment, lower cased, or '' for the home page */
  view: string;
  /** The second path segment, if any, as it appears in the URL */
  param?: string;
}

export function getHashPath(): string {
  // Read the hash from the href because some browsers (Firefox) pre-decode location.hash
  const href = window.location.href;
  const hashIndex = href.indexOf('#');
  return hashIndex === -1 ? '' : href.substring(hashIndex + 1);
}

export function replaceHashPath(hashPath: string): void {
  const href = window.location.href;
  const hashIndex = href.indexOf('#');
  window.location.replace(`${hashIndex === -1 ? href : href.slice(0, hashIndex)}#${hashPath}`);
}

export function parseHashRoute(hashPath: string): HashRoute {
  const searchIndex = hashPath.search(/[?#]/);
  let pathname = searchIndex === -1 ? hashPath : hashPath.slice(0, searchIndex);
  try {
    pathname = decodeURI(pathname);
  } catch {}

  const [view, param] = pathname.split('/');
  return {
    view: view.toLowerCase(),
    param: param || undefined,
  };
}
