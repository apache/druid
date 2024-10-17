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

import * as JSONBig from 'json-bigint-native';
import type { Dispatch, SetStateAction } from 'react';
import { useEffect, useState } from 'react';

import type { LocalStorageKeys } from '../utils';
import {
  base64UrlDecode,
  base64UrlEncode,
  localStorageGetJson,
  localStorageSetJson,
} from '../utils';

function encodeHashState(prefix: string, x: unknown): string {
  return prefix + base64UrlEncode(JSONBig.stringify(x));
}

function decodeHashState(prefix: string, x: string): any {
  if (!x.startsWith(prefix)) return;
  try {
    return JSONBig.parse(base64UrlDecode(x.slice(prefix.length)));
  } catch {
    return;
  }
}

export function useHashAndLocalStorageHybridState<T>(
  prefix: string,
  key: LocalStorageKeys,
  initialValue?: T,
  inflateFn?: (x: any) => T,
): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState(() => {
    // Try to read state from hash and fallback to local storage
    const valueToInflate =
      decodeHashState(prefix, window.location.hash) ?? localStorageGetJson(key);
    if (typeof valueToInflate === 'undefined') return initialValue;
    return inflateFn ? inflateFn(valueToInflate) : valueToInflate;
  });

  const setValue: Dispatch<SetStateAction<T>> = (value: T | ((prevState: T) => T)) => {
    const valueToStore = value instanceof Function ? value(state) : value;
    window.history.pushState(null, '', encodeHashState(prefix, value));
    setState(valueToStore);
    localStorageSetJson(key, valueToStore);
  };

  // Listen for "popstate" event (triggered by browser back/forward navigation)
  useEffect(() => {
    const handlePopState = () => {
      const valueToInflate = decodeHashState(prefix, window.location.hash);
      if (typeof valueToInflate === 'undefined') return;
      const value = inflateFn ? inflateFn(valueToInflate) : valueToInflate;
      setState(value);
      localStorageSetJson(key, value);
    };

    window.addEventListener('popstate', handlePopState);

    return () => {
      window.removeEventListener('popstate', handlePopState);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return [state, setValue];
}
