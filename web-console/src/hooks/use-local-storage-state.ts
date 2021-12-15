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
import { Dispatch, SetStateAction, useState } from 'react';

export function useLocalStorageState<T>(
  key: string,
  initialValue?: T,
): [T, Dispatch<SetStateAction<T>>] {
  const [state, setState] = useState(() => {
    try {
      const item = window.localStorage.getItem(key);
      return item ? JSONBig.parse(item) : initialValue;
    } catch {
      return initialValue;
    }
  });

  const setValue: Dispatch<SetStateAction<T>> = (value: T | ((prevState: T) => T)) => {
    const valueToStore = value instanceof Function ? value(state) : value;
    setState(valueToStore);
    try {
      window.localStorage.setItem(key, JSONBig.stringify(valueToStore));
    } catch {}
  };
  return [state, setValue];
}
