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

export function shallowCopy(v: any): any {
  return Array.isArray(v) ? v.slice() : { ...v };
}

export function isEmpty(v: any): boolean {
  return !(Array.isArray(v) ? v.length : Object.keys(v).length);
}

function isObjectOrArray(v: any): boolean {
  return Boolean(v && typeof v === 'object');
}

export function parsePath(path: string): string[] {
  const parts: string[] = [];
  let rest = path;
  while (rest) {
    const escapedMatch = /^\{([^{}]*)\}(?:\.(.*))?$/.exec(rest);
    if (escapedMatch) {
      parts.push(escapedMatch[1]);
      rest = escapedMatch[2];
      continue;
    }

    const normalMatch = /^([^.]*)(?:\.(.*))?$/.exec(rest);
    if (normalMatch) {
      parts.push(normalMatch[1]);
      rest = normalMatch[2];
      continue;
    }

    throw new Error(`Could not parse path ${path}`);
  }

  return parts;
}

export function makePath(parts: string[]): string {
  return parts.map(p => (p.includes('.') ? `{${p}}` : p)).join('.');
}

function isAppend(key: string): boolean {
  return key === '[append]' || key === '-1';
}

export function deepGet<T extends Record<string, any>>(value: T, path: string): any {
  const parts = parsePath(path);
  for (const part of parts) {
    value = (value || {})[part];
  }
  return value;
}

export function deepSet<T extends Record<string, any>>(value: T, path: string, x: any): T {
  const parts = parsePath(path);
  let myKey = parts.shift()!; // Must be defined
  const valueCopy = shallowCopy(value);
  if (Array.isArray(valueCopy) && isAppend(myKey)) myKey = String(valueCopy.length);
  if (parts.length) {
    const nextKey = parts[0];
    const rest = makePath(parts);
    valueCopy[myKey] = deepSet(value[myKey] || (isAppend(nextKey) ? [] : {}), rest, x);
  } else {
    valueCopy[myKey] = x;
  }
  return valueCopy;
}

export function deepSetIfUnset<T extends Record<string, any>>(value: T, path: string, x: any): T {
  if (typeof deepGet(value, path) !== 'undefined') return value;
  return deepSet(value, path, x);
}

export function deepSetMulti<T extends Record<string, any>>(
  value: T,
  changes: Record<string, any>,
): T {
  let newValue = value;
  for (const k in changes) {
    newValue = deepSet(newValue, k, changes[k]);
  }
  return newValue;
}

export function deepDelete<T extends Record<string, any>>(value: T, path: string): T {
  const valueCopy = shallowCopy(value);
  const parts = parsePath(path);
  const firstKey = parts.shift()!; // Must be defined
  if (parts.length) {
    const firstKeyValue = value[firstKey];
    if (firstKeyValue) {
      const restPath = makePath(parts);
      const prunedFirstKeyValue = deepDelete(value[firstKey], restPath);

      if (isEmpty(prunedFirstKeyValue)) {
        delete valueCopy[firstKey];
      } else {
        valueCopy[firstKey] = prunedFirstKeyValue;
      }
    } else {
      delete valueCopy[firstKey];
    }
  } else {
    if (Array.isArray(valueCopy) && !isNaN(Number(firstKey))) {
      valueCopy.splice(Number(firstKey), 1);
    } else {
      delete valueCopy[firstKey];
    }
  }
  return valueCopy;
}

export function deepMove<T extends Record<string, any>>(
  value: T,
  fromPath: string,
  toPath: string,
): T {
  value = deepSet(value, toPath, deepGet(value, fromPath));
  value = deepDelete(value, fromPath);
  return value;
}

export function deepExtend<T extends Record<string, any>>(target: T, diff: Record<string, any>): T {
  if (typeof target !== 'object') throw new TypeError(`Invalid target`);
  if (typeof diff !== 'object') throw new TypeError(`Invalid diff`);

  const newValue = shallowCopy(target);
  for (const key in diff) {
    const targetValue = target[key];
    const diffValue = diff[key];
    if (typeof diffValue === 'undefined') {
      delete newValue[key];
    } else {
      if (isObjectOrArray(targetValue) && isObjectOrArray(diffValue)) {
        newValue[key] = deepExtend(targetValue, diffValue);
      } else {
        newValue[key] = diffValue;
      }
    }
  }

  return newValue;
}

export function allowKeys<T>(obj: T, keys: (keyof T)[]): T {
  const newObj: T = {} as any;
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      newObj[key] = obj[key];
    }
  }
  return newObj;
}

export function deleteKeys<T>(obj: T, keys: (keyof T)[]): T {
  const newObj: T = { ...obj };
  for (const key of keys) {
    delete newObj[key];
  }
  return newObj;
}
