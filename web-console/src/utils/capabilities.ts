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

import axios from 'axios';

import { localStorageGetJson, LocalStorageKeys } from './local-storage-keys';

export type CapabilitiesMode = 'full' | 'no-sql' | 'no-proxy';

export type CapabilitiesModeExtended =
  | 'full'
  | 'no-sql'
  | 'no-proxy'
  | 'no-sql-no-proxy'
  | 'coordinator'
  | 'overlord';

export type QueryType = 'none' | 'nativeOnly' | 'nativeAndSql';

export interface CapabilitiesOptions {
  queryType: QueryType;
  coordinator: boolean;
  overlord: boolean;
}

export class Capabilities {
  static STATUS_TIMEOUT = 2000;
  static FULL: Capabilities;
  static COORDINATOR: Capabilities;
  static OVERLORD: Capabilities;

  private queryType: QueryType;
  private coordinator: boolean;
  private overlord: boolean;

  static async detectQueryType(): Promise<QueryType | undefined> {
    // Check SQL endpoint
    try {
      await axios.post(
        '/druid/v2/sql',
        { query: 'SELECT 1337', context: { timeout: Capabilities.STATUS_TIMEOUT } },
        { timeout: Capabilities.STATUS_TIMEOUT },
      );
    } catch (e) {
      const { response } = e;
      if (response.status !== 405 && response.status !== 404) {
        return; // other failure
      }
      try {
        await axios.get('/status', { timeout: Capabilities.STATUS_TIMEOUT });
      } catch (e) {
        return; // total failure
      }
      // Status works but SQL 405s => the SQL endpoint is disabled

      try {
        await axios.post(
          '/druid/v2',
          {
            queryType: 'dataSourceMetadata',
            dataSource: '__web_console_probe__',
            context: { timeout: Capabilities.STATUS_TIMEOUT },
          },
          { timeout: Capabilities.STATUS_TIMEOUT },
        );
      } catch (e) {
        if (response.status !== 405 && response.status !== 404) {
          return; // other failure
        }

        return 'none';
      }

      return 'nativeOnly';
    }

    return 'nativeAndSql';
  }

  static async detectNode(node: 'coordinator' | 'overlord'): Promise<boolean | undefined> {
    try {
      await axios.get(`/druid/${node === 'overlord' ? 'indexer' : node}/v1/isLeader`, {
        timeout: Capabilities.STATUS_TIMEOUT,
      });
    } catch (e) {
      return false;
    }

    return true;
  }

  static async detectCapabilities(): Promise<Capabilities | undefined> {
    const capabilitiesOverride = localStorageGetJson(LocalStorageKeys.CAPABILITIES_OVERRIDE);
    if (capabilitiesOverride) return new Capabilities(capabilitiesOverride as any);

    const queryType = await Capabilities.detectQueryType();
    if (typeof queryType === 'undefined') return;

    const coordinator = await Capabilities.detectNode('coordinator');
    if (typeof coordinator === 'undefined') return;

    const overlord = await Capabilities.detectNode('overlord');
    if (typeof overlord === 'undefined') return;

    return new Capabilities({
      queryType,
      coordinator,
      overlord,
    });
  }

  constructor(options: CapabilitiesOptions) {
    this.queryType = options.queryType;
    this.coordinator = options.coordinator;
    this.overlord = options.overlord;
  }

  public getMode(): CapabilitiesMode {
    if (!this.hasSql()) return 'no-sql';
    if (!this.hasCoordinatorAccess()) return 'no-proxy';
    return 'full';
  }

  public getModeExtended(): CapabilitiesModeExtended | undefined {
    const { queryType, coordinator, overlord } = this;

    if (queryType === 'nativeAndSql') {
      if (coordinator && overlord) {
        return 'full';
      }
      if (!coordinator && !overlord) {
        return 'no-proxy';
      }
    } else if (queryType === 'nativeOnly') {
      if (coordinator && overlord) {
        return 'no-sql';
      }
      if (!coordinator && !overlord) {
        return 'no-sql-no-proxy';
      }
    } else {
      if (coordinator) {
        return 'coordinator';
      }
      if (overlord) {
        return 'overlord';
      }
    }

    return;
  }

  public hasEverything(): boolean {
    return this.queryType === 'nativeAndSql' && this.coordinator && this.overlord;
  }

  public hasQuerying(): boolean {
    return this.queryType !== 'none';
  }

  public hasSql(): boolean {
    return this.queryType === 'nativeAndSql';
  }

  public hasCoordinatorAccess(): boolean {
    return this.coordinator;
  }

  public hasSqlOrCoordinatorAccess(): boolean {
    return this.hasSql() || this.hasCoordinatorAccess();
  }

  public hasOverlordAccess(): boolean {
    return this.overlord;
  }

  public hasSqlOrOverlordAccess(): boolean {
    return this.hasSql() || this.hasOverlordAccess();
  }
}
Capabilities.FULL = new Capabilities({
  queryType: 'nativeAndSql',
  coordinator: true,
  overlord: true,
});
Capabilities.COORDINATOR = new Capabilities({
  queryType: 'none',
  coordinator: true,
  overlord: false,
});
Capabilities.OVERLORD = new Capabilities({
  queryType: 'none',
  coordinator: false,
  overlord: true,
});
