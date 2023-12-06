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

import { Api } from '../singletons';

import { maybeGetClusterCapacity } from './index';

export type CapabilitiesMode = 'full' | 'no-sql' | 'no-proxy';

export type CapabilitiesModeExtended =
  | 'full'
  | 'no-sql'
  | 'no-proxy'
  | 'no-sql-no-proxy'
  | 'coordinator-overlord'
  | 'coordinator'
  | 'overlord';

export type QueryType = 'none' | 'nativeOnly' | 'nativeAndSql';

export interface CapabilitiesValue {
  queryType: QueryType;
  multiStageQuery: boolean;
  coordinator: boolean;
  overlord: boolean;
  clusterCapacity?: number;
}

export class Capabilities {
  static STATUS_TIMEOUT = 15000;
  static FULL: Capabilities;
  static NO_SQL: Capabilities;
  static COORDINATOR_OVERLORD: Capabilities;
  static COORDINATOR: Capabilities;
  static OVERLORD: Capabilities;
  static NO_PROXY: Capabilities;

  private readonly queryType: QueryType;
  private readonly multiStageQuery: boolean;
  private readonly coordinator: boolean;
  private readonly overlord: boolean;
  private readonly clusterCapacity?: number;

  static async detectQueryType(): Promise<QueryType | undefined> {
    // Check SQL endpoint
    try {
      await Api.instance.post(
        '/druid/v2/sql?capabilities',
        { query: 'SELECT 1337', context: { timeout: Capabilities.STATUS_TIMEOUT } },
        { timeout: Capabilities.STATUS_TIMEOUT },
      );
    } catch (e) {
      const { response } = e;
      if (response.status !== 405 && response.status !== 404) {
        return; // other failure
      }
      try {
        await Api.instance.get('/status?capabilities', { timeout: Capabilities.STATUS_TIMEOUT });
      } catch (e) {
        return; // total failure
      }
      // Status works but SQL 405s => the SQL endpoint is disabled

      try {
        await Api.instance.post(
          '/druid/v2?capabilities',
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

  static async detectManagementProxy(): Promise<boolean> {
    try {
      await Api.instance.get(`/proxy/enabled?capabilities`, {
        timeout: Capabilities.STATUS_TIMEOUT,
      });
    } catch (e) {
      const { response } = e;
      // If we detect error code 400 the management proxy is enabled but just does not know about the recently added /proxy/enabled route so treat this as a win.
      return response.status === 400;
    }

    return true;
  }

  static async detectNode(node: 'coordinator' | 'overlord'): Promise<boolean> {
    try {
      await Api.instance.get(
        `/druid/${node === 'overlord' ? 'indexer' : node}/v1/isLeader?capabilities`,
        {
          timeout: Capabilities.STATUS_TIMEOUT,
        },
      );
    } catch (e) {
      return false;
    }

    return true;
  }

  static async detectMultiStageQuery(): Promise<boolean> {
    try {
      const resp = await Api.instance.get(`/druid/v2/sql/task/enabled?capabilities`);
      return Boolean(resp.data.enabled);
    } catch {
      return false;
    }
  }

  static async detectCapabilities(): Promise<Capabilities | undefined> {
    const queryType = await Capabilities.detectQueryType();
    if (typeof queryType === 'undefined') return;

    let coordinator: boolean;
    let overlord: boolean;
    if (queryType === 'none') {
      // must not be running on the router, figure out what node the console is on (or both?)
      coordinator = await Capabilities.detectNode('coordinator');
      overlord = await Capabilities.detectNode('overlord');
    } else {
      // must be running on the router, figure out if the management proxy is working
      coordinator = overlord = await Capabilities.detectManagementProxy();
    }

    const multiStageQuery = await Capabilities.detectMultiStageQuery();

    return new Capabilities({
      queryType,
      multiStageQuery,
      coordinator,
      overlord,
    });
  }

  static async detectCapacity(capabilities: Capabilities): Promise<Capabilities> {
    if (!capabilities.hasOverlordAccess()) return capabilities;

    const capacity = await maybeGetClusterCapacity();
    if (!capacity) return capabilities;

    return new Capabilities({
      ...capabilities.valueOf(),
      clusterCapacity: capacity.totalTaskSlots,
    });
  }

  constructor(value: CapabilitiesValue) {
    this.queryType = value.queryType;
    this.multiStageQuery = value.multiStageQuery;
    this.coordinator = value.coordinator;
    this.overlord = value.overlord;
    this.clusterCapacity = value.clusterCapacity;
  }

  public valueOf(): CapabilitiesValue {
    return {
      queryType: this.queryType,
      multiStageQuery: this.multiStageQuery,
      coordinator: this.coordinator,
      overlord: this.overlord,
      clusterCapacity: this.clusterCapacity,
    };
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
      if (coordinator && overlord) {
        return 'coordinator-overlord';
      }
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

  public hasMultiStageQuery(): boolean {
    return this.multiStageQuery;
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

  public getClusterCapacity(): number | undefined {
    return this.clusterCapacity;
  }
}
Capabilities.FULL = new Capabilities({
  queryType: 'nativeAndSql',
  multiStageQuery: true,
  coordinator: true,
  overlord: true,
});
Capabilities.NO_SQL = new Capabilities({
  queryType: 'nativeOnly',
  multiStageQuery: false,
  coordinator: true,
  overlord: true,
});
Capabilities.COORDINATOR_OVERLORD = new Capabilities({
  queryType: 'none',
  multiStageQuery: false,
  coordinator: true,
  overlord: true,
});
Capabilities.COORDINATOR = new Capabilities({
  queryType: 'none',
  multiStageQuery: false,
  coordinator: true,
  overlord: false,
});
Capabilities.OVERLORD = new Capabilities({
  queryType: 'none',
  multiStageQuery: false,
  coordinator: false,
  overlord: true,
});
Capabilities.NO_PROXY = new Capabilities({
  queryType: 'nativeAndSql',
  multiStageQuery: true,
  coordinator: false,
  overlord: false,
});
