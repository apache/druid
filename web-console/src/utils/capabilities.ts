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

export type CapabilitiesMode = 'full' | 'no-sql' | 'no-proxy';

export interface CapabilitiesOptions {
  native: boolean;
  sql: boolean;
  coordinator: boolean;
  overlord: boolean;
}

export class Capabilities {
  static FULL: Capabilities;

  private native: boolean;
  private sql: boolean;
  private coordinator: boolean;
  private overlord: boolean;

  static fromMode(mode: CapabilitiesMode): Capabilities {
    return new Capabilities({
      native: mode !== 'no-sql',
      sql: mode !== 'no-sql',
      coordinator: mode !== 'no-proxy',
      overlord: mode !== 'no-proxy',
    });
  }

  constructor(options: CapabilitiesOptions) {
    this.native = options.native;
    this.sql = options.sql;
    this.coordinator = options.coordinator;
    this.overlord = options.overlord;
  }

  public getMode(): CapabilitiesMode {
    if (!this.hasSql()) return 'no-sql';
    if (!this.hasCoordinatorAccess()) return 'no-proxy';
    return 'full';
  }

  public hasEverything(): boolean {
    return this.native && this.sql && this.coordinator && this.overlord;
  }

  public hasNative(): boolean {
    return this.native;
  }

  public hasSql(): boolean {
    return this.sql;
  }

  public hasCoordinatorAccess(): boolean {
    return this.coordinator;
  }

  public hasOverlordAccess(): boolean {
    return this.overlord;
  }
}
Capabilities.FULL = Capabilities.fromMode('full');
