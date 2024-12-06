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

export interface Margin {
  top: number;
  right: number;
  bottom: number;
  left: number;
}

export class Stage {
  public readonly width: number;
  public readonly height: number;

  constructor(width: number, height: number) {
    this.width = width;
    this.height = height;
  }

  public equals(other: Stage | undefined): boolean {
    return Boolean(other && this.width === other.width && this.height === other.height);
  }

  public applyMargin(margin: Margin): Stage {
    return new Stage(
      this.width - margin.left - margin.right,
      this.height - margin.top - margin.bottom,
    );
  }
}
