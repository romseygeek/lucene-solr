/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.common.cloud;

import org.apache.zookeeper.KeeperException;

/**
 * Hook to listen for changes to Zookeeper connection state
 */
public interface ZkConnectionListener {

  /**
   * Called when a new zookeeper session has been established.
   *
   * Implement this to create zookeeper watches or join elections once
   * a zookeeper connection has been made.
   */
  default void onConnect() throws KeeperException, InterruptedException {};

  /**
   * Called when a zookeeper session has expired.
   *
   * Implement this to clean up any internal state that should be reset
   * before a connection is re-established.
   *
   * N.B. You should not make calls into Zookeeper from within this method
   */
  default void onSessionExpiry() {};

}
