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

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ZkCredentialsProvider.ZkCredentials;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ZkConnectionFactory {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String zkConnectString;
  private final int zkConnectTimeout;
  private final ZkCredentialsProvider credentials;

  public ZkConnectionFactory(String zkConnectString, int zkConnectTimeout) {
    this(zkConnectString, zkConnectTimeout, createZkCredentialsToAddAutomatically());
  }

  public ZkConnectionFactory(String zkConnectString, int zkConnectTimeout, ZkCredentialsProvider credentials) {
    this.zkConnectString = zkConnectString;
    this.zkConnectTimeout = zkConnectTimeout;
    this.credentials = credentials;
  }

  public int getClientTimeout() {
    return zkConnectTimeout;
  }

  public String getZkServerAddress() {
    return zkConnectString;
  }

  public ZkCredentialsProvider getCredentials() {
    return credentials;
  }

  protected SolrZooKeeper createSolrZooKeeper(final Watcher watcher) throws IOException {
    SolrZooKeeper result = new SolrZooKeeper(zkConnectString, zkConnectTimeout, watcher);

    for (ZkCredentials zkCredentials : credentials.getCredentials()) {
      result.addAuthInfo(zkCredentials.getScheme(), zkCredentials.getAuth());
    }

    return result;
  }

  public static final String ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkCredentialsProvider";

  private static ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    String zkCredentialsProviderClassName = System.getProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkCredentialsProviderClassName)) {
      try {
        log.info("Using ZkCredentialsProvider: " + zkCredentialsProviderClassName);
        return (ZkCredentialsProvider)Class.forName(zkCredentialsProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkCredentialsProvider does not point to a class implementing ZkCredentialsProvider and with a non-arg constructor", t);
      }
    }
    log.debug("Using default ZkCredentialsProvider");
    return new DefaultZkCredentialsProvider();
  }


}
