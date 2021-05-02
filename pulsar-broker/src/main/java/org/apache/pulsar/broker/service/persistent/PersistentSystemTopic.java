/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.broker.service.persistent;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.NotAllowedException;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;

public class PersistentSystemTopic extends PersistentTopic {

    private Map<String, Long> highestVersionOfTopic = new ConcurrentHashMap<>();
    private Map<String, Object> lock = new ConcurrentHashMap<>();

    public PersistentSystemTopic(String topic, ManagedLedger ledger, BrokerService brokerService)
            throws BrokerServiceException.NamingException {
        super(topic, ledger, brokerService);
    }

    @Override
    public boolean isSizeBacklogExceeded() {
        return false;
    }

    @Override
    public boolean isTimeBacklogExceeded() {
        return false;
    }

    @Override
    public boolean isSystemTopic() {
        return true;
    }

    @Override
    public void checkMessageExpiry() {
        // do nothing for system topic
    }

    @Override
    public void checkGC() {
        // do nothing for system topic
    }

    @Override
    public void publishMessage(ByteBuf headersAndPayload, PublishContext publishContext) {
        String keyVersion = publishContext.getKeyVersion();
        if (StringUtils.isBlank(keyVersion)) {
            super.publishMessage(headersAndPayload, publishContext);
            return;
        }
        String key = StringUtils.substringBeforeLast(keyVersion, "_");
        long version = Long.parseLong(StringUtils.substringAfterLast(keyVersion, "_"));
        lock.putIfAbsent(key, new Object());
        synchronized (lock.get(key)) {
            Long currentVersion = highestVersionOfTopic.get(key);
            if (currentVersion == null) {
                highestVersionOfTopic.put(key, version);
                super.publishMessage(headersAndPayload, publishContext);
            } else if (currentVersion.equals((version - 1))) {
                highestVersionOfTopic.put(key, version);
                super.publishMessage(headersAndPayload, publishContext);
            } else {
                publishContext.completed(new BrokerServiceException.NotAllowedException("Version expired")
                        , -1, -1);
                decrementPendingWriteOpsAndCheck();
            }
        }
    }
}
