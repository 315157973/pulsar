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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.UUID;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Starts 3 brokers that are in 3 different clusters
 */
@Test(groups = "broker")
public class ReplicatorTopicPoliciesTest extends ReplicatorTestBase {

    @Override
    @BeforeMethod(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        config1.setSystemTopicEnabled(true);
        config1.setTopicLevelPoliciesEnabled(true);
        config2.setSystemTopicEnabled(true);
        config2.setTopicLevelPoliciesEnabled(true);
        config3.setSystemTopicEnabled(true);
        config3.setTopicLevelPoliciesEnabled(true);
        super.setup();
        admin1.namespaces().removeBacklogQuota("pulsar/ns");
        admin1.namespaces().removeBacklogQuota("pulsar/ns1");
        admin1.namespaces().removeBacklogQuota("pulsar/global/ns");
    }

    @Override
    @AfterMethod(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testReplicatorTopicPolicies() throws Exception {
        final String namespace = "pulsar/partitionedNs-" + UUID.randomUUID();
        final String persistentTopicName = "persistent://" + namespace + "/partTopic" + UUID.randomUUID();

        admin1.namespaces().createNamespace(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet("r1", "r2", "r3"));
        // Create partitioned-topic from R1
        admin1.topics().createPartitionedTopic(persistentTopicName, 3);
        // List partitioned topics from R2
        Awaitility.await().untilAsserted(() -> assertNotNull(admin2.topics().getPartitionedTopicList(namespace)));
        Awaitility.await().untilAsserted(() -> assertEquals(
                admin2.topics().getPartitionedTopicList(namespace).get(0), persistentTopicName));
        assertEquals(admin1.topics().getList(namespace).size(), 3);
        // List partitioned topics from R3
        Awaitility.await().untilAsserted(() -> assertNotNull(admin3.topics().getPartitionedTopicList(namespace)));
        Awaitility.await().untilAsserted(() -> assertEquals(
                admin3.topics().getPartitionedTopicList(namespace).get(0), persistentTopicName));

        //int topic policies server
        pulsar1.getClient().newProducer().topic(persistentTopicName).create().close();
        pulsar2.getClient().newProducer().topic(persistentTopicName).create().close();
        pulsar3.getClient().newProducer().topic(persistentTopicName).create().close();

        RetentionPolicies retentionPolicies = new RetentionPolicies(1, 1);
        admin1.topicPolicies(true).setRetention(persistentTopicName, retentionPolicies);

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin2.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies);
        });
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin3.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies);
        });

        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin1.topicPolicies(true).getRetention(persistentTopicName), retentionPolicies);
            assertNull(admin1.topicPolicies().getRetention(persistentTopicName));
        });
    }

}
