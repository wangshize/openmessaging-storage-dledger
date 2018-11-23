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

package io.openmessaging.storage.dleger;

import io.openmessaging.storage.dleger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;
import io.openmessaging.storage.dleger.utils.UtilAll;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class LeaderElectorTest extends ServerTestHarness {

    @Test
    public void testSingleServer() throws Exception {
        String group = UUID.randomUUID().toString();
        DLegerServer dLegerServer = launchServer(group, String.format("n0-localhost:%d", nextPort()), "n0");
        MemberState memberState = dLegerServer.getMemberState();
        Thread.sleep(1000);
        Assert.assertTrue(memberState.isLeader());
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(dLegerServer.getMemberState().getSelfId());
            appendEntryRequest.setBody("Hello Single Server".getBytes());
            AppendEntryResponse appendEntryResponse = dLegerServer.getdLegerRpcService().append(appendEntryRequest).get();
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
        long term = memberState.currTerm();
        dLegerServer.shutdown();
        dLegerServer = launchServer(group, "n0-localhost:10011", "n0");
        memberState = dLegerServer.getMemberState();
        Thread.sleep(1000);
        Assert.assertTrue(memberState.isLeader());
        Assert.assertEquals(term, memberState.currTerm());

    }

    @Test
    public void testThreeServer() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(1000);
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        DLegerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(2, followerNum.get());
        Assert.assertNotNull(leaderServer);

        for (int i = 0; i < 10; i++) {
            long maxTerm = servers.stream().max((o1, o2) -> {
                if (o1.getMemberState().currTerm() < o2.getMemberState().currTerm()) {
                    return -1;
                } else if (o1.getMemberState().currTerm() > o2.getMemberState().currTerm()) {
                    return 1;
                } else {
                    return 0;
                }
            }).get().getMemberState().currTerm();
            DLegerServer candidate = servers.get(i % servers.size());
            candidate.getdLegerLeaderElector().testRevote(maxTerm + 1);
            Thread.sleep(100);
            leaderNum.set(0);
            followerNum.set(0);
            leaderServer = parseServers(servers, leaderNum, followerNum);
            Assert.assertEquals(1, leaderNum.get());
            Assert.assertEquals(2, followerNum.get());
            Assert.assertNotNull(leaderServer);
            Assert.assertTrue(candidate == leaderServer);
        }
        //write some data
        for (int i = 0; i < 5; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(leaderServer.getMemberState().getSelfId());
            appendEntryRequest.setBody("Hello Three Server".getBytes());
            AppendEntryResponse appendEntryResponse = leaderServer.getdLegerRpcService().append(appendEntryRequest).get();
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
    }

    @Test
    public void testThreeServerAndRestartFollower() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(1000);
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        DLegerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(2, followerNum.get());
        Assert.assertNotNull(leaderServer);

        //restart the follower, the leader should keep the same
        long term = leaderServer.getMemberState().currTerm();
        for (DLegerServer server : servers) {
            if (server == leaderServer) {
                continue;
            }
            String followerId = server.getMemberState().getSelfId();
            server.shutdown();
            server = launchServer(group, peers, followerId);
            Thread.sleep(2000);
            Assert.assertTrue(server.getMemberState().isFollower());
            Assert.assertTrue(leaderServer.getMemberState().isLeader());
            Assert.assertEquals(term, server.getMemberState().currTerm());
        }
    }

    @Test
    public void testThreeServerAndShutdownLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        while (parseServers(servers, leaderNum, followerNum) == null && UtilAll.elapsed(start) < 1000) {
            Thread.sleep(100);
        }
        Thread.sleep(300);
        leaderNum.set(0);
        followerNum.set(0);
        DLegerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(2, followerNum.get());
        Assert.assertNotNull(leaderServer);

        //shutdown the leader, should elect another leader
        leaderServer.shutdown();
        Thread.sleep(1500);
        List<DLegerServer> leftServers = new ArrayList<>();
        for (DLegerServer server : servers) {
            if (server != leaderServer) {
                leftServers.add(server);
            }
        }
        start = System.currentTimeMillis();
        while (parseServers(leftServers, leaderNum, followerNum) == null && UtilAll.elapsed(start) < 3 * leaderServer.getdLegerConfig().getHeartBeatTimeIntervalMs()) {
            Thread.sleep(100);
        }
        leaderNum.set(0);
        followerNum.set(0);
        Assert.assertNotNull(parseServers(leftServers, leaderNum, followerNum));
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(1, followerNum.get());

    }

    @Test
    public void testThreeServerAndShutdownFollowers() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);

        long start = System.currentTimeMillis();
        while (parseServers(servers, leaderNum, followerNum) == null && UtilAll.elapsed(start) < 1000) {
            Thread.sleep(100);
        }
        Thread.sleep(300);
        leaderNum.set(0);
        followerNum.set(0);

        DLegerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(2, followerNum.get());
        Assert.assertNotNull(leaderServer);

        //restart the follower, the leader should keep the same
        for (DLegerServer server : servers) {
            if (server == leaderServer) {
                continue;
            }
            server.shutdown();
        }

        long term = leaderServer.getMemberState().currTerm();
        start = System.currentTimeMillis();
        while (leaderServer.getMemberState().isLeader() && UtilAll.elapsed(start) < 4 * leaderServer.getdLegerConfig().getHeartBeatTimeIntervalMs()) {
            Thread.sleep(100);
        }
        Assert.assertTrue(leaderServer.getMemberState().isCandidate());
        Assert.assertEquals(term, leaderServer.getMemberState().currTerm());
    }

}

