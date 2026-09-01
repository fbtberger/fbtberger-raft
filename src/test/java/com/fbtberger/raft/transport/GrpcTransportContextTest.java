/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.RaftServiceGrpc;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A peer RPC must not be cancelled because the call that happened to trigger it ended.
 *
 * <p>gRPC propagates an inbound call's {@link Context} to outbound calls made on the same
 * thread and cancels them with it. {@link com.fbtberger.raft.RaftNode} dispatches peer RPCs
 * from inside other RPCs' completion handlers -- TimeoutNow goes out from the AppendEntries
 * response callback, replication pipelines the next batch from the previous one's -- so
 * without care the outbound call inherits a context that is about to end.
 *
 * <p>This is not theory. A leadership transfer failed in two rehearsals out of three, always
 * with "CANCELLED: io.grpc.Context was cancelled without error" and always with the leader
 * still leading afterwards. It failed under load and worked when idle, which is exactly the
 * difference between dispatching from the caller's own thread and dispatching later from a
 * replication callback. The same error had been sitting in the node logs against
 * AppendEntries for days, where it is invisible because replication just sends again.
 *
 * <p>The server here never answers until the test lets it, so the cancellation is guaranteed
 * to arrive while the call is still in flight -- which is the only arrangement that can tell
 * a detached call from a lucky one.
 */
class GrpcTransportContextTest {

    private static final String SERVER = "grpc-transport-context-test";

    private Server server;
    private ManagedChannel channel;
    private GrpcTransport transport;
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch received = new CountDownLatch(1);
    private final ExecutorService responders = Executors.newCachedThreadPool();

    @BeforeEach
    void setUp() throws Exception {
        server = InProcessServerBuilder.forName(SERVER)
                .addService(new RaftServiceGrpc.RaftServiceImplBase() {
                    @Override
                    public void timeoutNow(TimeoutNowRequest request,
                                           StreamObserver<TimeoutNowResponse> observer) {
                        received.countDown();
                        // Answer from another thread once released, so the call is genuinely
                        // outstanding while the caller's context is cancelled.
                        responders.submit(() -> {
                            try {
                                release.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            observer.onNext(TimeoutNowResponse.getDefaultInstance());
                            observer.onCompleted();
                        });
                    }
                })
                .build().start();
        channel = InProcessChannelBuilder.forName(SERVER).build();
        transport = new GrpcTransport(channel);
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        responders.shutdownNow();
        if (transport != null) {
            transport.close();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void aPeerCallOutlivesTheContextItWasIssuedIn() throws Exception {
        Context.CancellableContext caller = Context.current().withCancellation();

        CompletableFuture<TimeoutNowResponse> pending = caller.call(
                () -> transport.timeoutNow(TimeoutNowRequest.newBuilder().setTerm(7).build()));

        assertTrue(received.await(5, TimeUnit.SECONDS), "the server never saw the call");

        // Exactly what happens when the AppendEntries handler that dispatched this returns.
        caller.cancel(new RuntimeException("the call that triggered this one ended"));

        release.countDown();
        assertNotNull(pending.get(5, TimeUnit.SECONDS),
                "the peer RPC died with the context it was issued in");
    }
}
