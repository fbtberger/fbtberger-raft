package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.RequestVoteRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public final class HadoopTransportServer implements RaftTransportServer {

    private final RPC.Server server;

    public HadoopTransportServer(int port, RaftRpcHandler handler) throws IOException {
        this(port, handler, new Configuration());
    }

    public HadoopTransportServer(int port, RaftRpcHandler handler, Configuration conf) throws IOException {
        this.server = new RPC.Builder(conf)
                .setProtocol(HadoopRaftProtocol.class)
                .setInstance(new ProtocolImpl(handler))
                .setBindAddress("0.0.0.0")
                .setPort(port)
                .setNumHandlers(4)
                .build();
    }

    @Override
    public void start() throws IOException {
        server.start();
    }

    @Override
    public void close() {
        server.stop();
    }

    private static final class ProtocolImpl implements HadoopRaftProtocol {
        private final RaftRpcHandler handler;

        ProtocolImpl(RaftRpcHandler handler) { this.handler = handler; }

        @Override
        public BytesWritable requestVote(BytesWritable request) throws IOException {
            return new BytesWritable(handler.handleRequestVote(
                    RequestVoteRequest.parseFrom(unwrap(request))).toByteArray());
        }

        @Override
        public BytesWritable appendEntries(BytesWritable request) throws IOException {
            return new BytesWritable(handler.handleAppendEntries(
                    AppendEntriesRequest.parseFrom(unwrap(request))).toByteArray());
        }

        @Override
        public BytesWritable installSnapshot(BytesWritable request) throws IOException {
            return new BytesWritable(handler.handleInstallSnapshot(
                    InstallSnapshotRequest.parseFrom(unwrap(request))).toByteArray());
        }

        @Override
        public BytesWritable preVote(BytesWritable request) throws IOException {
            return new BytesWritable(handler.handlePreVote(
                    PreVoteRequest.parseFrom(unwrap(request))).toByteArray());
        }

        @Override
        public long getProtocolVersion(String protocol, long clientVersion) {
            return HadoopRaftProtocol.versionID;
        }

        @Override
        public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                                                       int clientMethodsHash) {
            return new ProtocolSignature(HadoopRaftProtocol.versionID, null);
        }

        private static byte[] unwrap(BytesWritable bw) {
            byte[] raw = bw.getBytes();
            int len = bw.getLength();
            if (raw.length == len) return raw;
            byte[] trimmed = new byte[len];
            System.arraycopy(raw, 0, trimmed, 0, len);
            return trimmed;
        }
    }
}
