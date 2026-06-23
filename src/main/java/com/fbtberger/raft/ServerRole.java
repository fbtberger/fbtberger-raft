package com.fbtberger.raft;

/** The three roles a Raft server can be in at any time (paper, Figure 4). */
public enum ServerRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
