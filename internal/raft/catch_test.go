package raft

import "testing"

func TestCatchQuorumDoubleCount(t *testing.T) {
	if !CatchCorrectQuorumNoLoneLeader() {
		t.Fatal("correct raft lone node became leader")
	}
	if !CatchQuorumDoubleCount() {
		t.Fatal("failed to catch double-count quorum")
	}
}

func TestCatchStaleVote(t *testing.T) {
	if !CatchCorrectRejectsStaleVote() {
		t.Fatal("correct raft granted stale vote")
	}
	if !CatchStaleVoteGranted() {
		t.Fatal("failed to catch missing log-uptodate check")
	}
}

func TestCatchNoUpdateTermOnAE(t *testing.T) {
	if !CatchCorrectUpdatesTermOnAE() {
		t.Fatal("correct raft did not update term on AE")
	}
	if !CatchNoUpdateTermOnAE() {
		t.Fatal("failed to catch no-update-term-on-ae")
	}
}

func TestCatchOldTermCommit(t *testing.T) {
	if !CatchCorrectHoldsOldTerm() {
		t.Fatal("correct raft committed old-term entry on bare majority")
	}
	if !CatchOldTermBareMajority() {
		t.Fatal("failed to catch old-term bare majority commit")
	}
}
