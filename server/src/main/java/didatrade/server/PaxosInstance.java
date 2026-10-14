package didatrade.server;

import java.util.HashMap;
import java.util.Map;

public class PaxosInstance {
    int instance_nb;
    int command_id;
    int read_ballot;
    int write_ballot;
    int accept_ballot;
    int n_accepts;
    boolean decided;
    boolean value_is_locked;
    Map<Integer, Integer> fast_votes = new HashMap<>(); // acceptorId -> value
    int fast_ballot = -1;        // ballot these fast_votes belong to
    boolean fast_collision = false;  // true once a collision is confirmed (no value can reach super-majority)

    public PaxosInstance() {
	this.instance_nb     = 0;
        this.command_id      = 0;
        this.read_ballot     = -1;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.n_accepts       = 0;
	this.decided         = false;
        this.value_is_locked = false;
    }

    
    public PaxosInstance(int id) {
	this.instance_nb     = id;
        this.command_id      = 0;
        this.read_ballot     = -1;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.n_accepts       = 0;
	this.decided         = false;
        this.value_is_locked = false;
    }

    public PaxosInstance(int id, int ballot) {
	this.instance_nb     = id;
        this.command_id      = 0;
        this.read_ballot     = ballot;
        this.write_ballot    = -1;
	this.accept_ballot   = -1;
        this.n_accepts       = 0;
	this.decided         = false;
        this.value_is_locked = false;
    }

        // Records one acceptor's fast vote; resets bookkeeping if this is a
    // higher ballot than what we'd previously tracked (new fast round).
    synchronized void recordFastVote(int ballot, int acceptorId, int value) {
        if (ballot > this.fast_ballot) {
            this.fast_votes.clear();
            this.fast_ballot = ballot;
            this.fast_collision = false;
        }
        if (ballot == this.fast_ballot) {
            this.fast_votes.put(acceptorId, value);
        }
    }

    // Returns the value with the most votes, and how many votes it has -
    // used by fastlearn() to check against superMajority / detect collision.
    synchronized int[] leadingVoteAndCount() {
        Map<Integer, Integer> tally = new HashMap<>();
        for (int v : this.fast_votes.values())
            tally.merge(v, 1, Integer::sum);

        int bestValue = -1, bestCount = 0;
        for (Map.Entry<Integer, Integer> e : tally.entrySet()) {
            if (e.getValue() > bestCount) {
                bestCount = e.getValue();
                bestValue = e.getKey();
            }
        }
        return new int[]{bestValue, bestCount};
    }

    synchronized int votesReceived() {
        return this.fast_votes.size();
    }

}
