package didatrade.server;

public interface ProposalStrategy {
    // Runs once per leadership term. Returns the instance number the
    // caller should resume proposing NEW client work from (accounting
    // for any value recovered from a previous leader at probeInstance),
    // or -1 if leadership wasn't established (ballot preempted - retry
    // once current_ballot updates).
    int establishLeadership(int ballot, int probeInstance);

    // Proposes one client value for one instance under the given ballot.
    // Returns false if rejected (ballot preempted).
    boolean propose(int instance, int ballot, int value);
}