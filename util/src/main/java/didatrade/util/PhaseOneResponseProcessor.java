package didatrade.util;

import java.util.ArrayList;

import didatrade.DidaTradePaxos;
import didatrade.configs.ConfigurationScheduler;

public class PhaseOneResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply> {
    private boolean accepted;
    private int value;
    private int valballot;
    private int maxballot;
    private int quorum;
    private int responses;

    public PhaseOneResponseProcessor(ConfigurationScheduler scheduler, int low_ballot, int high_ballot) {
        this.accepted = true;
        this.value = -1;
        this.valballot = -1;
        this.maxballot = -1;
        this.quorum = scheduler.quorum(high_ballot);
        this.responses = 0;
    }

    public boolean getAccepted() {
        return this.accepted && responses >= quorum;
    }

    public int getValue() {
        return this.value;
    }

    public int getValballot() {
        return this.valballot;
    }

    public int getMaxballot() {
        return this.maxballot;
    }

    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response) {
        if (last_response.getAccepted() == false) {
            this.accepted = false;
            if (last_response.getMaxballot() > this.maxballot) {
                this.maxballot = last_response.getMaxballot();
            }
            return true; // Stop early if rejected
        }
        this.responses++;

        // Keep track of the value with the highest valballot we've seen so far
        if (last_response.getValballot() > this.valballot) {
            this.valballot = last_response.getValballot();
            this.value = last_response.getValue();
        }

        if (this.responses >= this.quorum) {
            return true; // We reached a quorum!
        } else {
            return false; // Still need more responses
        }
    }
}
