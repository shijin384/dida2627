package didatrade.server;

import java.util.*;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;

import didatrade.configs.ConfigurationScheduler;

public class MainLoop implements Runnable {
    DidaTradeServerState server_state;

    private boolean has_work;
    private int next_instance_to_propose;
    private int next_instance_to_apply;
    private int established_ballot;

    private final ProposalStrategy classic;
    private final ProposalStrategy fast;

    public MainLoop(DidaTradeServerState state) {
        this.server_state = state;
        this.has_work = false;
        this.next_instance_to_propose = 0;
        this.next_instance_to_apply = 0;
        this.established_ballot = -1;
        this.classic = new ClassicPaxosStrategy(state);
        this.fast = new FastPaxosStrategy(state);
    }

    public void run() {
        while (true) {
            this.server_state.waitIfFrozen();
            boolean did_something = tryPropose();
            did_something |= tryApply();
            if (!did_something)
                waitForWork();
        }
    }

    public synchronized void wakeup() {
        this.has_work = true;
        notifyAll();
    }

    private synchronized void waitForWork() {
        this.has_work = false;
        while (!this.has_work) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean tryPropose() {
        int ballot = this.server_state.getCurrentBallot();
        if (this.server_state.scheduler.leader(ballot) != this.server_state.my_id)
            return false;

        if (this.established_ballot != ballot) {
            int probe = probeInstanceFor();
			boolean fast = this.server_state.scheduler.fastpaxos(ballot);
    		this.server_state.setFastPaxosMode(fast);
            int result = strategyFor().establishLeadership(ballot, probe);
            if (result < 0)
                return false; // lost the ballot race; retry once current_ballot updates
            this.next_instance_to_propose = result;
            this.established_ballot = ballot;
            return true;
        }

        // Collision recovery (fast rounds only - classic entries never set
        // fast_collision): resolve BEFORE handing out any new instance.
        PaxosInstance pending_check = this.server_state.paxos_log.getEntry(this.next_instance_to_apply);
        if (pending_check != null && !pending_check.decided && pending_check.fast_collision) {
            int recoveredValue = pending_check.leadingVoteAndCount()[0];
            System.out.println("[MULTIPAXOS] Recovering collided instance " + this.next_instance_to_apply
                + " via classic round, value=" + recoveredValue);
            boolean ok = classic.propose(this.next_instance_to_apply, ballot, recoveredValue);
            if (!ok) {
                this.established_ballot = -1;
                return false;
            }
            return true;
        }

        RequestRecord request_record = this.server_state.req_history.getFirstPending();
        if (request_record == null)
            return false;

        int instance = this.next_instance_to_propose;
        PaxosInstance existing = this.server_state.paxos_log.getEntry(instance);
        if (existing != null && existing.decided) {
            this.next_instance_to_propose++;
            return true;
        }

        request_record.markInProgress(instance);
        boolean ok = strategyFor().propose(instance, ballot, request_record.getId());
        if (!ok) {
            request_record.markPendingAgain();
            this.established_ballot = -1;
            return false;
        }
        this.next_instance_to_propose++;
        return true;
    }

    // Where a new leadership term should start probing from: never behind
    // what's already applied, and never behind where we'd already gotten
    // to proposing (covers the case of losing and regaining leadership
    // without any apply progress in between).
    private int probeInstanceFor() {
        this.next_instance_to_propose = Math.max(this.next_instance_to_propose, this.next_instance_to_apply);
        return this.next_instance_to_propose;
    }

    private boolean tryApply() {
        boolean did_something = false;
        while (true) {
            PaxosInstance entry = this.server_state.paxos_log.getEntry(this.next_instance_to_apply);
            if (entry == null || !entry.decided)
                break;

            RequestRecord request_record = this.server_state.req_history.getIfPending(entry.command_id);
            if (request_record == null)
                break;

            applyCommand(request_record);
            System.out.println("Applied instance " + this.next_instance_to_apply
                + " command id = " + entry.command_id);
            this.next_instance_to_apply++;
            did_something = true;
        }
        return did_something;
    }

    private void applyCommand(RequestRecord request_record) {
        DidaTradeCommand command = request_record.getRequest();
        boolean result = false;
        int balance = 0;

        switch (command.getAction()) {
        case DidaTradeAction.POPULATE:
            result = this.server_state.trade_manager.populate(command.getQuantity());
            break;
        case DidaTradeAction.ADDUSER:
            result = this.server_state.trade_manager.add_user(command.getUserId(), command.getQuantity(), command.getStock());
            break;
        case DidaTradeAction.SELL:
            result = this.server_state.trade_manager.sell(command.getUserId(), command.getQuantity());
            break;
        case DidaTradeAction.BUY:
            result = this.server_state.trade_manager.acquire(command.getUserId(), command.getQuantity());
            break;
        case DidaTradeAction.BALANCE:
            balance = this.server_state.trade_manager.balance(command.getUserId());
            command.setQuantity(balance);
            result = (balance != -1);
            break;
        case DidaTradeAction.DUMP:
            this.server_state.trade_manager.dump();
            result = true;
            break;
        default:
            result = false;
            System.err.println("*** Unknown command ****");
            break;
        }

        request_record.setResponse(result);
        this.server_state.req_history.moveToProcessed(request_record.getId());
    }

    private ProposalStrategy strategyFor() {
    	return this.server_state.getFastPaxosMode() ? this.fast : this.classic;
	}
}