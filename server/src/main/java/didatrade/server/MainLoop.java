package didatrade.server;

import java.util.*;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneResponseProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

import didatrade.configs.ConfigurationScheduler;

public class MainLoop implements Runnable {
	DidaTradeServerState server_state;

	private boolean has_work;

	// Leader-side: next instance this replica may try to fill with a
	// client command. Only meaningful while we are the established leader.
	private int next_instance_to_propose;

	// Every replica (leader or not): next instance, in strict order, whose
	// decided command must be applied to the state machine. Instances can
	// be *decided* out of order (pipelining), but are always *applied* in
	// order.
	private int next_instance_to_apply;

	// Multi-Paxos optimization: once Phase 1 succeeds for a ballot, skip it
	// for every later instance under the same ballot (see phasetwo's
	// acceptor check - the promise is global, not per-instance). -1 = not
	// established yet for our current ballot.
	private int established_ballot;

	public MainLoop(DidaTradeServerState state) {
		this.server_state = state;
		this.has_work = false;
		this.next_instance_to_propose = 0;
		this.next_instance_to_apply = 0;
		this.established_ballot = -1;
	}

	// All three cursor fields above are only ever touched by this worker
	// thread - no locking needed on them. Only has_work (via wakeup /
	// waitForWork) crosses threads, from DidaTradePaxosServiceImpl.learn().
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

	// ------------------------------------------------------------------
	// Proposing side (leader only). Does ONE unit of work per call -
	// either establishes leadership for a new ballot, or proposes one
	// pending client request - so run()'s loop naturally chains the two
	// without ever double-proposing a value for the same instance.
	// ------------------------------------------------------------------
	private boolean tryPropose() {
		int ballot = this.server_state.getCurrentBallot();
		if (this.server_state.scheduler.leader(ballot) != this.server_state.my_id)
			return false; // not the leader for the current ballot

		if (this.established_ballot != ballot) {
			if (!establishLeadership(ballot))
				return false; // lost the ballot race; retry once current_ballot updates
			this.established_ballot = ballot;
			return true;
		}

		RequestRecord request_record = this.server_state.req_history.getFirstPending();
		if (request_record == null)
			return false; // nothing PENDING - either idle, or everything outstanding is IN_PROGRESS

		int instance = this.next_instance_to_propose;
		PaxosInstance existing = this.server_state.paxos_log.getEntry(instance);
		if (existing != null && existing.decided) {
			this.next_instance_to_propose++;
			return true;
		}

		request_record.markInProgress(instance); // <-- THE FIX: getFirstPending() will now skip this record

		boolean ok = proposePhaseTwo(instance, ballot, request_record.getId());
		if (!ok) {
			request_record.markPendingAgain(); // release it back to PENDING for retry under new ballot
			this.established_ballot = -1;
			return false;
		}

		this.next_instance_to_propose++;
		return true;
	}

	// Runs Phase 1 ONCE per leadership term: resyncs the propose cursor to
	// the first not-yet-applied instance, and recovers/completes any value
	// a previous leader may already have accepted there. Everything after
	// this probe instance is free to go straight to Phase 2.
	private boolean establishLeadership(int ballot) {
		this.next_instance_to_propose = Math.max(this.next_instance_to_propose, this.next_instance_to_apply);
		int probe_instance = this.next_instance_to_propose;

		// For testing purposes
		System.out.println(
				"[MULTIPAXOS] Establishing leadership for ballot " + ballot + " probing instance " + probe_instance);

		List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
		int n_acceptors = acceptors.size();
		int low_ballot = Math.max(this.server_state.getCompletedBallot(), 0);

		DidaTradePaxos.PhaseOneRequest.Builder req = DidaTradePaxos.PhaseOneRequest.newBuilder();
		req.setInstance(probe_instance);
		req.setRequestballot(ballot);
		DidaTradePaxos.PhaseOneRequest phase_one_request = req.build();

		PhaseOneResponseProcessor processor = new PhaseOneResponseProcessor(this.server_state.scheduler, low_ballot,
				ballot);
		ArrayList<DidaTradePaxos.PhaseOneReply> responses = new ArrayList<>();
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector = new GenericResponseCollector<>(responses,
				n_acceptors, processor);

		for (int i = 0; i < n_acceptors; i++) {
			CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> obs = new CollectorStreamObserver<>(collector);
			this.server_state.async_stubs[acceptors.get(i)].phaseone(phase_one_request, obs);
		}
		collector.waitUntilDone();

		System.out.println("Leadership Phase 1 for ballot " + ballot + " probing instance "
				+ probe_instance + " -> accepted=" + processor.getAccepted());

		if (!processor.getAccepted()) {
			int maxballot = processor.getMaxballot();
			if (maxballot > this.server_state.getCurrentBallot())
				this.server_state.setCurrentBallot(maxballot);
			return false;
		}

		if (processor.getValballot() > -1) {
			// A previous leader left a value here - it MUST be re-proposed
			// as-is before this instance is free for new client work.
			// Advance past it; the caller's next tryPropose() call will
			// move on to fresh client requests starting at probe+1.
			proposePhaseTwo(probe_instance, ballot, processor.getValue());
			this.next_instance_to_propose = probe_instance + 1;
		}
		// else: nothing accepted there yet - probe_instance stays as
		// next_instance_to_propose, free for the caller to fill.

		return true;
	}

	// Fires Phase 2 for one instance and waits only for the Accept quorum
	// (not for the separate learn() notification round-trip) before
	// returning - this alone is the main latency win over the old
	// per-instance-decided blocking. See note at the end about extending
	// this to true fire-and-forget pipelining.
	private boolean proposePhaseTwo(int instance, int ballot, int value) {
		List<Integer> acceptors = this.server_state.scheduler.acceptors(ballot);
		int n_acceptors = acceptors.size();

		DidaTradePaxos.PhaseTwoRequest.Builder req = DidaTradePaxos.PhaseTwoRequest.newBuilder();
		req.setInstance(instance);
		req.setRequestballot(ballot);
		req.setValue(value);
		DidaTradePaxos.PhaseTwoRequest phase_two_request = req.build();

		PhaseTwoResponseProcessor processor = new PhaseTwoResponseProcessor(this.server_state.scheduler.quorum(ballot));
		ArrayList<DidaTradePaxos.PhaseTwoReply> responses = new ArrayList<>();
		GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(responses,
				n_acceptors, processor);

		for (int i = 0; i < n_acceptors; i++) {
			CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> obs = new CollectorStreamObserver<>(collector);
			this.server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request, obs);
		}
		collector.waitUntilDone();

		if (!processor.getAccepted()) {
			this.server_state.setCurrentBallot(processor.getMaxballot());
			return false;
		}
		return true;
		// Quorum-of-Accepted -> `decided` is recorded by the acceptors'
		// learner notification in DidaTradePaxosServiceImpl.phasetwo/.learn();
		// tryApply() below picks it up whenever it fires.
	}

	// ------------------------------------------------------------------
	// Applying side (every replica). Walks the log strictly in instance
	// order, applying every consecutively-decided entry. Stops at the
	// first gap - instances may be decided out of order, but they're
	// never applied out of order.
	// ------------------------------------------------------------------
	private boolean tryApply() {
		boolean did_something = false;

		while (true) {
			PaxosInstance entry = this.server_state.paxos_log.getEntry(this.next_instance_to_apply);
			if (entry == null || !entry.decided)
				break;

			RequestRecord request_record = this.server_state.req_history.getIfPending(entry.command_id);
			if (request_record == null)
				break; // decision arrived before this replica registered the client request locally

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
				result = this.server_state.trade_manager.add_user(command.getUserId(), command.getQuantity(),
						command.getStock());
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
}