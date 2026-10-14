package didatrade.server;

import java.util.*;
import didatrade.DidaTradePaxos;
import didatrade.util.*;

public class ClassicPaxosStrategy implements ProposalStrategy {
    private final DidaTradeServerState state;

    public ClassicPaxosStrategy(DidaTradeServerState state) {
        this.state = state;
    }

    @Override
    public int establishLeadership(int ballot, int probeInstance) {
        List<Integer> acceptors = state.scheduler.acceptors(ballot);
        int n_acceptors = acceptors.size();
        int low_ballot = Math.max(state.getCompletedBallot(), 0);

        System.out.println("[MULTIPAXOS] Establishing leadership (classic) for ballot " + ballot
            + " probing instance " + probeInstance);

        DidaTradePaxos.PhaseOneRequest.Builder req = DidaTradePaxos.PhaseOneRequest.newBuilder();
        req.setInstance(probeInstance);
        req.setRequestballot(ballot);

        PhaseOneResponseProcessor processor =
            new PhaseOneResponseProcessor(state.scheduler, low_ballot, ballot);
        ArrayList<DidaTradePaxos.PhaseOneReply> responses = new ArrayList<>();
        GenericResponseCollector<DidaTradePaxos.PhaseOneReply> collector =
            new GenericResponseCollector<>(responses, n_acceptors, processor);

        for (int a : acceptors) {
            CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> obs = new CollectorStreamObserver<>(collector);
            state.async_stubs[a].phaseone(req.build(), obs);
        }
        collector.waitUntilDone();

        System.out.println("Leadership Phase 1 for ballot " + ballot + " probing instance "
            + probeInstance + " -> accepted=" + processor.getAccepted());

        if (!processor.getAccepted()) {
            int maxballot = processor.getMaxballot();
            if (maxballot > state.getCurrentBallot()) state.setCurrentBallot(maxballot);
            return -1;
        }

        if (processor.getValballot() > -1) {
            // A previous leader left a value here - re-propose it as-is
            // before this instance is free for new client work.
            propose(probeInstance, ballot, processor.getValue());
            return probeInstance + 1;
        }
        return probeInstance; // nothing accepted there yet - free for the caller to fill
    }

    @Override
    public boolean propose(int instance, int ballot, int value) {
        List<Integer> acceptors = state.scheduler.acceptors(ballot);
        int n_acceptors = acceptors.size();

        DidaTradePaxos.PhaseTwoRequest.Builder req = DidaTradePaxos.PhaseTwoRequest.newBuilder();
        req.setInstance(instance);
        req.setRequestballot(ballot);
        req.setValue(value);

        PhaseTwoResponseProcessor processor =
            new PhaseTwoResponseProcessor(state.scheduler.quorum(ballot));
        ArrayList<DidaTradePaxos.PhaseTwoReply> responses = new ArrayList<>();
        GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> collector =
            new GenericResponseCollector<>(responses, n_acceptors, processor);

        for (int a : acceptors) {
            CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> obs = new CollectorStreamObserver<>(collector);
            state.async_stubs[a].phasetwo(req.build(), obs);
        }
        collector.waitUntilDone();

        if (!processor.getAccepted()) {
            state.setCurrentBallot(processor.getMaxballot());
            return false;
        }
        return true;
    }
}