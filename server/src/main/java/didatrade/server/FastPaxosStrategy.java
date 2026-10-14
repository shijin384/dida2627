package didatrade.server;

import java.util.*;
import didatrade.DidaTradePaxos;
import didatrade.util.*;

public class FastPaxosStrategy implements ProposalStrategy {
    private final DidaTradeServerState state;
    private final ClassicPaxosStrategy classicFallback; // Phase 1 / collision recovery still classic

    public FastPaxosStrategy(DidaTradeServerState state) {
        this.state = state;
        this.classicFallback = new ClassicPaxosStrategy(state);
    }

    @Override
    public int establishLeadership(int ballot, int probeInstance) {
        // Leadership establishment is still a classic Phase 1 - fast
        // rounds only change Phase 2. Reuse it directly.
        return classicFallback.establishLeadership(ballot, probeInstance);
    }

    @Override
    public boolean propose(int instance, int ballot, int value) {
        List<Integer> acceptors = state.scheduler.acceptors(ballot);
        int n_acceptors = acceptors.size();

        DidaTradePaxos.FastAcceptRequest.Builder req = DidaTradePaxos.FastAcceptRequest.newBuilder();
        req.setInstance(instance);
        req.setRequestballot(ballot);
        req.setValue(value);
        DidaTradePaxos.FastAcceptRequest fast_req = req.build();

        ArrayList<DidaTradePaxos.FastAcceptReply> responses = new ArrayList<>();
        GenericResponseCollector<DidaTradePaxos.FastAcceptReply> collector =
            new GenericResponseCollector<>(responses, n_acceptors);

        for (int a : acceptors) {
            CollectorStreamObserver<DidaTradePaxos.FastAcceptReply> obs = new CollectorStreamObserver<>(collector);
            state.async_stubs[a].fastaccept(fast_req, obs);
        }
        collector.waitUntilDone();

        boolean anyRejected = responses.stream().anyMatch(r -> !r.getAccepted());
        if (anyRejected) {
            int maxballot = responses.stream().mapToInt(DidaTradePaxos.FastAcceptReply::getMaxballot).max().orElse(ballot);
            state.setCurrentBallot(maxballot);
            return false;
        }
        return true; // dispatched; decided-ness resolves asynchronously via fastlearn()
    }
}