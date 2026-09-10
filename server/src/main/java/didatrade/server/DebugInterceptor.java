package didatrade.server;

import java.util.Random;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class DebugInterceptor implements ServerInterceptor {

    private final DidaTradeServerState serverState;

    public DebugInterceptor(DidaTradeServerState state) {
        this.serverState = state;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        
        // Block this gRPC thread if the server is frozen.
        // Once unfrozen, this method will return and the call will proceed normally.

        // Debug mode 2 is the freeze mode where the server is frozen and gRPC threads should block until unfrozen.
        if (serverState.getDebugMode() == 2) {
            System.out.println("Server is frozen. Blocking gRPC thread until unfrozen.");
            this.serverState.waitIfFrozen();
        }
        
        // Debug mode 4 is the slow mode where the server is in slow mode and gRPC threads should block until slow mode is turned off.
        if (serverState.getDebugMode() == 4) {
            System.out.println("Server is in slow mode. Blocking gRPC thread until slow mode is turned off.");
            Random rand = new Random();
            try {
                int sleepTime = rand.nextInt(5000) + 1000; // Sleep for 1-6 seconds
                System.out.println("Sleeping for " + sleepTime + " milliseconds.");
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return next.startCall(call, headers);
    }
}
