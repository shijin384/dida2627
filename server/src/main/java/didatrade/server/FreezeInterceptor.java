package didatrade.server;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

public class FreezeInterceptor implements ServerInterceptor {

    private final DidaTradeServerState serverState;

    public FreezeInterceptor(DidaTradeServerState state) {
        this.serverState = state;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        
        // Block this gRPC thread if the server is frozen.
        // Once unfrozen, this method will return and the call will proceed normally.
        this.serverState.waitIfFrozen();

        return next.startCall(call, headers);
    }
}
