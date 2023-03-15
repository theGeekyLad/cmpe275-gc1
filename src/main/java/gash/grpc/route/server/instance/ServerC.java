package gash.grpc.route.server.instance;

import gash.grpc.route.server.RouteServerImpl;
import io.grpc.stub.StreamObserver;
import route.Route;

public class ServerC extends RouteServerImpl {

    public static void main(String[] args) throws Exception {
        RouteServerImpl.main(new String[]{"/home/thegeekylad/Classes/cmpe-275/lab-2/grpc-playground/grpc-playground/src/main/resources/conf/serverC.conf"});
    }

    @Override
    public void request(Route request, StreamObserver<Route> responseObserver) {
        super.request(request, responseObserver);
        responseObserver.onCompleted();
    }
}
