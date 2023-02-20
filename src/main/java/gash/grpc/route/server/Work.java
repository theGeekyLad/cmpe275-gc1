package gash.grpc.route.server;

import io.grpc.stub.StreamObserver;
import route.Route;

public class Work {
	private StreamObserver<route.Route> responseObserver;
	private Route request;

	private int stats;
	private int someOtherStuff;

	public Work(route.Route request, StreamObserver<route.Route> ro) {
		this.request = request;
		this.responseObserver = ro;
	}
}
