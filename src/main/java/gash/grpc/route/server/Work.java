package gash.grpc.route.server;

import io.grpc.stub.StreamObserver;
import route.Route;

public class Work {
	private StreamObserver<route.Route> responseObserver;
	Route request;

	private int stats;
	private int someOtherStuff;

	public Work(route.Route request, StreamObserver<route.Route> ro) {
		this.request = request;
		this.responseObserver = ro;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("W: ").append(request.getId());
		sb.append(" from: ").append(request.getOrigin());
		sb.append(" dest: ").append(request.getDestination());
		sb.append(" path: ").append(request.getPath());

		return sb.toString();
	}
}
