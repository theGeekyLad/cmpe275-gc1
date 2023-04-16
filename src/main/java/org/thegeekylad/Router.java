package org.thegeekylad;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

public class Router {
	private static Router router;
	private ManagedChannel ch;

	public static Router getInstance() {
		if (router == null)
			router = new Router();
		return router;
	}

	private void response(Route reply) {
		// TODO handle the reply/response from the server
		var payload = new String(reply.getPayload().toByteArray());
		System.out.println("reply: " + reply.getId() + ", from: " + reply.getOrigin() + ", payload: " + payload);
	}

	public void run(int linkPort, Route request) {
		ch = ManagedChannelBuilder.forAddress("localhost", linkPort).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);

		// blocking!
		var r = stub.request(request);

		response(r);

		close();
	}

	public void close() {
		if (ch != null)
			ch.shutdown();
	}
}
