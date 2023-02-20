package gash.grpc.route.server;

import io.grpc.stub.StreamObserver;

public class Worker extends Thread {

	private RouteServerImpl _server;
	
	public Worker(RouteServerImpl server) {
		this._server = server;
	}

	private void doWork(Work w) {
		if (w != null) {
			// TODO do work
		}
	}

	@Override
	public void run() {
		// TODO not a good idea to spin on work --> wastes CPU cycles
		while (true) {
			var w = _server.checkForWork();
			doWork(w);
		}
	}
}
