package gash.grpc.route.server;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thegeekylad.util.ConsoleColors;
import org.thegeekylad.util.MyLogger;

/**
 * Core components to process work; shared with/across all sessions.
 * 
 * @author gash
 *
 */
public class Engine {

	public static Logger logger = LoggerFactory.getLogger("server");
	private static Engine instance;
	private static Properties conf;

	public enum ServerType {
		SERVER,
		CLIENT
	};

	public String serverName;
	public Long serverID;
	public Integer serverPort;
	public String serverRole;
	public Integer linkPort;
	private Long nextMessageID;

	/* workQueue/containers */
	public LinkedBlockingDeque<Work> workQueue, mgmtQueue;

	/* worker threads */
	public ArrayList<Worker> workers;
	public MgmtWorker manager;

	/* connectivity */
	public ArrayList<Link> links;

	// ------------------------------------------------------------------------

	public boolean hadBeenRunning;
	public MyLogger myLogger;

	// ------------------------------------------------------------------------

	private Engine() {
		hadBeenRunning = false;
		myLogger = new MyLogger("Engine", ConsoleColors.BLUE);
	}

	public static void configure(Properties conf) {
		if (Engine.conf == null) {
			Engine.conf = conf;
			instance = new Engine();
			instance.init();
		}
	}

	public static Engine getInstance() {
		if (instance == null)
			throw new RuntimeException("Engine not initialized");

		return instance;
	}

	public static Properties getConf() {
		return conf;
	}

	// ------------------------------------------------------------------------

	private synchronized void init() {
		if (conf == null) {
			Engine.logger.error("server is not configured!");
			throw new RuntimeException("server not configured!");
		}

		if (manager != null) {
			Engine.logger.error("trying to re-init() logistics!");
			return;
		}

		// extract settings. Here we are using basic properties which, requires
		// type checking and should also include range checking as well.

		String tmp = conf.getProperty("server.name");
		if (tmp == null)
			throw new RuntimeException("missing server name");
		serverName = tmp;

		tmp = conf.getProperty("server.id");
		if (tmp == null)
			throw new RuntimeException("missing server ID");
		serverID = Long.parseLong(tmp);

		tmp = conf.getProperty("server.port");
		if (tmp == null)
			throw new RuntimeException("missing server port");
		serverPort = Integer.parseInt(tmp);
		if (serverPort <= 1024)
			throw new RuntimeException("server port must be above 1024");

		tmp = conf.getProperty("server.role");
		if (tmp == null)
			throw new RuntimeException("missing server role");
		serverRole = tmp;

		// monotonically increasing number
		nextMessageID = 0L;

		// our list of connections to other servers
		links = new ArrayList<Link>();
		tmp = conf.getProperty("server.link.name");
		if (tmp != null) {
			Link link = new Link(
					tmp,
					Integer.parseInt(conf.getProperty("server.link.id")),
					conf.getProperty("server.link.ip"),
					Integer.parseInt(conf.getProperty("server.link.port"))
			);
			myLogger.log(String.format("Links to %s with an id of %d running at %s", link.getServerName(), link.getServerID(), link.getIP() + ":" + link.getPort()));
			links.add(link);
		}

		myLogger.log("starting queues");
		workQueue = new LinkedBlockingDeque<Work>();
		mgmtQueue = new LinkedBlockingDeque<Work>();

		myLogger.log("starting workers");
		workers = new ArrayList<Worker>();
		var w = new Worker();
		workers.add(w);
		w.start();

		myLogger.log("starting manager");
		manager = new MgmtWorker();
		manager.start();

		myLogger.log("initializaton complete");
	}

	public synchronized void shutdown(boolean hard) {
		myLogger.log("server shutting down.");

		if (!hard && workQueue.size() > 0) {
			try {
				while (workQueue.size() > 0) {
					Thread.sleep(2000);
				}
			} catch (InterruptedException e) {
				Engine.logger.error("Waiting for work queue to empty interrupted, shutdown hard", e);
			}
		}
		for (var w : workers) {
			w.shutdown();
		}

		manager.shutdown();
	}

	public synchronized void increaseWorkers() {
		var w = new Worker();
		workers.add(0, w);
		w.start();
	}

	public synchronized void decreaseWorkers() {
		if (workers.size() > 0) {
			var w = workers.remove(0);
			w.shutdown();
		}
	}

	public Long getServerID() {
		return serverID;
	}

	public synchronized Long getNextMessageID() {
		// TODO this should be a hash value (but we want to retain the implicit
		// ordering effect of an increasing number)
		if (nextMessageID == Long.MAX_VALUE)
			nextMessageID = 0l;

		return ++nextMessageID;
	}

	public Integer getServerPort() {
		return serverPort;
	}
}
