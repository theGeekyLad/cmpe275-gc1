package gash.grpc.route.server;

public class Link {
	private String serverName;
	private int serverID;
	private int port;
	private String IP;

	private long created;
	private long lastCheck;

	// TODO what other data is needed?

	public Link(String name, int id, String IPaddr, int port) {
		created = System.currentTimeMillis();
		serverName = name;
		serverID = id;
		this.port = port;
		this.IP = IPaddr;
	}

	public void touchLastChecked() {
		lastCheck = System.currentTimeMillis();
	}

	public String getServerName() {
		return serverName;
	}

	public void setServerName(String serverName) {
		this.serverName = serverName;
	}

	public int getServerID() {
		return serverID;
	}

	public void setServerID(int serverID) {
		this.serverID = serverID;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getIP() {
		return IP;
	}

	public void setIP(String iP) {
		IP = iP;
	}

	public long getCreated() {
		return created;
	}

	public long getLastCheck() {
		return lastCheck;
	}

}
