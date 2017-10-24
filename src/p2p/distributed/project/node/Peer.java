package p2p.distributed.project.node;

public class Peer {
    private String ip = "localhost";
    private int port = -1;
    private String username;

    public Peer(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return this.ip;
    }

    public int getPort() {
        return this.port;
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
