package p2p.distributed.project.node;

public class FileMetaData {
    private Peer peer;
    private String name;

    public FileMetaData(Peer peer, String name) {
        this.peer = peer;
        this.name = name;
    }

    public Peer getPeer() {
        return peer;
    }

    public String getName() {
        return name;
    }
}
