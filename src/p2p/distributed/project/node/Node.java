package p2p.distributed.project.node;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import static p2p.distributed.project.node.Node.INFO;
import static p2p.distributed.project.node.Node.log;

public class Node {
    public static final String REG_OK = "REGOK";
    public static final String INFO = "INFO";
    public static final String WARN = "WARN";
    public static final String ERROR = "ERROR";
    public static final String DEBUG = "DEBUG";
    public static final String SER = "SER";
    public static final String JOIN = "JOIN";
    public static final String SEROK = "SEROK";


    private List<Peer> routingTable = new ArrayList<>();
    private String[] fileList;
    private List<FileMetaData> fileMetaDataList = new ArrayList<>();

    private String bootstrapIp;
    private int bootstrapPort;
    private String nodeName;
    private String nodeIp;
    private int nodePort;
    private List<String> receivedSearchQueryList = new ArrayList<>();

    public Node(String bootstrapIp, int bootstrapPort, String nodeName, String nodeIp, int nodePort) {
        this.bootstrapIp = bootstrapIp;
        this.bootstrapPort = bootstrapPort;
        this.nodeName = nodeName;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
    }

    public String getBootstrapIp() {
        return this.bootstrapIp;
    }

    public int getBootstrapPort() {
        return this.bootstrapPort;
    }

    public String getNodeIp() {
        return this.nodeIp;
    }

    public int getNodePort() {
        return this.nodePort;
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public List<Peer> getRoutingTable() {
        return this.routingTable;
    }

    public String[] getFileList() {
        return this.fileList;
    }

    public void addToRoutingTable(Peer peer) {
        this.routingTable.add(peer);
        log(INFO, "UPDATE: Routing table : " + this.routingTable);
    }

    public static void main(String[] args) throws UnknownHostException {

        String bootstrapIp = "localhost";
        int bootstrapPort = 55555;

        String nodeIp = getNodeIpAddress();
        int nodePort = 11002;
        String nodeName = "node2";

        Node node = new Node(bootstrapIp, bootstrapPort, nodeName, nodeIp, nodePort);

        log(INFO, "This node : " + nodeIp + ":" + nodePort);

        node.assignFiles();

        List<Peer> peersToConnect = node.connectToBootstrapNode();

        //1. connect to bootstrap and get peers
        log(INFO, "Peers : " + peersToConnect);

        //2. connect to peers from above
        node.connectToPeers(peersToConnect, node.nodeIp, node.nodePort);

        //3. start listening
        //startListening(port);
        (new NodeThread(node)).start();

        //4. start listening to incoming search queries
        node.startListeningForSearchQueries();
    }

    private static String getNodeIpAddress() {
        String nodeIp = "";
        try {
//            DatagramSocket socket = new DatagramSocket();
//            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
//            nodeIp = socket.getLocalAddress().getHostAddress();

            nodeIp = InetAddress.getLocalHost().getHostAddress();
        } catch (IOException e) {
        }
        return nodeIp;
    }


    private void assignFiles() {
        String[] fileList = {
                "Adventures of Tintin",
                "Jack and Jill",
                "Glee",
                "The Vampire Diarie",
                "King Arthur",
                "Windows XP",
                "Harry Potter",
                "Kung Fu Panda",
                "Lady Gaga",
                "Twilight",
                "Windows 8",
                "Mission Impossible",
                "Turn Up The Music",
                "Super Mario",
                "American Pickers",
                "Microsoft Office 2010",
                "Happy Feet",
                "Modern Family",
                "American Idol",
                "Hacking for Dummies",
        };

        Random random = new Random();

        String[] subFileList = new String[5];
        log(INFO, "This node file list : ");
        for (int i = 0; i < 5; i++) {
            int randIndex = random.nextInt(fileList.length - 1);

            if (!Arrays.asList(subFileList).contains(fileList[randIndex])) {
                subFileList[i] = fileList[randIndex];
                System.out.println("\t\t" + subFileList[i]);
            } else {
                i--;
            }
        }

        this.fileList = subFileList;
    }

    private void startListeningForSearchQueries() {
        String fileName;
        Scanner in = new Scanner(System.in);

        while (true) {
            log(INFO, "Enter a file name as the search string : ");
            fileName = in.nextLine();
            log(DEBUG, "File name : " + fileName);
            //search its own list first
            String searchedFile = searchInFileList(fileName);

            if (!searchedFile.isEmpty()) {
                log(INFO, "Searched file '" + searchedFile + "' is in current node '" +
                        this.nodeIp + ":" + this.nodePort + "'");
            } else {
                sendSearchQuery(fileName, "");
            }
        }
    }

    private String getSenderAddressFromSearchQuery(String searchQuery) {
        String[] query = searchQuery.split(" ");
        return query[2] + ":" + query[3];
    }

    public void sendSearchQuery(String fileName, String searchQuery) {
        DatagramSocket clientSocket = null;

        if (this.receivedSearchQueryList.contains(searchQuery)) {
            log(INFO, "Query already received '" + searchQuery + "'");
            return;
        } else if (!searchQuery.isEmpty()) {
            this.receivedSearchQueryList.add(searchQuery);
        }

        try {
            for (Peer peer : this.routingTable) {
                String message;
                if (searchQuery.isEmpty()) {
                    message = this.prependTheLengthToMessage("SER " + this.nodeIp + " " +
                            this.nodePort + " \"" + fileName + "\"");
                } else {
                    String peerAddress = peer.getIp() + ":" + peer.getPort();
                    if (peerAddress.equals(getSenderAddressFromSearchQuery(searchQuery))) {
                        continue;
                    }
                    message = searchQuery;
                }

                this.receivedSearchQueryList.add(message);

                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();

                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log(INFO, "SEND: Sending search query '" + message + "' to '" + peer + "'");
                clientSocket.send(sendPacket);
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
        } finally {
            if (clientSocket != null) {
                clientSocket.close();
            }
        }
    }


    private void connectToPeers(List<Peer> peersToConnect, String nodeIp, int nodePort) {
        DatagramSocket clientSocket = null;
        try {
            for (Peer peer : peersToConnect) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();
                byte[] receiveData = new byte[1024];
                String message = this.prependTheLengthToMessage("JOIN " + nodeIp + " " + nodePort);
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log(INFO, "SEND: Join message to '" + peer + "'");
                clientSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                clientSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log(INFO, "RECEIVE: " + responseMessage + " from '" + peer + "'");

                if (responseMessage.contains("JOINOK 0")) {
                    this.addToRoutingTable(peer);
                } else {
                    log(ERROR, "Error in connecting to the peer");
                }

                //TODO: update file list
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
        } finally {
            if (clientSocket != null) {
                clientSocket.close();
            }
        }
    }

    public String prependTheLengthToMessage(String message) {
        return String.format("%04d", message.length() + 5) + " " + message;
    }

    private List<Peer> connectToBootstrapNode() {
        List<Peer> peers = new ArrayList<>();
        DatagramSocket clientSocket = null;

        try {
            InetAddress bootstrapHost = InetAddress.getByName(bootstrapIp);
            clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String message = this.prependTheLengthToMessage("REG " + nodeIp + " " + nodePort + " " + nodeName);
            byte[] sendData = message.getBytes();

            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, bootstrapHost, bootstrapPort);
            log(INFO, "SEND: Bootstrap server join message '" + message + "'");
            clientSocket.send(sendPacket);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);

            String responseMessage = new String(receivePacket.getData()).trim();
            log(INFO, "RECEIVE: Bootstrap server : " + responseMessage);

            //unsuccessful reply - FROM SERVER:0015 REGOK 9998
            //successful reply - FROM SERVER:0050 REGOK 2 10.100.1.124 57314 10.100.1.124 56314

            String[] response = responseMessage.split(" ");

            if (response.length >= 4 && REG_OK.equals(response[1])) {
                if (2 == Integer.parseInt(response[2]) || 1 == Integer.parseInt(response[2])) {
                    for (int i = 3; i < response.length; ) {
                        Peer neighbour = new Peer(response[i], Integer.parseInt(response[i + 1]));
                        peers.add(neighbour);
                        i = i + 2;
                    }
                } else {
                    log(WARN, responseMessage);
                }
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
        } finally {
            if (clientSocket != null) {
                clientSocket.close();
            }
        }
        return peers;
    }

    public static void log(String level, Object msg) {
        System.out.println(level + " : " + msg.toString());
    }

    public String searchInFileList(String fileName) {
        String queriedFile = "";

        for (String file : fileList) {
            if (file.contains(fileName)) {
                queriedFile = file;
                break;
            }
        }
        return queriedFile;
    }
}

class NodeThread extends Thread {

    Node node;

    NodeThread(Node node) {
        this.node = node;
    }

    private String getFileNameFromSearchQuery(String query) {
        String[] response = query.split(" ");

        String filename = response[4];
        for (int i = 5; i <= response.length - 1; i++) {
            filename += " " + response[i];
        }

        return filename.replace("\"", "");
    }

    public void run() {
        DatagramSocket serverSocket;
        try {
            serverSocket = new DatagramSocket(node.getNodePort());
            log(Node.INFO, "Started listening on '" + node.getNodePort() + "' for incoming data...");

            while (true) {
                byte[] buffer = new byte[65536];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                serverSocket.receive(incoming);

                byte[] data = incoming.getData();
                String incomingMessage = new String(data, 0, incoming.getLength());

                String[] response = incomingMessage.split(" ");
                byte[] sendData = null;

                InetAddress responseAddress = incoming.getAddress();
                int responsePort = incoming.getPort();

                if (response.length >= 5 && Node.SER.equals(response[1])) {
                    log(Node.INFO, "RECEIVE: Search query received from '" + responseAddress + ":" +
                            responsePort + "': " + incomingMessage);

                    String searchFilename = this.getFileNameFromSearchQuery(incomingMessage);

                    String fileSearchResults = node.searchInFileList(searchFilename);
                    String responseString = "";

                    if (fileSearchResults.isEmpty()) {
                        if (node.getRoutingTable().size() > 0) {
                            node.sendSearchQuery(searchFilename, incomingMessage);
                        }
                    } else {

                        responseString = node.prependTheLengthToMessage("SEROK " + node.getNodeIp() + " "
                                + node.getNodePort() + " " + fileSearchResults);

                        log(INFO, "FOUND: File found locally : " + responseString);

                        sendTheResultToOriginalNode(response[2], Integer.parseInt(response[3]), responseString);

                    }
                    sendData = responseString.getBytes();

                } else if (response.length >= 4 && Node.JOIN.equals(response[1])) {
                    log(Node.INFO, "RECEIVE: Join query received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");
                    sendData = node.prependTheLengthToMessage("JOINOK 0").getBytes();
                    node.addToRoutingTable(new Peer(responseAddress.getHostAddress(), Integer.parseInt(response[3])));
                } else if (response.length >= 4 && Node.SEROK.equals(response[1])) {
                    log(Node.INFO, "RECEIVE: Search results received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");
                }

                if (sendData != null) {
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, responseAddress,
                            responsePort);
                    serverSocket.send(sendPacket);
                }
            }
        } catch (IOException e) {
            log(Node.ERROR, e);
            e.printStackTrace();
        }
    }

    private void sendTheResultToOriginalNode(String responseIp, int responsePort, String message) {
        DatagramSocket serverSocket = null;
        try {
            serverSocket = new DatagramSocket();

            InetAddress responseAddress = InetAddress.getByName(responseIp);

            byte[] sendData = message.getBytes();

            log(INFO, "SEND: Search results to originated node '" + responseIp + ":" + responsePort + "'");

            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, responseAddress, responsePort);
            serverSocket.send(sendPacket);
        } catch (IOException e) {
            log(Node.ERROR, e);
            e.printStackTrace();
        } finally {
            if (serverSocket != null) {
                serverSocket.close();
            }
        }
    }
}
