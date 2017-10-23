package p2p.distributed.project.node;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;
import java.net.NetworkInterface;

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

    private String bootstrapIp = "192.168.8.101";
    private int bootstrapPort = 55555;
    private String nodeIp = "";
    private int nodePort = 11004;
    private String nodeName = "node4";


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

    public static void main(String[] args) {


        Node node = new Node();
        node.assignTheNodeIpAddress();
        node.fileList = node.assignFiles();

        List<Peer> peersToConnect = node.connectToBootstrapNode(node.bootstrapIp, node.bootstrapPort, node.nodeIp, node.nodePort, node.nodeName);

        //1. connect to bootstrap and get peers
        node.log(INFO, "Peers : " + peersToConnect);

        //2. connect to peers from above
        node.connectToPeers(peersToConnect, node.nodeIp, node.nodePort);

        //3. start listening
        //startListening(port);
        (new NodeThread(node)).start();

        //4. start listening to incoming search queries
        node.startListeningForSearchQueries();
    }

    private void assignTheNodeIpAddress() {
        try {
            DatagramSocket socket = new DatagramSocket();
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            this.nodeIp = socket.getLocalAddress().getHostAddress();
        } catch (IOException e) {
        }
    }

    private String[] assignFiles() {
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
        System.out.println("**** Assigned File Names ****");
        for (int i = 0; i < 5; i++) {
            int randIndex = random.nextInt(fileList.length-1);

            if (!Arrays.asList(subFileList).contains(fileList[randIndex])) {
                subFileList[i] = fileList[randIndex];
                System.out.println(subFileList[i]);
            } else {
                i--;
            }
        }
        System.out.println("*****************************\n");

        return subFileList;
    }

    private void startListeningForSearchQueries() {
        String fileName;
        Scanner in = new Scanner(System.in);

        while (true) {
            System.out.println("Enter a file name as the search string");
            fileName = in.nextLine();
            System.out.println("File Name: " + fileName);

            this.sendSearchQuery(fileName, "");
        }
    }

    public void sendSearchQuery(String fileName, String searchQuery) {
        DatagramSocket clientSocket = null;

        try {
            for (Peer peer : this.routingTable) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();
                byte[] receiveData = new byte[1024];

                String message = "";

                if (searchQuery == "") {
                    message = this.prependTheLengthToMessage("SER " + this.nodeIp + " " + this.nodePort + " \"" + fileName + "\"");
                } else {
                    message = searchQuery;
                }

                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                clientSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                clientSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();

                log(INFO, responseMessage);
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
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
                clientSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                clientSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log(INFO, responseMessage);

                if (responseMessage.contains("JOINOK 0")) {
                    this.routingTable.add(peer);
                } else {
                    log(ERROR, "Error in connecting to the peer");
                }

                //TODO: update file list
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
        }
    }

    public String prependTheLengthToMessage(String message) {
        return String.format("%04d", message.length() + 5) + " " + message;
    }

    private List<Peer> connectToBootstrapNode(String bootstrapIp,  int bootstrapPort, String nodeIp, int nodePort, String nodeName) {
        List<Peer> peers = new ArrayList<>();
        DatagramSocket clientSocket = null;

        try {
            InetAddress bootstrapHost = InetAddress.getByName(bootstrapIp);
            clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String message = this.prependTheLengthToMessage("REG " + nodeIp + " " + nodePort + " " + nodeName);
            byte[] sendData = message.getBytes();

            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, bootstrapHost, bootstrapPort);
            clientSocket.send(sendPacket);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);

            String responseMessage = new String(receivePacket.getData()).trim();
            log(INFO, "Bootstrap server : " + responseMessage);

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

    public void log(String level, Object msg) {
        System.out.println(level + " : " + msg.toString());
    }
}

class NodeThread extends Thread {

    Node node;

    NodeThread(Node node) { this.node = node;}

    private String searchInFileList(String fileName) {

        String[] fileList = node.getFileList();

        /*String matchingFileNames = "";

        for (int i = 0; i < 5; i++) {
            log(Node.INFO, fileList[i]);
            if (fileName == fileList[i]) {
                matchingFileNames = ""
            }

        }*/

        if (Arrays.asList(fileList).contains(fileName)) {
            return fileName;
        } else {
            return "FALSE";
        }
    }

    private String getFileNameFromSearchQuery(String query) {
        String[] response = query.split(" ");

        String filename = response[4];
        for (int i = 5; i <= response.length-1; i++) {
            filename += " " + response[i];
        }

        return filename.replace("\"", "");
    }

    public void run() {
        DatagramSocket serverSocket;
        try {
            serverSocket = new DatagramSocket(node.getNodePort());
            node.log(Node.INFO, "Started listening on '" + node.getNodePort() + "' for incoming data...");

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
                    node.log(Node.INFO, "SEARCH QUERY RECEIVED : " + incomingMessage);

                    String searchFilename = this.getFileNameFromSearchQuery(incomingMessage);

                    String fileSearchResults = searchInFileList(searchFilename);
                    String responseString = "";

                    if (fileSearchResults == "FALSE") {
                        if (node.getRoutingTable().size() > 0) {
                            node.sendSearchQuery(searchFilename, incomingMessage);
                        }
                    } else {
                        responseAddress = InetAddress.getByName(response[2]);
                        responsePort = Integer.parseInt(response[3]);

                        responseString =  node.prependTheLengthToMessage("SEROK " + node.getNodeIp() + " " + node.getNodePort() + " " + fileSearchResults);
                    }
                    sendData  = responseString.getBytes();

                } else if (response.length >= 4 && Node.JOIN.equals(response[1])) {
                    node.log(Node.INFO, "JOIN QUERY RECEIVED : " + incomingMessage);
                    sendData = node.prependTheLengthToMessage("JOINOK 0").getBytes();
                } else if (response.length >= 4 && Node.SEROK.equals(response[1])) {
                    node.log(Node.INFO, "SEARCH RESULTS RECEIVED : " + incomingMessage);
                }

                if (sendData != null) {
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, responseAddress,
                            responsePort);
                    serverSocket.send(sendPacket);
                }
            }
        } catch (IOException e) {
            node.log(Node.ERROR, e);
            e.printStackTrace();;
        }
    }
}
