package project;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static project.Node.INFO;
import static project.Node.DEBUG;
import static project.Node.default_hops_count;

public class Node {

    public static void main(String[] args) throws UnknownHostException {

        String bootstrapIp = "localhost";
        int bootstrapPort = 55555;

        String nodeIp = getNodeIpAddress();

        Random random = new Random();

        int randomNumber = random.nextInt(1000);

        int nodePort = 10000 + randomNumber;
        String nodeName = "node" + randomNumber;

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
        System.exit(0);
    }


    public static final String REG_OK = "REGOK";
    public static final String UN_ROK = "UNROK";
    public static final String INFO = "INFO";
    public static final String WARN = "WARN";
    public static final String ERROR = "ERROR";
    public static final String DEBUG = "DEBUG";
    public static final String SER = "SER";
    public static final String JOIN = "JOIN";
    public static final String SEROK = "SEROK";
    public static final String LEAVE = "LEAVE";

    public static final int default_hops_count = 2;


    private List<Peer> routingTable = new ArrayList<>();
    private String[] fileList;

    private String bootstrapIp;
    private int bootstrapPort;
    private String nodeName;
    private String nodeIp;
    private int nodePort;
    private Map<String, Long> receivedSearchQueryMap = new ConcurrentHashMap<>();

    private Map<String, Integer> sentSearchQueryMap = new ConcurrentHashMap<>();

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
        if (!routingTable.contains(peer)) {
            routingTable.add(peer);
        }
        log(INFO, "UPDATE: Routing table : " + routingTable);
    }

    public void removeFromRoutingTable(Peer peer) {
        routingTable.remove(peer);
        log(INFO, "UPDATE: Routing table : " + routingTable);
    }

    private static String getNodeIpAddress() {
        String address = "127.0.0.1";
        try {
//            DatagramSocket socket = new DatagramSocket();
//            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
//            nodeIp = socket.getLocalAddress().getHostAddress();

            Enumeration networkInterfaces = NetworkInterface.getNetworkInterfaces();

            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface netface = (NetworkInterface) networkInterfaces.nextElement();
                Enumeration addresses = netface.getInetAddresses();

                while (addresses.hasMoreElements()) {
                    InetAddress ip = (InetAddress) addresses.nextElement();
                    if (!ip.isLoopbackAddress() && isIP(ip.getHostAddress())) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            log(ERROR, "Cannot get local ip address - " + e);
            e.printStackTrace();
        }
        return address;
    }

    private static boolean isIP(String hostAddress) {
        return hostAddress.split("[.]").length == 4;
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

            if (LEAVE.toLowerCase().equals(fileName.toLowerCase())) {
                sendLeaveMessageToPeers();
                sendUnRegMessageToBootstrap();
                break;
            }

            log(DEBUG, "File name : " + fileName);
            //search its own list first
            List<String> searchedFiles = searchInCurrentFileList(fileName);

            if (!searchedFiles.isEmpty()) {
                log(INFO, "FOUND: Searched file found in current node '" +
                        nodeIp + ":" + nodePort + "' as '" + searchedFiles + "'");
            } else {
                String searchQuery = constructSearchQuery(nodeIp, nodePort, fileName, default_hops_count);
                addToSentSearchQueryMap(fileName.toLowerCase(), default_hops_count);
                sendSearchQuery(searchQuery);
            }
        }
    }

    public String constructSearchQuery(String ip, int port, String fileName, int hopsCount) {
        return prependTheLengthToMessage("SER " + ip + " " + port + " \"" + fileName + "\" " + hopsCount);
    }

    private void sendUnRegMessageToBootstrap() {
        DatagramSocket clientSocket = null;
        try {
            InetAddress bootstrapHost = InetAddress.getByName(bootstrapIp);
            clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String message = this.prependTheLengthToMessage("UNREG " + nodeIp + " " + nodePort + " " + nodeName);
            byte[] sendData = message.getBytes();

            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, bootstrapHost, bootstrapPort);
            log(INFO, "SEND: Bootstrap server un-register message '" + message + "'");
            clientSocket.send(sendPacket);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);

            String responseMessage = new String(receivePacket.getData()).trim();
            log(INFO, "RECEIVE: Bootstrap server response '" + responseMessage + "'");

            //successful reply - 0012 UNROK 0
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
        } finally {
            if (clientSocket != null) {
                clientSocket.close();
            }
        }
    }

    private void sendLeaveMessageToPeers() {
        DatagramSocket clientSocket = null;
        try {
            for (Peer peer : routingTable) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();
                byte[] receiveData = new byte[1024];
                String message = this.prependTheLengthToMessage("LEAVE " + nodeIp + " " + nodePort);
                byte[] sendData = message.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log(INFO, "SEND: Leave message to '" + peer + "'");
                clientSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                clientSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log(INFO, "RECEIVE: " + responseMessage + " from '" + peer + "'");
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

    private String getSenderAddressFromSearchQuery(String searchQuery) {
        String[] query = searchQuery.split(" ");
        return query[2] + ":" + query[3];
    }

    public void sendSearchQuery(String searchQuery) {
        DatagramSocket clientSocket = null;

        try {
            for (Peer peer : routingTable) {
                String peerAddress = peer.getIp() + ":" + peer.getPort();
                if (peerAddress.equals(getSenderAddressFromSearchQuery(searchQuery))) {
                    continue;
                }
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();

                byte[] sendData = searchQuery.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log(INFO, "SEND: Sending search query '" + searchQuery + "' to '" + peer + "'");
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

    public void addToReceivedQueryMap(String searchQuery, long timestamp) {
        receivedSearchQueryMap.put(searchQuery, timestamp);
    }

    public void removeFromReceivedQueryMap(String searchQuery) {
        receivedSearchQueryMap.remove(searchQuery);
    }

    public Map<String, Long> getReceivedSearchQueryMap() {
        return receivedSearchQueryMap;
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
            log(INFO, "SEND: Bootstrap server register message '" + message + "'");
            clientSocket.send(sendPacket);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);

            String responseMessage = new String(receivePacket.getData()).trim();
            log(INFO, "RECEIVE: Bootstrap server response '" + responseMessage + "'");

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

    public List<String> searchInCurrentFileList(String fileName) {
        List<String> queriedFileList = new ArrayList<>();

        for (String file : fileList) {
            if (file.toLowerCase().matches(".*\\b" + fileName.toLowerCase() + "\\b.*")) {
                queriedFileList.add(file);
            }
        }
        return queriedFileList;
    }

    public Map<String, Integer> getSentSearchQueryMap() {
        return sentSearchQueryMap;
    }

    public void addToSentSearchQueryMap(String query, int hops) {
        sentSearchQueryMap.put(query, hops);
    }

    public void removeFromSentSearchQueryMap(String query) {
        sentSearchQueryMap.remove(query);
    }

    public void updateSentSearchQueryMap(String query, int hops) {
        sentSearchQueryMap.replace(query, hops);
    }
}

class NodeThread extends Thread {

    Node node;

    NodeThread(Node node) {
        this.node = node;
    }

    private String getFileNameFromSearchQuery(String query) {
//        String[] response = query.split(" ");
//
//        String filename = response[4];
//        for (int i = 5; i <= response.length - 1; i++) {
//            filename += " " + response[i];
//        }
//
//        return filename.replace("\"", "");
        return query.trim().replaceAll("\"", "");
    }

    public void run() {
        DatagramSocket serverSocket;
        try {
            serverSocket = new DatagramSocket(node.getNodePort());
            Node.log(INFO, "Started listening on '" + node.getNodePort() + "' for incoming data...");

            while (true) {
                byte[] buffer = new byte[65536];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                serverSocket.receive(incoming);

                byte[] data = incoming.getData();
                String incomingMessage = new String(data, 0, incoming.getLength());

                //incomingMessage = 0047 SER 129.82.62.142 5070 "Lord of the rings" 3

                String[] response = splitIncomingMessage(incomingMessage);
                byte[] sendData = null;

                InetAddress responseAddress = incoming.getAddress();
                int responsePort = incoming.getPort();

                if (response.length >= 6 && Node.SER.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Search query received from '" + responseAddress + ":" +
                            responsePort + "' as '" + incomingMessage + "'");

                    String searchFilename = this.getFileNameFromSearchQuery(response[4]);

                    List<String> fileSearchResultsList = node.searchInCurrentFileList(searchFilename);
                    String responseString = "";

                    if (fileSearchResultsList.isEmpty()) {
                        if (node.getRoutingTable().size() > 0) {
                            String searchQueryWithoutHops = response[2] + ":" + response[3] + ":" + searchFilename;

                            if (node.getReceivedSearchQueryMap().keySet().contains(searchQueryWithoutHops)) {
                                long timeInterval = System.currentTimeMillis() -
                                        node.getReceivedSearchQueryMap().get(searchQueryWithoutHops);
                                if (timeInterval < 2000) {
                                    Node.log(DEBUG, "DROP: Query already received within " + (timeInterval / 1000) + " sec, " +
                                            "hence dropping '" + incomingMessage + "'");
                                    continue;
                                }
                                node.removeFromReceivedQueryMap(searchQueryWithoutHops);
                            }

                            int currentHopsCount = Integer.parseInt(response[5]);
                            Node.log(DEBUG, "Current hops count for '" + incomingMessage + "' is : " + currentHopsCount);
                            if (currentHopsCount == 0) {
                                Node.log(DEBUG, "DROP: Maximum hops reached hence dropping '" + incomingMessage + "'");
                                continue;
                            }

                            String updatedSearchQuery = node.constructSearchQuery(response[2],
                                    Integer.parseInt(response[3]), searchFilename, currentHopsCount - 1);

                            if (!updatedSearchQuery.isEmpty()) {
                                node.addToReceivedQueryMap(searchQueryWithoutHops, System.currentTimeMillis());
                            }
                            node.sendSearchQuery(updatedSearchQuery);
                        }
                    } else {
                        String fileSearchResults = String.join(" ", fileSearchResultsList);
                        responseString = node.prependTheLengthToMessage("SEROK " + fileSearchResultsList.size() + " " +
                                node.getNodeIp() + " " + node.getNodePort() + " \"" + fileSearchResults + "\" " +
                                Integer.parseInt(response[5]));

                        Node.log(INFO, "FOUND: File found locally : " + responseString);

                        sendTheResultToOriginalNode(response[2], Integer.parseInt(response[3]), responseString);

                    }
                    sendData = responseString.getBytes();

                } else if (response.length >= 4 && Node.JOIN.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Join query received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");
                    node.addToRoutingTable(new Peer(responseAddress.getHostAddress(), Integer.parseInt(response[3])));
                    sendData = node.prependTheLengthToMessage("JOINOK 0").getBytes();
                } else if (response.length >= 4 && Node.LEAVE.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Leave query received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");
                    node.removeFromRoutingTable(new Peer(responseAddress.getHostAddress(), Integer.parseInt(response[3])));
                    sendData = node.prependTheLengthToMessage("LEAVEOK 0").getBytes();
                } else if (response.length >= 4 && Node.SEROK.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Search results received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");
//                    0041 SEROK 192.168.1.2 11003 Windows XP 2
//                    0066 SEROK 2 10.100.1.124 11001 "American Pickers American Idol" 2
                    int currentHopCount = Integer.parseInt(response[6]);
                    checkForBestResult(node, incomingMessage, response[5], currentHopCount);
                }

                if (sendData != null) {
                    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, responseAddress,
                            responsePort);
                    serverSocket.send(sendPacket);
                }
            }
        } catch (IOException e) {
            Node.log(Node.ERROR, e);
            e.printStackTrace();
        }
    }

    private String[] splitIncomingMessage(String incomingMessage) {
        List<String> list = new ArrayList<>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(incomingMessage);
        while (m.find()) {
            list.add(m.group(1));
        }
        return list.toArray(new String[list.size()]);
    }

    private boolean checkForBestResult(Node node, String incomingMessage, String fileName, int hops) {
        int hopsCount = default_hops_count - hops;
        for (String queriedName : node.getSentSearchQueryMap().keySet()) {
            if (fileName != null && !fileName.isEmpty() && fileName.toLowerCase().contains(queriedName)) {
                if (hopsCount == 0 || node.getSentSearchQueryMap().get(queriedName) > hopsCount) {
                    Node.log(INFO, "BEST RESULT: With less number of hops '" + hopsCount + "' received '" +
                            incomingMessage + "'");
                    node.updateSentSearchQueryMap(queriedName, hopsCount);
                    return true;
                } else {
                    Node.log(INFO, "IGNORE: Number of hops '" + hopsCount + "' exceeds or equal to the current " +
                            "best hops count '" + node.getSentSearchQueryMap().get(queriedName) + " for '" +
                            incomingMessage + "'");
                }
            }
        }
//        log(DEBUG, "The search result '" + incomingMessage + "' is not the best so far...");
        return false;
    }

    private void sendTheResultToOriginalNode(String responseIp, int responsePort, String message) {
        DatagramSocket serverSocket = null;
        try {
            serverSocket = new DatagramSocket();

            InetAddress responseAddress = InetAddress.getByName(responseIp);

            byte[] sendData = message.getBytes();

            Node.log(INFO, "SEND: Search results to originated node '" + responseIp + ":" + responsePort + "'");

            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, responseAddress, responsePort);
            serverSocket.send(sendPacket);
        } catch (IOException e) {
            Node.log(Node.ERROR, e);
            e.printStackTrace();
        } finally {
            if (serverSocket != null) {
                serverSocket.close();
            }
        }
    }
}

class Peer {
    private String ip = "localhost";
    private int port = -1;

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Peer) {
            Peer current = (Peer) obj;
            if (this.getIp().equals(current.getIp()) && this.getPort() == current.getPort()) {
                return true;
            }
        }
        return false;
    }

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
