package main.java.project;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.NetworkInterface;
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

import static main.java.project.Node.INFO;
import static main.java.project.Node.DEBUG;
import static main.java.project.Node.getFileNameFromSearchQuery;
import static main.java.project.Node.splitIncomingMessage;

public class Node {

    public static void main(String[] args) throws UnknownHostException {

        String bootstrapIp = "localhost";

        int bootstrapPort = 55555;

        String bootstrapAddress = System.getProperty("bootstrapAddress");

        if (checkForNotEmpty(bootstrapAddress)) {
            String[] values = bootstrapAddress.split(":");
            bootstrapIp = values[0];
            bootstrapPort = Integer.parseInt(values[1]);
        }

        String nodeIp = getLocalNodeIp();

        Random random = new Random();

        int randomNumber = random.nextInt(1000);

        int nodePort = 10000 + randomNumber;

        String nodeIpAddress = System.getProperty("nodeAddress");
        if (checkForNotEmpty(nodeIpAddress)) {
            String[] values = nodeIpAddress.split(":");
            nodeIp = values[0];
            nodePort = Integer.parseInt(values[1]);
        }

        String nodeName = "node" + nodePort;

        String nodeNameSystemProp = System.getProperty("nodeName");
        if (checkForNotEmpty(nodeNameSystemProp)) {
            nodeName = nodeNameSystemProp;
        }

        int hopsCount = default_hops_count;

        String hopsCountSystemProp = System.getProperty("hopsCount");
        if (checkForNotEmpty(hopsCountSystemProp)) {
            hopsCount = Integer.parseInt(hopsCountSystemProp);
        }

        Node node = new Node(bootstrapIp, bootstrapPort, nodeName, nodeIp, nodePort, hopsCount);

        log(INFO, "This node : " + nodeIp + ":" + nodePort);

        //1. assign own file list
        node.assignFiles();

        //2. connect to bootstrap and get peers
        List<Peer> peersToConnect = node.connectToBootstrapNode();

        log(INFO, "Peers : " + peersToConnect);

        //3. connect to peers from above
        node.connectToPeers(peersToConnect, node.nodeIp, node.nodePort);

        //4. start listening
        //startListening(port);
        (new NodeThread(node)).start();

        //5. start listening to incoming search queries
        node.startListeningForQueries();
        System.exit(0);
    }

    public static final String REG_OK = "REGOK";
    public static final String INFO = "INFO";
    public static final String INDENT = "\t";
    public static final String WARN = "WARN";
    public static final String ERROR = "ERROR";
    public static final String DEBUG = "DEBUG";
    public static final String SER = "SER";
    public static final String JOIN = "JOIN";
    public static final String SEROK = "SEROK";
    public static final String LEAVE = "LEAVE";
    public static final String RANK = "RANK";
    public static final String COM = "COM";
    public static final String COMRPLY = "COMRPLY";
    public static final String VERIFIED = "VERIFIED";
    public static final String UNVERIFIED = "UNVERIFIED";


    public static final int default_hops_count = 4;

    private List<Peer> routingTable = new ArrayList<>();
    private List<Forum> forumList = new ArrayList<>();
    private String[] fileList;
    private String bootstrapIp;
    private int bootstrapPort;
    private String nodeName;
    private String nodeIp;
    private int nodePort;
    private int hopsCount;

    private Map<String, Long> receivedSearchQueryMap = new ConcurrentHashMap<>();

    private Map<String, Integer> sentSearchQueryMap = new ConcurrentHashMap<>();

    private Map<String, Double> rankingMap = new ConcurrentHashMap<>();

    private Map<String, Long> receivedRankMessageMap = new ConcurrentHashMap<>();

    private Map<String, Long> receivedForumMessageMap = new ConcurrentHashMap<>();

    private Map<String, Long> receivedForumReplyMessageMap = new ConcurrentHashMap<>();

    public Node(String bootstrapIp, int bootstrapPort, String nodeName, String nodeIp, int nodePort, int hopsCount) {
        this.bootstrapIp = bootstrapIp;
        this.bootstrapPort = bootstrapPort;
        this.nodeName = nodeName;
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
        this.hopsCount = hopsCount;
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

    public int getHopsCount() {
        return hopsCount;
    }

    public List<Forum> getForumList() {
        return forumList;
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

    private static String getLocalNodeIp() {
        String address = "localhost";
        try {

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
        } catch (Exception e) {
            log(ERROR, "Cannot get local ip address - " + e);
            e.printStackTrace();
        }
        return address;
    }

    private static boolean isIP(String hostAddress) {
        return hostAddress.split("[.]").length == 4;
    }

    private static boolean checkForNotEmpty(String input) {
        return (input != null && !input.isEmpty());
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

    private void startListeningForQueries() {
        String query;
        Scanner in = new Scanner(System.in);

        while (true) {
            log(INFO, "Enter a query : ");
            query = in.nextLine();

            if (LEAVE.toLowerCase().equals(query.toLowerCase())) {
                sendLeaveMessageToPeers();
                sendUnRegMessageToBootstrap();
                break;
            }

            log(DEBUG, "Query : " + query);

            //RANK "Kung fu panda" 4
            if (query.startsWith(RANK)) {
                String[] messages = splitIncomingMessage(query);
                String fileToRank = getFileNameFromSearchQuery(messages[1]);
                List<String> fileSearchResultsList = searchInCurrentFileList(fileToRank);

                if (fileSearchResultsList.size() > 0) {
                    log(INFO, "FOUND: Ranking file found in current node '" + nodeIp + ":" + nodePort + "' as '" +
                            fileSearchResultsList + "'");
                    continue;
                }
                String rankMessage = prependLengthToMessage("RANK " + nodeIp + ":" + nodePort + " " + messages[1] +
                        " " + messages[2]);
                addToReceivedRankMessageMap(rankMessage, System.currentTimeMillis());
                sendRankingMessageToPeers(rankMessage);
                continue;
            } else if (query.startsWith(COM + " ")) {
                String[] messages = splitIncomingMessage(query);
                Long currentTime = System.currentTimeMillis();
                Forum forum = addToCurrentForumList(messages[1], currentTime.toString() , nodeIp + ":" + nodePort);

                String forumCreationMessage = prependLengthToMessage("COM " + nodeIp + ":" + nodePort + " " + forum.getCommentTime() +
                        " " + forum.getComment());

                addToReceivedForumMessageMap(forumCreationMessage, System.currentTimeMillis());
                sendForumInitiationMessageToPeers(forumCreationMessage);
                printForumList();
                continue;
            } else if (query.startsWith(COMRPLY + " ")) {
                String[] messages = splitIncomingMessage(query);
                Long currentTime = System.currentTimeMillis();
                Forum forum = addForumReplyToCurrentForumList(messages[3], nodeIp + ":" + nodePort, currentTime.toString() , messages[1], messages[2], true);

                String forumReplyMessage = "COMRPLY " + messages[1] + " " + messages[2] + " " + nodeIp + ":" + nodePort + " " + currentTime.toString() +
                        " " + messages[3];

                if (forum != null) {
                    forumReplyMessage = prependLengthToMessage(forumReplyMessage + " " + VERIFIED);
                } else {
                    forumReplyMessage = prependLengthToMessage(forumReplyMessage + " " + UNVERIFIED);
                }

                log(INFO, "Forum reply mesage : " + forumReplyMessage);
                addToReceivedForumReplyMessageMap(forumReplyMessage, System.currentTimeMillis());
                sendForumReplyMessageToPeers(forumReplyMessage);
                printForumList();
                continue;
            }

            //search its own list first
            List<String> searchedFiles = searchInCurrentFileList(query);

            if (!searchedFiles.isEmpty()) {
                log(INFO, "FOUND: Searched file found in current node '" + nodeIp + ":" + nodePort + "' as '" +
                        searchedFiles + "'");
            } else {
                String searchQuery = constructSearchQuery(nodeIp, nodePort, query, hopsCount);
                addToSentSearchQueryMap(query.toLowerCase(), hopsCount);
                sendSearchQuery(searchQuery);
            }
        }
    }

    public String constructSearchQuery(String ip, int port, String fileName, int hopsCount) {
        return prependLengthToMessage("SER " + ip + " " + port + " \"" + fileName + "\" " + hopsCount);
    }

    private void sendUnRegMessageToBootstrap() {
        DatagramSocket clientSocket = null;
        try {
            InetAddress bootstrapHost = InetAddress.getByName(bootstrapIp);
            clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String message = this.prependLengthToMessage("UNREG " + nodeIp + " " + nodePort + " " + nodeName);
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
                String message = this.prependLengthToMessage("LEAVE " + nodeIp + " " + nodePort);
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

    public void sendRankingMessageToPeers(String rankMessage) {
        DatagramSocket clientSocket = null;
        try {
            for (Peer peer : routingTable) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();

                byte[] sendData = rankMessage.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log(INFO, "SEND: Sending rank query '" + rankMessage + "' to '" + peer + "'");
                clientSocket.send(sendPacket);
            }
        } catch (Exception e) {
            log(ERROR, e);
            e.printStackTrace();
        } finally {
            if (clientSocket != null) {
                clientSocket.close();
            }
        }
    }

    public void sendForumInitiationMessageToPeers(String forumCreationMessage) {
        DatagramSocket clientSocket = null;
        try {
            for (Peer peer : routingTable) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();

                byte[] sendData = forumCreationMessage.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log(INFO, "SEND: Sending forum distribution query '" + forumCreationMessage + "' to '" + peer + "'");
                clientSocket.send(sendPacket);
            }
        } catch (Exception e) {
            log(ERROR, e);
            e.printStackTrace();
        } finally {
            if (clientSocket != null) {
                clientSocket.close();
            }
        }
    }

    public void sendForumReplyMessageToPeers(String forumReplyMessage) {
        DatagramSocket clientSocket = null;
        try {
            for (Peer peer : routingTable) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();

                byte[] sendData = forumReplyMessage.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                log(INFO, "SEND: Sending forum rely query '" + forumReplyMessage + "' to '" + peer + "'");
                clientSocket.send(sendPacket);
            }
        } catch (Exception e) {
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
                String message = this.prependLengthToMessage("JOIN " + nodeIp + " " + nodePort);
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

    public String prependLengthToMessage(String message) {
        return String.format("%04d", message.length() + 5) + " " + message;
    }

    private List<Peer> connectToBootstrapNode() {
        List<Peer> peers = new ArrayList<>();
        DatagramSocket clientSocket = null;

        try {
            InetAddress bootstrapHost = InetAddress.getByName(bootstrapIp);
            System.out.println(bootstrapHost);
            clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String message = prependLengthToMessage("REG " + nodeIp + " " + nodePort + " " + nodeName);
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
        if (level != null) {
            System.out.println(level + " : " + msg.toString());
        } else {
            System.out.println(msg.toString());
        }

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

    public Forum getMatchingForumFromTheList(String ownerIp, String commentTime) {
        Forum forum = null;
        for (Forum temforum: getForumList()) {
            if (temforum.getCommentTime().equals(commentTime.toString())  &&
                    temforum.getOwnerIp().equals(ownerIp.toString())
                    ) {
                forum = temforum;
            }
        }
        return forum;
    }

    public Forum addToCurrentForumList(String comment, String commentTime, String ownerIp) {
        Forum forum = getMatchingForumFromTheList(commentTime, ownerIp);

        if (forum == null) {
            forum = new Forum(comment, commentTime, ownerIp);
            this.forumList.add(forum);
        }
        return forum;
    }

    public void updateCurrentForumList(Forum forumToUpdate) {
        Boolean isForumUpdated = false;
        Forum forum = null;
        for (Forum temforum: getForumList()) {
            if (temforum.getCommentTime() == forumToUpdate.getCommentTime() &&
                    temforum.getOwnerIp() == forumToUpdate.getOwnerIp()
                    ) {
                forum = temforum;
                break;
            }
        }

        if (forum != null) {
            int index = this.forumList.indexOf(forum);
            this.forumList.remove(index);
            this.forumList.add(index, forumToUpdate);
            isForumUpdated = true;
        }

        if (!isForumUpdated) {
            log(INFO, "No matching forum to update");
        }
    }

    public void printForumList() {
        log(null, "");
        log(INFO, "Forum List");
        for (Forum temforum: getForumList()) {
            log(null, temforum.getComment() + " " + temforum.getCommentTime() + " " + temforum.getOwnerIp());
            for (ForumReply forumReply: temforum.getForumReply()) {
                log(INDENT, forumReply.getComment() + " " + forumReply.getCommentTime() + " " + forumReply.getOwnerIp());
            }
        }
        log(null, "");
    }

    public Forum addForumReplyToCurrentForumList(String reply, String replyOwnerIp, String replyTime, String forumOwnerIp, String forumTime, Boolean addIfOnlyForumOriginator) {

        Forum forum = null;
        if (addIfOnlyForumOriginator) {
            if (forumOwnerIp.equals((nodeIp + ":" + nodePort).toString())) {
                log(INFO, "Reply to forum initiated by same node : " + nodeIp + ":" + nodePort + " => " +forumOwnerIp);
                forum = getMatchingForumFromTheList(forumOwnerIp, forumTime);
                ForumReply forumReply = new ForumReply(reply, replyTime, replyOwnerIp);
                forum.addForumReply(forumReply);
                log(INFO,"Matching forum found: " + forum.getComment());
                updateCurrentForumList(forum);
            }
        } else {
            log(INFO, "Initiated by another form:" + nodeIp + ":" + nodePort + " => " +forumOwnerIp);
            forum = getMatchingForumFromTheList(forumOwnerIp, forumTime);
            ForumReply forumReply = new ForumReply(reply, replyTime, replyOwnerIp);
            forum.addForumReply(forumReply);
            updateCurrentForumList(forum);
        }


        return forum;
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
        sentSearchQueryMap.put(query, hops);
    }

    public double rankFile(String incomingMessage, String fileToRank, String ranker, int suggestedRank) {
        double newRank = suggestedRank;

        String rankerWithFileName = ranker + "-" + fileToRank;
        //check for duplicates
        if (receivedRankMessageMap.containsKey(rankerWithFileName)) {
            Node.log(DEBUG, "DROP: Rank message already received from '"+ranker+"' hence dropping '" +
                    incomingMessage + "'");
            newRank = rankingMap.get(fileToRank);
        } else {
            receivedRankMessageMap.put(rankerWithFileName, System.currentTimeMillis());
            if (rankingMap.containsKey(fileToRank)) {
                newRank = 0.5 * (rankingMap.get(fileToRank) + suggestedRank);
                Node.log(INFO, "UPDATE-RANK: '" + fileToRank + "' - '" + newRank + "'");
            }
        }
        rankingMap.put(fileToRank, newRank);
        return newRank;
    }

    public Map<String, Long> getReceivedRankMessageMap() {
        return receivedRankMessageMap;
    }


    public void addToReceivedRankMessageMap(String message, Long timestamp) {
        receivedRankMessageMap.put(message, timestamp);
    }

    public void addToReceivedForumMessageMap(String message, Long timestamp) {
        receivedForumMessageMap.put(message, timestamp);
    }

    public void addToReceivedForumReplyMessageMap(String message, Long timestamp) {
        receivedForumReplyMessageMap.put(message, timestamp);
    }

    public Map<String, Long> getReceivedForumMessageMap() {
        return receivedForumMessageMap;
    }

    public Map<String, Long> getReceivedForumReplyMessageMap() {
        return receivedForumReplyMessageMap;
    }


    public void removeFromReceivedRankMessageMap(String message) {
        receivedRankMessageMap.remove(message);
    }

    public void removeFromReceivedForumMessageMap(String message) {
        receivedForumMessageMap.remove(message);
    }

    public void removeFromReceivedForumReplyMessageMap(String message) {
        receivedForumReplyMessageMap.remove(message);
    }

    public static String[] splitIncomingMessage(String incomingMessage) {
        List<String> list = new ArrayList<>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(incomingMessage);
        while (m.find()) {
            list.add(m.group(1));
        }
        return list.toArray(new String[list.size()]);
    }

    public static String getFileNameFromSearchQuery(String query) {
        return query.trim().replaceAll("\"", "");
    }
}

class NodeThread extends Thread {

    private Node node;

    NodeThread(Node node) {
        this.node = node;
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

                    String searchFilename = getFileNameFromSearchQuery(response[4]);

                    List<String> fileSearchResultsList = node.searchInCurrentFileList(searchFilename);
                    String responseString = "";

                    if (fileSearchResultsList.isEmpty()) {
                        if (node.getRoutingTable().size() > 0) {
                            String searchQueryWithoutHops = response[2] + ":" + response[3] + ":" + searchFilename;

                            if (node.getReceivedSearchQueryMap().keySet().contains(searchQueryWithoutHops)) {
                                long timeInterval = System.currentTimeMillis() -
                                        node.getReceivedSearchQueryMap().get(searchQueryWithoutHops);
                                if (timeInterval < 2000) {
                                    Node.log(DEBUG, "DROP: Query already received within " + (timeInterval / 1000) +
                                            " sec, hence dropping '" + incomingMessage + "'");
                                    continue;
                                }
                                node.removeFromReceivedQueryMap(searchQueryWithoutHops);
                            }

                            int currentHopsCount = Integer.parseInt(response[5]);
                            Node.log(DEBUG, "Current hops count for '" + incomingMessage + "' is : " +
                                    currentHopsCount);
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
                        String fileSearchResults = joinSearchResults(fileSearchResultsList);
                        responseString = node.prependLengthToMessage("SEROK " + fileSearchResultsList.size() + " " +
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
                    sendData = node.prependLengthToMessage("JOINOK 0").getBytes();
                } else if (response.length >= 4 && Node.LEAVE.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Leave query received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");
                    node.removeFromRoutingTable(new Peer(responseAddress.getHostAddress(),
                            Integer.parseInt(response[3])));
                    sendData = node.prependLengthToMessage("LEAVEOK 0").getBytes();
                } else if (response.length >= 4 && Node.SEROK.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Search results received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");
//                    message - 0041 SEROK 192.168.1.2 11003 Windows XP 2
//                    message - 0066 SEROK 2 10.100.1.124 11001 "American Pickers American Idol" 2
                    int currentHopCount = Integer.parseInt(response[6]);
                    checkForBestResult(node, incomingMessage, response[5], currentHopCount);
                } else if (response.length >= 4 && Node.RANK.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Rank query received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");

                    if (node.getReceivedRankMessageMap().containsKey(incomingMessage)) {
                        long timeInterval = System.currentTimeMillis() -
                                node.getReceivedRankMessageMap().get(incomingMessage);

                        if (timeInterval < 2000) {
                            Node.log(DEBUG, "DROP: DUPLICATE: Rank message already sent " + (0.005 * timeInterval) +
                                    " sec before, hence dropping '" + incomingMessage + "'");
                            continue;
                        }
                        node.removeFromReceivedRankMessageMap(incomingMessage);
                    }

                    //0018 RANK 129.82.123.45 "Kung fu panda" 4
                    String fileToRank = getFileNameFromSearchQuery(response[3]);
                    List<String> fileSearchResultsList = node.searchInCurrentFileList(fileToRank);

                    if (fileSearchResultsList.size() > 0) {
                        int suggestedRank = Integer.parseInt(response[4]);
                        String ranker = response[2];
                        double newRanking = node.rankFile(incomingMessage, fileToRank, ranker, suggestedRank);
                        Node.log(INFO, "RANK : '" + fileToRank + "' - '" + newRanking + "'");
                    } else {
                        node.addToReceivedRankMessageMap(incomingMessage, System.currentTimeMillis());
                        node.sendRankingMessageToPeers(incomingMessage);
                    }
                } else if (response.length >= 4 && Node.COM.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Forum query received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");

                    if (node.getReceivedForumMessageMap().containsKey(incomingMessage)) {
                        long timeInterval = System.currentTimeMillis() - node.getReceivedForumMessageMap().get(incomingMessage);

                        if (timeInterval < 2000) {
                            Node.log(DEBUG, "DROP: DUPLICATE: COM message already sent " + (0.005 * timeInterval) +
                                    " sec before, hence dropping '" + incomingMessage + "'");
                            continue;
                        }
                        node.removeFromReceivedForumMessageMap(incomingMessage);
                    }

                    //0018 RANK 129.82.123.45 "Kung fu panda" 4

                    /*String fileToRank = getFileNameFromSearchQuery(response[3]);
                    List<String> fileSearchResultsList = node.searchInCurrentFileList(fileToRank);
*/
                    /*if (fileSearchResultsList.size() > 0) {
                        int suggestedRank = Integer.parseInt(response[4]);
                        String ranker = response[2];
                        double newRanking = node.rankFile(incomingMessage, fileToRank, ranker, suggestedRank);
                        Node.log(INFO, "RANK : '" + fileToRank + "' - '" + newRanking + "'");
                    } else {*/
                        //node.addToReceivedRankMessageMap(incomingMessage, System.currentTimeMillis());

                    //}
                    node.addToCurrentForumList(response[4], response[3], response[2]);
                    node.addToReceivedForumMessageMap(incomingMessage, System.currentTimeMillis());
                    node.sendForumInitiationMessageToPeers(incomingMessage);
                    node.printForumList();

                } else if (response.length >= 4 && Node.COMRPLY.equals(response[1])) {
                    Node.log(INFO, "RECEIVE: Forum reply received from '" + responseAddress + ":" + responsePort +
                            "' as '" + incomingMessage + "'");

                    if (node.getReceivedForumReplyMessageMap().containsKey(incomingMessage)) {
                        long timeInterval = System.currentTimeMillis() - node.getReceivedForumReplyMessageMap().get(incomingMessage);

                        if (timeInterval < 2000) {
                            Node.log(DEBUG, "DROP: DUPLICATE: COMRPLY message already sent " + (0.005 * timeInterval) +
                                    " sec before, hence dropping '" + incomingMessage + "'");
                            continue;
                        }
                        node.removeFromReceivedForumReplyMessageMap(incomingMessage);
                    }

                    Boolean addIfFromOriginator = null;
                    if (Node.VERIFIED.toString().equals(response[7].toString())) {
                        addIfFromOriginator = false;
                    } else {
                        addIfFromOriginator = true;
                    }

                    Forum forum = node.addForumReplyToCurrentForumList(response[6], response[4] , response[5] , response[2], response[3], addIfFromOriginator);

                    node.addToReceivedForumReplyMessageMap(incomingMessage, System.currentTimeMillis());

                    if (Node.UNVERIFIED.toString().equals(response[7].toString()) && forum != null) {
                        incomingMessage = incomingMessage.replace(Node.UNVERIFIED, Node.VERIFIED);
                    }

                    node.addToReceivedForumReplyMessageMap(incomingMessage, System.currentTimeMillis());
                    node.sendForumReplyMessageToPeers(incomingMessage);
                    node.printForumList();

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

    private String joinSearchResults(List<String> fileSearchResultsList) {
        StringBuilder sb = new StringBuilder();
        for (String s : fileSearchResultsList) {
            sb.append(s);
            sb.append(" ");
        }
        return sb.toString().trim();
    }

    private boolean checkForBestResult(Node node, String incomingMessage, String fileName, int hops) {
        int hopsCount = node.getHopsCount() - hops;
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

    String getIp() {
        return this.ip;
    }

    int getPort() {
        return this.port;
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
