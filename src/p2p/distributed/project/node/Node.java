package p2p.distributed.project.node;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.*;

public class Node {
    public static final String REG_OK = "REGOK";
    public static final String INFO = "INFO";
    public static final String WARN = "WARN";
    public static final String ERROR = "ERROR";
    public static final String DEBUG = "DEBUG";
    public static final String SER = "SER";
    public static final String JOIN = "JOIN";


    private List<Peer> routingTable = new ArrayList<>();
    private List<FileMetaData> fileList = new ArrayList<>();

    public static void main(String[] args) {
        String[] fileList = assignFiles();
        int port = 11003;
//        int port = Integer.parseInt(System.getProperty("port"));
        String username = "kicha3";
//        String username = System.getProperty("username");
//        String bootstrapNode = System.getProperty("bootstrap.address");

        //1. connect to bootstrap and get peers
        List<Peer> peers = connectToBootstrapNode("192.168.8.100:55555", port, username);

        log(INFO, "Peers : " + peers);

        //2. connect to peers from above
        connectToPeers(peers);

        //3. start listening
        //startListening(port);
        (new NodeThread(fileList, port)).start();

        //4. start listening to incoming search queries
        startListeningForSearchQueries(peers);
    }

    private static String[] assignFiles() {
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

    private static void startListeningForSearchQueries(List<Peer> peers) {
        String fileName;
        Scanner in = new Scanner(System.in);

        while (true) {
            System.out.println("Enter a file name as the search string");
            fileName = in.nextLine();
            System.out.println("File Name: " + fileName);

            sendSearchQuery(fileName, peers);
        }
    }

    private static void sendSearchQuery(String fileName, List<Peer> peers) {
        DatagramSocket clientSocket = null;

        try {
            for (Peer peer : peers) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();
                byte[] receiveData = new byte[1024];
                String sentence = " SER " + peer.getIp() + " " + peer.getPort() + " \"" + fileName + "\"";
                sentence = String.format("%04d", sentence.length() + 4) + sentence;

                byte[] sendData = sentence.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                clientSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                clientSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();

                log(INFO, responseMessage);

                //TODO: Receive the file list
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
        }
    }

    /*private static void startListening(int port) {
        DatagramSocket serverSocket;
        try {
            serverSocket = new DatagramSocket(port);
            log(INFO, "Started listening on '" + port + "' for incoming data...");

            while (true) {
                byte[] buffer = new byte[65536];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                serverSocket.receive(incoming);

                byte[] data = incoming.getData();
                String incomingMessage = new String(data, 0, incoming.getLength());
                log(INFO, "Received : " + incomingMessage);
                byte[] sendData = "0014 JOINOK 0".getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, incoming.getAddress(),
                        incoming.getPort());
                serverSocket.send(sendPacket);
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();;
        }
    }*/

    private static void connectToPeers(List<Peer> peers) {
        DatagramSocket clientSocket = null;
        try {
            for (Peer peer : peers) {
                InetAddress address = InetAddress.getByName(peer.getIp());
                clientSocket = new DatagramSocket();
                byte[] receiveData = new byte[1024];
                String sentence = "0027 JOIN " + peer.getIp() + " " + peer.getPort();
                byte[] sendData = sentence.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, peer.getPort());
                clientSocket.send(sendPacket);
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                clientSocket.receive(receivePacket);
                String responseMessage = new String(receivePacket.getData()).trim();
                log(INFO, responseMessage);

                //TODO: update file list
            }
        } catch (IOException e) {
            log(ERROR, e);
            e.printStackTrace();
        }
    }

    private static List<Peer> connectToBootstrapNode(String bootstrapAddress, int myPort, String username) {
        List<Peer> peers = new ArrayList<>();
        DatagramSocket clientSocket = null;
        try {
            String[] address = bootstrapAddress.split(":");
            InetAddress bootstrapHost = InetAddress.getByName(address[0]);
            int bootstrapPort = Integer.parseInt(address[1]);
            clientSocket = new DatagramSocket();
            byte[] receiveData = new byte[1024];
            String sentence = "0033 REG " + address[0] + " " + myPort + " " + username;
            byte[] sendData = sentence.getBytes();
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


    private static void log(String level, Object msg) {
        System.out.println(level + " : " + msg.toString());
    }
}

class NodeThread extends Thread {

    String[] fileList;
    int port;
    NodeThread(String[] fileList, int port) { this.fileList = fileList; this.port = port; }

    public static String searchInFileList(String[] fileList, String fileName) {
        for (int i = 0; i < 5; i++) {
            log(Node.INFO, fileList[i]);
        }
        log(Node.INFO, fileName);
        if (Arrays.asList(fileList).contains(fileName)) {
            return "File Exists";
        } else {
            return "File Not found";
        }
    }

    public void run() {
        DatagramSocket serverSocket;
        try {
            serverSocket = new DatagramSocket(port);
            log(Node.INFO, "Started listening on '" + port + "' for incoming data...");

            while (true) {
                byte[] buffer = new byte[65536];
                DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
                serverSocket.receive(incoming);

                byte[] data = incoming.getData();
                String incomingMessage = new String(data, 0, incoming.getLength());

                String[] response = incomingMessage.split(" ");
                byte[] sendData = null;
                if (response.length >= 5 && Node.SER.equals(response[1])) {
                    log(Node.INFO, "SEARCH QUERY RECEIVED : " + incomingMessage);

                    String filename = response[4];
                    for (int i = 5; i <= response.length-1; i++) {
                        filename += " " + response[i];
                    }

                    filename = filename.replace("\"", "");
                    sendData  = ("SEARCH QUERY RESULT: " + searchInFileList(fileList, filename)).getBytes();
                } else if (response.length >= 4 && Node.JOIN.equals(response[1])) {
                    log(Node.INFO, "JOIN QUERY RECEIVED : " + incomingMessage);
                    sendData = "0014 JOINOK 0".getBytes();
                }

                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, incoming.getAddress(),
                        incoming.getPort());
                serverSocket.send(sendPacket);
            }
        } catch (IOException e) {
            log(Node.ERROR, e);
            e.printStackTrace();;
        }
    }

    private static void log(String level, Object msg) {
        System.out.println(level + " : " + msg.toString());
    }

}
