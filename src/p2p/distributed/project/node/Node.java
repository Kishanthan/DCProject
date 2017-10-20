package p2p.distributed.project.node;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class Node {
    public static final String REG_OK = "REGOK";
    public static final String INFO = "INFO";
    public static final String WARN = "WARN";
    public static final String ERROR = "ERROR";
    public static final String DEBUG = "DEBUG";

    private List<Peer> routingTable = new ArrayList<>();
    private List<FileMetaData> fileList = new ArrayList<>();

    public static void main(String[] args) {
        int port = 57005;
//        int port = Integer.parseInt(System.getProperty("port"));
        String username = "kicha5";
//        String username = System.getProperty("username");
//        String bootstrapNode = System.getProperty("bootstrap.address");

        //1. connect to bootstrap and get peers
        List<Peer> peers = connectToBootstrapNode("10.100.1.124:55555", port, username);

        log(INFO, "Peers : " + peers);

        //2. connect to peers from above
        connectToPeers(peers);

        //3. start listening
        startListening(port);
    }

    private static void startListening(int port) {
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
    }

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
