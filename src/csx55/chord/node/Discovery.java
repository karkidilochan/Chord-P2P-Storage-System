package csx55.chord.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import csx55.chord.tcp.TCPConnection;
import csx55.chord.tcp.TCPServer;
import csx55.chord.wireformats.Event;
import csx55.chord.wireformats.Protocol;
import csx55.chord.wireformats.Register;
import csx55.chord.wireformats.RegisterResponse;
import csx55.chord.wireformats.SetupChord;

/**
 * The Registry class maintains information about messaging nodes and handles
 * various functionalities:
 * - Registering/Deregistering messaging nodes
 * - Constructing overlay by relaying routing messages between nodes
 * - Assigning weights to the links between nodes
 */
public class Discovery implements Node {
    // Constants representing different commands
    private static final String PEER_NODES = "peer-nodes";

    private Map<String, TCPConnection> connections = new HashMap<>();

    /**
     * The main method of the Registry application.
     * It initializes the Registry, starts a TCP server, and provides user
     * interaction through commands.
     *
     * @param args Command-line arguments. Expects the port number as the first
     *             argument.
     */
    public static void main(String[] args) {
        // Check if the port number is provided as a command-line argument
        if (args.length < 1) {
            System.out.println("Error starting the Registry. Usage: java csx55.overlay.node.Registry portnum");
        }

        Discovery registry = new Discovery();

        /*
         * defining serverSocket in try-with-resources statement ensures
         * that the serverSocket is closed after the block ends
         */
        try (ServerSocket serverSocket = new ServerSocket(Integer.valueOf(args[0]))) {
            /*
             * start the server thread after initializing the server socket
             * invoke start function to start a new thread execution(invoking run() is not
             * the right way)
             */
            (new Thread(new TCPServer(registry, serverSocket))).start();

            // Take commands from console
            registry.takeCommands();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /* Takes user commands from console */
    private void takeCommands() {
        System.out.println("This is the Registry command console. Please enter a valid command to start the overlay.");

        try (Scanner scan = new Scanner(System.in)) {
            while (true) {
                String line = scan.nextLine().toLowerCase();
                String[] input = line.split("\\s+");
                switch (input[0]) {
                    case PEER_NODES:
                        listPeerNodes();
                        break;

                    default:
                        System.out.println("Please enter a valid command! Options are:\n" +
                                " - peer-nodes\n");
                        break;
                }
            }
        }
    }

    /**
     * Handles incoming events received from TCP connections.
     *
     * @param event      The event to handle.
     * @param connection The TCP connection associated with the event.
     */
    public void handleIncomingEvent(Event event, TCPConnection connection) {
        switch (event.getType()) {
            case Protocol.REGISTER_REQUEST:
                handleRegistrationEvent((Register) event, connection);
                break;

            case Protocol.DEREGISTER_REQUEST:
                handleDeregistrationEvent((Register) event, connection);
                break;
        }
    }

    /**
     * Handles registration or deregistration events received from messaging nodes.
     * Synchronized to make sure this method runs in a single thread at a given
     * time.
     * 
     * @param registerEvent The registration event to handle.
     * @param connection    The TCP connection associated with the event.
     * @param register      A boolean indicating whether it's a registration or
     *                      deregistration request.
     */
    private synchronized void handleRegistrationEvent(Register registerEvent, TCPConnection connection) {
        // typecast event object to Register
        String nodes = registerEvent.getConnectionReadable();
        String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

        String message = checkRegistrationStatus(nodes, ipAddress, true);
        byte status;

        String randomKey = null;

        /* TODO: detect identifier collision */
        if (message.length() == 0 && validatePeerID(registerEvent)) {
            /* validate peer id */

            if (connections.size() == 0) {
                randomKey = nodes;
            } else {
                Random random = new Random();
                int randomIndex = random.nextInt(connections.keySet().size());

                // Retrieve the key at the random index
                List<String> keysList = new ArrayList<>(connections.keySet());
                randomKey = keysList.get(randomIndex);
            }

            connections.put(nodes, connection);

            message = "Registration request successful.  The number of messaging nodes currently "
                    + "constituting the overlay is (" + connections.size() + ").\n";
            status = Protocol.SUCCESS;

        } else {
            System.out.println("Unable to process request. Responding with a failure while peerID validation is "
                    + validatePeerID(registerEvent));
            status = Protocol.FAILURE;
        }
        RegisterResponse response = new RegisterResponse(status, message);
        try {
            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            connections.remove(nodes);
            e.printStackTrace();
        }

        /* send live peer info after sending register response message */

        if (status == Protocol.SUCCESS) {
            sendLivePeerInfo(connection, randomKey);
        }
    }

    private synchronized void handleDeregistrationEvent(Register registerEvent, TCPConnection connection) {
        // typecast event object to Register
        String nodes = registerEvent.getConnectionReadable();
        String ipAddress = connection.getSocket().getInetAddress().getHostAddress();

        String message = checkRegistrationStatus(nodes, ipAddress, false);
        byte status;
        if (message.length() == 0) {

            connections.remove(nodes);
            System.out.println("Deregistered " + nodes + ". There are now ("
                    + connections.size() + ") connections.\n");
            message = "Deregistration request successful.  The number of messaging nodes currently "
                    + "constituting the overlay is (" + connections.size() + ").\n";
            status = Protocol.SUCCESS;
        } else {
            System.out.println("Unable to process request. Responding with a failure.");
            status = Protocol.FAILURE;
        }
        RegisterResponse response = new RegisterResponse(status, message);
        try {
            connection.getTCPSenderThread().sendData(response.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Generates a status message based on the registration details.
     *
     * @param nodeDetails  Details of the node (e.g., IP address and port).
     * @param connectionIP IP address extracted from the TCP connection.
     * @param register     A boolean indicating whether it's a registration or
     *                     deregistration request.
     */
    private String checkRegistrationStatus(String nodeDetails, String connectionIP,
            final boolean register) {

        String message = "";
        if (connections.containsKey(nodeDetails) && register) {
            message = nodeDetails + " has previously registered";
        } else if (!connections.containsKey(nodeDetails) && !register) {
            message = nodeDetails + " hasn't registered. ";
        }
        if (!nodeDetails.split(":")[0].equals(connectionIP)
                && !connectionIP.equals("localhost")) {
            message += "Mismatch of IP and port in the request and the socket.";
        }

        System.out.println("Connected Node: " + nodeDetails);

        return message;
    }

    private boolean validatePeerID(Register registerEvent) {
        return registerEvent.getConnectionReadable().hashCode() == registerEvent.getPeerID();

    }

    /**
     * Lists the messaging nodes registered in the overlay.
     * Displays a message if no connections are present.
     */
    private void listPeerNodes() {
        if (connections.size() == 0) {
            System.out.println(
                    "No connections in the registry.");
        } else {
            System.out.println("\nThere are " + connections.size() + " total links:\n");
            connections.forEach((key, value) -> System.out.println("\t" + key));
        }
    }

    private void sendLivePeerInfo(TCPConnection connection, String randomKey) {
        SetupChord message;

        // if (connections.size() == 1) {
        // message = new SetupChord(true);
        // } else {
        // /* send a random live peers network information */

        String[] parts = randomKey.split(":");

        message = new SetupChord(Protocol.SETUP_CHORD, parts[0], Integer.parseInt(parts[1]));
        try {
            connection.getTCPSenderThread().sendData(message.getBytes());
        } catch (IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        // }

    }

}
