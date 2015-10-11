package ben.mom.server;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import ben.mom.message.Message;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * MOM Server.
 */
public final class MomServer {

    /**
     * The Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(MomServer.class.getSimpleName());

    /**
     * The port that the Server is listening to connections on.
     */
    private final int serverPort;

    /**
     * All the Connections, keyed by their names.
     */
    private final Map<String, Connection> connections = new HashMap<>();

    /**
     * The last message received of each type.
     */
    private final Map<Class<?>, Message> receivedMessages = new HashMap<>();

    /**
     * The messages to send.
     */
    private final BlockingQueue<Message> messagesToSend = new LinkedBlockingQueue<>();

    /**
     * The message queue thread.
     */
    private final Thread messageQueueThread;

    /**
     * The welcome loop.
     */
    private final WelcomeLoop welcomeLoop;

    /**
     * The connection listener.
     */
    private final ConnectionListener connectionListener = new ConnectionListener();

    /**
     * True if this server is a mock.
     */
    private final boolean mock;

    /**
     * True if the Server is running.
     */
    private boolean running;

    /**
     * Constructor.
     * @param serverPort the port number
     * @param mock true to capture messages instead of forwarding them
     * @throws IOException something went wrong
     */
    public MomServer(int serverPort, boolean mock) throws IOException {
        this.serverPort = serverPort;
        this.mock = mock;

        LOGGER.info("Starting MOM Server");
        running = true;

        MessageQueueLoop messageQueueLoop = new MessageQueueLoop();
        messageQueueThread = new Thread(messageQueueLoop, "Message queue Loop");
        messageQueueThread.start();

        welcomeLoop = new WelcomeLoop();
        Thread welcomeThread = new Thread(welcomeLoop, "Welcome Loop");
        welcomeThread.start();

        LOGGER.info("MOM Server started");
    }

    /**
     * Send a message.
     * @param destination the message destination
     * @param body the message body
     */
    public void sendMessage(@NotNull String destination, @NotNull Serializable body) {
        Message message = new Message(destination, body);
        messagesToSend.add(message);
    }

    /**
     * Get the last received message of a specific type.
     * @param type the type of the message
     * @return the message, null if none have been received
     */
    @Nullable
    public Message getLastReceivedMessage(@NotNull Class<?> type) {
        return receivedMessages.get(type);
    }

    /**
     * Process a message.
     * <p>
     *     Passes the message on to each Client Connection.
     * </p>
     * @param message the message to process
     */
    private void processMessage(@NotNull Message message) {
        if (message.getDestination().equals("ALL")) {
            for (Connection clientConnection : connections.values()) {
                clientConnection.addMessage(message);
            }
        }
        else {
            Connection connection = connections.get(message.getDestination());
            if (connection == null) {
                LOGGER.error("Message for unknown client '" + message.getDestination() + "' " + message.getBody().getClass().getSimpleName());
            }
            else {
                connection.addMessage(message);
            }
        }
    }

    /**
     * Stop the server.
     */
    public void stop() {
        if (running) {
            LOGGER.info("Stopping MOM Server");
            running = false;
            welcomeLoop.stop();
            try {
                messageQueueThread.join();
            }
            catch (InterruptedException e) {
                LOGGER.error("Could not join welcome thread", e);
            }
            for (Connection connection : new HashSet<>(connections.values())) {
                connection.stop();
            }
            LOGGER.info("MOM Server stopped");
        }
    }

    /**
     * Loop to receive new connections from clients.
     */
    private class WelcomeLoop implements Runnable {

        /**
         * The server socket.
         */
        private final ServerSocket socket;

        /**
         * Constructor
         * @throws IOException something went wrong
         */
        public WelcomeLoop() throws IOException {
            socket = new ServerSocket(serverPort);
        }

        @Override
        public void run() {
            LOGGER.info("Welcome loop is running");

            try {
                while (running) {
                    Socket newSocket = socket.accept();
                    LOGGER.info("New connection on " + newSocket.getPort());
                    Connection connection = new Connection(newSocket);
                    try {
                        String clientName = connection.run(connectionListener);
                        connections.put(clientName, connection);
                    }
                    catch (Exception e) {
                        LOGGER.error("Connection could not be started");
                        connection.stop();
                    }
                }
                socket.close();
            }
            catch (IOException e) {
                if (running) {
                    LOGGER.error("Welcome loop interrupted", e);
                    stop();
                }
            }

            LOGGER.info("Welcome loop is stopped");
        }

        /**
         * Stop the welcome loop.
         */
        public void stop() {
            try {
                socket.close();
            }
            catch (IOException e) {
                LOGGER.error("Welcome socket could not be closed", e);
            }
        }
    }

    /**
     * Loop to process the messages in the queue.
     */
    private class MessageQueueLoop implements Runnable {

        @Override
        public void run() {
            LOGGER.info("Message queue loop is running");
            try {
                while (running) {
                    Message message = messagesToSend.poll(1000, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        processMessage(message);
                    }
                }
            }
            catch (InterruptedException e) {
                if (running) {
                    LOGGER.error("Message queue loop interrupted", e);
                    stop();
                }
            }

            LOGGER.info("Message queue loop is stopped");
        }
    }

    /**
     * Connection Listener.
     * <p>
     *     Added to each Connection.
     *     Used to add the received messages to the queue of the simulator and to clean up if the connection has been
     *     dropped.
     * </p>
     */
    private class ConnectionListener implements IConnectionListener {

        @Override
        public void messageReceived(@NotNull Message message) {
            if (mock) {
                receivedMessages.put(message.getBody().getClass(), message);
            }
            else {
                messagesToSend.add(message);
            }
        }

        @Override
        public void connectionDropped(String clientName) {
            LOGGER.info("Connection to " + clientName + " dropped");
            assert connections.containsKey(clientName);
            connections.remove(clientName);
        }
    }
}
