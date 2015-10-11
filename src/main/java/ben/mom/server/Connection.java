package ben.mom.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import ben.mom.client.ClientDetails;
import ben.mom.message.Message;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Connection.
 * <p>
 *     A single connection to a client.
 * </p>
 */
public class Connection {

    /**
     * The Logger.
     */
    @NotNull
    private static final Logger LOGGER = LogManager.getLogger(Connection.class.getSimpleName());

    /**
     * Messages to send.
     */
    @NotNull
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    /**
     * The socket.
     */
    @NotNull
    private final Socket socket;

    /**
     * The Input Stream.
     */
    private ObjectInputStream inStream;

    /**
     * The Output Stream.
     */
    private ObjectOutputStream outStream;

    /**
     * The name of the client.
     */
    private String clientName;

    /**
     * True if the connection is running.
     */
    private boolean running;

    /**
     * The Connection Listeners.
     */
    private IConnectionListener connectionListener;

    /**
     * Constructor.
     * @param socket the Socket
     */
    public Connection(@NotNull Socket socket) {
        this.socket = socket;
    }

    /**
     * Start the connection.
     * @param connectionListener the connection listener
     * @return the name of the client
     * @throws Exception something went wrong
     */
    public final String run(@NotNull IConnectionListener connectionListener) throws Exception {
        LOGGER.info("Connection starting");
        assert !running;

        this.connectionListener = connectionListener;
        running = true;

        inStream = new ObjectInputStream(socket.getInputStream());
        outStream = new ObjectOutputStream(socket.getOutputStream());

        Message message = (Message) inStream.readObject();

        if (!message.getDestination().equals("MOM_SERVER") || message.getBody().getClass() != ClientDetails.class) {
            throw new Exception("First message was not the client details");
        }

        ClientDetails clientDetails = (ClientDetails) message.getBody();
        clientName = clientDetails.getClientName();

        new Thread(new InLoop(), "In Loop").start();
        new Thread(new OutLoop(), "Out Loop").start();

        LOGGER.info("Connection started with name " + clientName);
        return clientName;
    }

    /**
     * Add a message to the queue of messages to send.
     * @param message the message to add
     */
    public final void addMessage(@NotNull Message message) {
        messages.add(message);
    }

    /**
     * Stop the connection.
     */
    public final void stop() {
        LOGGER.info("Connection stopping");
        running = false;

        try {
            socket.close();
        }
        catch (IOException e) {
            LOGGER.error("Could not close the socket", e);
        }

        connectionListener.connectionDropped(clientName);
        LOGGER.info("Connection stopped");
    }

    /**
     * Input Loop.
     */
    private class InLoop implements Runnable {

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        Message message = (Message) inStream.readObject();
                        connectionListener.messageReceived(message);
                    }
                    catch (ClassNotFoundException e) {
                        LOGGER.error("Could not read an object", e);
                    }
                }
            }
            catch (IOException e) {
                stop();
            }
        }
    }

    /**
     * Output Loop.
     * <p>
     * Takes messages from the queue and sends them across the ether.
     */
    private class OutLoop implements Runnable {

        @Override
        public void run() {
            try {
                while (running) {
                    Message message = messages.poll(1000, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        outStream.reset();
                        outStream.writeObject(message);
                    }
                }
            }
            catch (@NotNull IOException | InterruptedException e) {
                stop();
            }
        }
    }
}
