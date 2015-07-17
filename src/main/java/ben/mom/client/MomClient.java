package ben.mom.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import ben.mom.message.Message;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * MOM Client implementation.
 */
public final class MomClient implements IMomClient {

    /**
     * The Logger.
     */
    private static final Logger LOGGER = LogManager.getLogger(MomClient.class.getSimpleName());

    /**
     * The input stream.
     */
    @NotNull
    private final ObjectInputStream inStream;

    /**
     * The output stream.
     */
    @NotNull
    private final ObjectOutputStream outStream;

    /**
     * The message processors, keyed by message class then message name.
     */
    private final Map<Class<?>, Set<IMessageProcessor<?>>> messageProcessors = new HashMap<>();

    /**
     * Messages to send.
     */
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();

    /**
     * The socket.
     */
    private final Socket socket;

    /**
     * True if the client is running.
     */
    private boolean running;

    /**
     * Constructor.
     * @param clientName the name of the controller
     * @param serverHostName the address of the MOM_SERVER Server
     * @param serverPort the port number that the MOM_SERVER Server is listening to new connections on
     * @throws IOException the controller could not initialise the connection
     */
    public MomClient(@NotNull String clientName, @NotNull String serverHostName, int serverPort) throws IOException {
        socket = new Socket(serverHostName, serverPort);
        outStream = new ObjectOutputStream(socket.getOutputStream());
        inStream = new ObjectInputStream(socket.getInputStream());

        running = true;

        // Send the controller name to the simulator
        sendMessage("MOM_SERVER", new ClientDetails(clientName));

        new Thread(new InLoop(), "In Loop").start();
        new Thread(new OutLoop(), "Out Loop").start();
    }

    @Override
    public final <T> void subscribe(@NotNull Class<T> messageClass, @NotNull IMessageProcessor<T> subscriber) {
        Set<IMessageProcessor<?>> processorsForThisType = messageProcessors.get(messageClass);
        if (processorsForThisType == null) {
            processorsForThisType = new HashSet<>();
            messageProcessors.put(messageClass, processorsForThisType);
        }
        processorsForThisType.add(subscriber);
    }

    @Override
    public final void sendMessage(@NotNull String destination, @NotNull Serializable messageObject) {
        Message message = new Message(destination, messageObject);
        messages.add(message);
    }

    /**
     * Process a Message.
     * <p>
     * Calls processMessage on all processors that are registered for this message class.
     * @param <T> the class of the message object that will be processed
     * @param body the message body
     */
    private <T> void processMessage(@NotNull T body) {
        Class<?> messageClass = body.getClass();
        Set<IMessageProcessor<?>> processorsForThisType = messageProcessors.get(messageClass);
        if (processorsForThisType != null) {
            for (IMessageProcessor<?> processor : processorsForThisType) {
                ((IMessageProcessor<T>) processor).processMessage(body);
            }
        }
    }

    public void stop() {
        if (running) {
            LOGGER.info("Stopping MOM Client");
            running = false;
            try {
                socket.close();
            }
            catch (IOException e) {
                LOGGER.error("Socket could not be closed", e);
            }
            LOGGER.info("MOM Client stopped");
        }
    }

    /**
     * In Loop.
     * <p>
     *     Gets messages from the input stream and processes them.
     * </p>
     */
    private class InLoop implements Runnable {

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        Message message = (Message) inStream.readObject();
                        processMessage(message.getBody());
                    }
                    catch (ClassNotFoundException e) {
                        LOGGER.error("Could not read message", e);
                    }
                }
            }
            catch (IOException e) {
                if (running) {
                    LOGGER.error("In stream closed", e);
                    stop();
                }
            }
        }
    }

    /**
     * Out Loop.
     * <p>
     *     Gets messages from the queue and sends them to the MOM_SERVER Server.
     * </p>
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
            catch (IOException e) {
                if (running) {
                    LOGGER.error("Output stream closed", e);
                    stop();
                }
            }
            catch (InterruptedException e) {
                if (running) {
                    LOGGER.error("Out loop interrupted", e);
                    stop();
                }
            }
        }
    }
}
