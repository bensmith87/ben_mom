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
import org.jetbrains.annotations.Nullable;

/**
 * MOM Client implementation.
 */
public final class MomClient implements IMomClient {

    /**
     * The Logger.
     */
    @NotNull
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
    @NotNull
    private final Map<Class<?>, Set<IMessageProcessor<?>>> messageProcessors = new HashMap<>();

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
     * The event queue, null to process the messages on the input loop thread.
     */
    @Nullable
    private final IEventQueue eventQueue;

    /**
     * The input thread.
     */
    @NotNull
    private final Thread inThread;

    /**
     * The output thread.
     */
    @NotNull
    private final Thread outThread;

    /**
     * True if the client is running.
     */
    private boolean running;

    /**
     * Constructor.
     * @param clientName the name of the controller
     * @param serverHostName the address of the MOM_SERVER Server
     * @param serverPort the port number that the MOM_SERVER Server is listening to new connections on
     * @param eventQueue the event queue, null to process the messages on the input loop thread
     * @throws IOException the controller could not initialise the connection
     */
    public MomClient(@NotNull String clientName, @NotNull String serverHostName, int serverPort, @Nullable IEventQueue eventQueue) throws IOException {
        LOGGER.info("Starting MOM Client");
        this.eventQueue = eventQueue;

        socket = new Socket(serverHostName, serverPort);
        outStream = new ObjectOutputStream(socket.getOutputStream());
        inStream = new ObjectInputStream(socket.getInputStream());

        running = true;

        // Send the controller name to the server.
        sendMessage("MOM_SERVER", new ClientDetails(clientName));

        inThread = new Thread(new InLoop(), "In Loop");
        outThread = new Thread(new OutLoop(), "Out Loop");
        inThread.start();
        outThread.start();
        LOGGER.info("MOM Client started");
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
     *     Calls processMessage on all processors that are registered for this message class.
     * </p>
     * @param <T> the class of the message object that will be processed
     * @param body the message body
     */
    @SuppressWarnings("unchecked")
    private <T> void processMessage(@NotNull T body) {
        Class<?> messageClass = body.getClass();
        Set<IMessageProcessor<?>> processorsForThisType = messageProcessors.get(messageClass);
        if (processorsForThisType != null) {
            for (IMessageProcessor<?> processor : processorsForThisType) {
                IMessageProcessor<T> typedProcessor = (IMessageProcessor<T>) processor;
                if (eventQueue == null) {
                    typedProcessor.processMessage(body);
                }
                else {
                    eventQueue.invoke(() -> typedProcessor.processMessage(body));
                }
            }
        }
    }

    /**
     * Stop the MOM client.
     */
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

            try {
                if (Thread.currentThread() != inThread) {
                    inThread.join();
                }
            } catch (InterruptedException e) {
                LOGGER.error("Could not join the input thread", e);
            }

            try {
                if (Thread.currentThread() != outThread) {
                    outThread.join();
                }
            } catch (InterruptedException e) {
                LOGGER.error("Could not join the output thread", e);
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
            LOGGER.info("Input loop is running");
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
            LOGGER.info("Input loop is stopped");
        }
    }

    /**
     * Out Loop.
     * <p>
     *     Gets messages from the queue and sends them to the MOM Server.
     * </p>
     */
    private class OutLoop implements Runnable {

        @Override
        public void run() {
            LOGGER.info("Output loop is running");
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
            LOGGER.info("Output loop is stopped");
        }
    }
}
