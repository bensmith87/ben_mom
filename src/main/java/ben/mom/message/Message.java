package ben.mom.message;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Message.
 */
public class Message implements Serializable {

    /**
     * Serial version UID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Message destination.
     */
    private final String destination;

    /**
     * Message body.
     */
    private final Serializable body;

    /**
     * Constructor.
     * @param destination the destination of the Message (client name or 'All')
     * @param body the Body of the Message
     */
    public Message(@NotNull String destination, @NotNull Serializable body) {
        this.destination = destination;
        this.body = body;
    }

    /**
     * Get the destination of the Message.
     * @return the destination of the Message (client name or 'All')
     */
    @NotNull
    public final String getDestination() {
        return destination;
    }

    /**
     * Get the Body of the Message.
     * @return the Body of the Message.
     */
    @NotNull
    public final Serializable getBody() {
        return body;
    }
}
