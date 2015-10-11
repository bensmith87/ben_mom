package ben.mom.client;

import net.jcip.annotations.Immutable;

import java.io.Serializable;

/**
 * Client Details.
 */
@Immutable
public class ClientDetails implements Serializable {

    /**
     * The name of the client.
     */
    private final String clientName;

    /**
     * Constructor.
     * @param clientName the client name
     */
    public ClientDetails(String clientName) {
        this.clientName = clientName;
    }

    /**
     * Get the client name.
     * @return the client name
     */
    public String getClientName() {
        return clientName;
    }
}
