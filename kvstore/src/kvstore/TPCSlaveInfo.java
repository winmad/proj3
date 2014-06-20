package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.*;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {

    private long slaveID;
    private String hostname;
    private int port;

    /**
     * Construct a TPCSlaveInfo to represent a slave server.
     *
     * @param info as "SlaveServerID@Hostname:Port"
     * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
     */
    public TPCSlaveInfo(String info) throws KVException {
        if (info == null || !info.contains("@") || !info.contains(":"))
        	throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        
        int p1 = info.indexOf('@');
        int p2 = info.indexOf(':');
        
        try {
        	slaveID = Long.parseLong(info.substring(0, p1));
        	hostname = info.substring(p1 + 1 , p2);
        	port = Integer.parseInt(info.substring(p2 + 1 , info.length()));
        }
        catch (NumberFormatException ex) {
        	throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
    }

    public long getSlaveID() {
        return slaveID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Create and connect a socket within a certain timeout.
     *
     * @return Socket object connected to SlaveServer, with timeout set
     * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
     *         or ERROR_COULD_NOT_CONNECT
     */
    public Socket connectHost(int timeout) throws KVException {
        Socket sock = null;
        
        try {
        	sock = new Socket();
        	sock.connect(new InetSocketAddress(hostname , port) , timeout);
        }
        catch (SocketTimeoutException ex) {
        	throw new KVException(KVConstants.ERROR_SOCKET_TIMEOUT);
        }
        catch (IOException ex) {
        	throw new KVException(KVConstants.ERROR_COULD_NOT_CONNECT);
        }
        catch (Exception ex) {
        	throw new KVException(KVConstants.ERROR_COULD_NOT_CREATE_SOCKET);
        }
        return sock;
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param sock Socket to be closed
     */
    public void closeHost(Socket sock) {
    	try {
        	sock.close();
        }
        catch (Exception ex) {
        }
    }
}
