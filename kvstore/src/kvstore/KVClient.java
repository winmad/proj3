package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * Client API used to issue requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

    private String server;
    private int port;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port on which the server is listening
     */
    public KVClient(String server, int port) {
        this.server = server;
        this.port = port;
    }
    
    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to create or connect socket
     */
    private Socket connectHost() throws KVException {
    	Socket sock = null;
    	try {
    		sock = new Socket(server , port);
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
     * @param  sock Socket to be closed
     */
    private void closeHost(Socket sock) {
        try {
        	sock.close();
        }
        catch (Exception ex) {
        }
    }

    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
        if (key == null || key.length() == 0)
        	throw new KVException(KVConstants.ERROR_INVALID_KEY);
        if (value == null || value.length() == 0)
        	throw new KVException(KVConstants.ERROR_INVALID_VALUE);
        
        KVMessage kvm = new KVMessage(KVConstants.PUT_REQ);
    	kvm.setKey(key);
    	kvm.setValue(value);
    	
    	Socket sock = null;
        try {
        	sock = connectHost();
        	kvm.sendMessage(sock);
        	
        	KVMessage receive = new KVMessage(sock);
        	if (!receive.getMessage().equals(KVConstants.SUCCESS))
        		throw new KVException(receive.getMessage());
        }
        catch (KVException ex) {
        	throw ex;
        }
        finally {
        	if (sock != null)
        		closeHost(sock);
        }
    }

    /**
     * Issues a GET request to the server.
     *
     * @param  key String to get value for in server
     * @return String value associated with key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public String get(String key) throws KVException {
        if (key == null || key.length() == 0)
        	throw new KVException(KVConstants.ERROR_INVALID_KEY);
        
        KVMessage kvm = new KVMessage(KVConstants.GET_REQ);
        kvm.setKey(key);
        
        Socket sock = null;
        String value = null;
        
        try {
        	sock = connectHost();
        	kvm.sendMessage(sock);
        	
        	KVMessage receive = new KVMessage(sock);
        	
        	if (receive.getKey() == null || receive.getValue() == null)
        		throw new KVException(receive.getMessage());
        	value = receive.getValue();
        }
        catch (KVException ex) {
        	throw ex;
        }
        finally {
        	if (sock != null)
        		closeHost(sock);
        }
        return value;
    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
    	if (key == null || key.length() == 0)
        	throw new KVException(KVConstants.ERROR_INVALID_KEY);
        
        KVMessage kvm = new KVMessage(KVConstants.DEL_REQ);
        kvm.setKey(key);
        
        Socket sock = null;
        
        try {
        	sock = connectHost();
        	kvm.sendMessage(sock);
        	
        	KVMessage receive = new KVMessage(sock);
        	if (!receive.getMessage().equals(KVConstants.SUCCESS))
        		throw new KVException(receive.getMessage());
        }
        catch (KVException ex) {
        	throw ex;
        }
        finally {
        	if (sock != null)
        		closeHost(sock);
        }
    }


}
