package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    private long slaveID;
    private KVServer kvServer;
    private TPCLog tpcLog;
    private ThreadPool threadpool;

    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
        String regInfo = slaveID + "@" + server.getHostname() + ":" + server.getPort();
        KVMessage reg = new KVMessage(KVConstants.REGISTER , regInfo);
        Socket sock = null;
        
        try {
        	sock = new Socket(masterHostname , 9090);
        }
        catch (IOException ex) {
    		throw new KVException(KVConstants.ERROR_COULD_NOT_CONNECT);
    	}
    	catch (Exception ex) {
    		throw new KVException(KVConstants.ERROR_COULD_NOT_CREATE_SOCKET);
    	}
        
        try {
        	reg.sendMessage(sock);
        	KVMessage resp = new KVMessage(sock);
        	if (!resp.getMsgType().equals(KVConstants.RESP) ||
        		!resp.getMessage().equals("Successfully registered " + regInfo))
        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
        catch (Exception ex) {
        	throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
        finally {
        	try {
        		if (sock != null)
        			sock.close();
        	}
        	catch (Exception ex) {        		
        	}
        }
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        try {
        	threadpool.addJob(new MasterHandler(master));
        }
        catch (InterruptedException ex) {        	
        }
    }

    /**
     * Runnable class containing routine to service a message from the master.
     */
    private class MasterHandler implements Runnable {

        private Socket master;

        /**
         * Construct a MasterHandler.
         *
         * @param master Socket connected to master with the message
         */
        public MasterHandler(Socket master) {
            this.master = master;
        }

        /**
         * Processes request from master and sends back a response with the
         * result. This method needs to handle both phase1 and phase2 messages
         * from the master. The delivery of the response is best-effort. If
         * we are unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            KVMessage req = null;
            KVMessage resp = null;
            
            try {
            	req = new KVMessage(master);
            	
            	if (req.getMsgType().equals(KVConstants.PUT_REQ)) {
            		if (req.getKey() == null || req.getKey().length() == 0)
            			resp = new KVMessage(KVConstants.ABORT , KVConstants.ERROR_INVALID_KEY);
            		else if (req.getKey().length() > KVServer.MAX_KEY_SIZE)
            			resp = new KVMessage(KVConstants.ABORT , KVConstants.ERROR_OVERSIZED_KEY);
            		else if (req.getValue() == null || req.getValue().length() == 0)
            			resp = new KVMessage(KVConstants.ABORT , KVConstants.ERROR_INVALID_VALUE);
            		else if (req.getValue().length() > KVServer.MAX_VAL_SIZE)
            			resp = new KVMessage(KVConstants.ABORT , KVConstants.ERROR_OVERSIZED_VALUE);
            		else
            			resp = new KVMessage(KVConstants.READY);
            	}
            	else if (req.getMsgType().equals(KVConstants.DEL_REQ)) {
            		if (req.getKey() == null || req.getKey().length() == 0)
            			resp = new KVMessage(KVConstants.ABORT , KVConstants.ERROR_INVALID_KEY);
            		else if (req.getKey().length() > KVServer.MAX_KEY_SIZE)
            			resp = new KVMessage(KVConstants.ABORT , KVConstants.ERROR_OVERSIZED_KEY);
            		else if (!kvServer.hasKey(req.getKey()))
            			resp = new KVMessage(KVConstants.ABORT , KVConstants.ERROR_NO_SUCH_KEY);
            		else
            			resp = new KVMessage(KVConstants.READY);
            	}
            	else if (req.getMsgType().equals(KVConstants.GET_REQ)) {
            		if (req.getKey() == null || req.getKey().length() == 0)
            			resp = new KVMessage(KVConstants.RESP , KVConstants.ERROR_INVALID_KEY);
            		else if (req.getKey().length() > KVServer.MAX_KEY_SIZE)
            			resp = new KVMessage(KVConstants.RESP , KVConstants.ERROR_OVERSIZED_KEY);
            		else if (!kvServer.hasKey(req.getKey()))
            			resp = new KVMessage(KVConstants.RESP , KVConstants.ERROR_NO_SUCH_KEY);
            		else {
            			String value = kvServer.get(req.getKey());
            			resp = new KVMessage(KVConstants.RESP);
            			resp.setKey(req.getKey());
            			resp.setValue(value);
            		}
            	}
            	else if (req.getMsgType().equals(KVConstants.COMMIT)) {
            		resp = new KVMessage(KVConstants.ACK);
            		
            		KVMessage lastMsg = tpcLog.getLastEntry();
            		
            		if (lastMsg.getMsgType().equals(KVConstants.PUT_REQ)) {
            			kvServer.put(lastMsg.getKey() , lastMsg.getValue());
            		}
            		else if (lastMsg.getMsgType().equals(KVConstants.DEL_REQ)) {
            			kvServer.del(lastMsg.getKey());
            		}
            	}
            	else if (req.getMsgType().equals(KVConstants.ABORT)) {
            		resp = new KVMessage(KVConstants.ACK);
            	}
            }
            catch (Exception ex) {
            	return;
            }
            
            tpcLog.appendAndFlush(req);
            if (resp != null) {
            	try {
            		resp.sendMessage(master);
            	}
            	catch (Exception ex) {            		
            	}
            }
        }

    }

}
