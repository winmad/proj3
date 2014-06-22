package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class TPCMaster {

    private int numSlaves;
    private KVCache masterCache;

    public LinkedList<TPCSlaveInfo> slaves;
    
    public static final int TIMEOUT = 3000;
    public static final int BLOCK_TIME = 500;

    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        
        slaves = new LinkedList<TPCSlaveInfo>();
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered.Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public void registerSlave(TPCSlaveInfo slave) {
        synchronized(slaves) {
        	for (int i = 0; i < slaves.size(); i++) {
        		if (slaves.get(i).getSlaveID() == slave.getSlaveID()) {
        			slaves.set(i , slave);
        			return;
        		}
        		
        		if (isLessThanUnsigned(slave.getSlaveID() , slaves.get(i).getSlaveID())) {
        			if (slaves.size() == numSlaves)
        				return;
        			slaves.add(i , slave);
        			return;
        		}
        	}
        	
        	if (slaves.size() == numSlaves)
        		return;
        	slaves.add(slave);
        }
    }

    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }

    
    public int findFirstReplicaIndex(long val) {
    	assert(slaves.size() >= 2);
    	int N = slaves.size() - 1;
    	if (isLessThanEqualUnsigned(val , slaves.get(0).getSlaveID()) ||
    		!isLessThanEqualUnsigned(val , slaves.get(N).getSlaveID()))
    		return 0;
    	
    	int l = 0 , r = N , mid;
    	while (l + 1 < r) {
    		mid = ((l + r) >> 1);
    		if (!isLessThanEqualUnsigned(val , slaves.get(mid).getSlaveID()))
    			l = mid;
    		else 
    			r = mid;
    	}
    	return r;
    }
    
    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
    	int index = findFirstReplicaIndex(hashTo64bit(key));
        return slaves.get(index);
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
        int index = (findFirstReplicaIndex(firstReplica.getSlaveID()) + 1) % slaves.size();
        return slaves.get(index);
    }

    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq)
            throws KVException {
    	// wait until all slaves register before servicing any requests
        while (slaves.size() < numSlaves) {
        	try {
        		Thread.sleep(BLOCK_TIME);
        	}
        	catch (Exception ex) {        		
        	}
        }
        
        Lock lock = masterCache.getLock(msg.getKey());
        String errMsg = null;
        try {
        	lock.lock();
        	
        	// phase 1
        	TPCSlaveInfo reps[] = new TPCSlaveInfo[2];
        	reps[0] = findFirstReplica(msg.getKey());
        	reps[1] = findSuccessor(reps[0]);
        	
        	boolean commit = true;
        	
        	Socket sock;
        	KVMessage resp;
        	
        	for (int i = 0; i < 2; i++) {
        		sock = null;
        		resp = null;
        		try {
        			sock = reps[i].connectHost(TIMEOUT);
        			msg.sendMessage(sock);
        			resp = new KVMessage(sock);
        			if (!resp.getMsgType().equals(KVConstants.READY)) {
        				commit = false;
        				errMsg = resp.getMessage();
        			}
        		}
        		catch (KVException ex) {
        			commit = false;
            		if (errMsg == null)
            			errMsg = ex.getKVMessage().getMessage();
        		}
        		finally {
        			if (sock != null) {
        				reps[i].closeHost(sock);
        			}
        		}
        	}
        	
        	// phase 2
        	KVMessage decision = null;
        	if (commit) {
        		decision = new KVMessage(KVConstants.COMMIT);
        		
        		if (isPutReq)
        			masterCache.put(msg.getKey() , msg.getValue());
        		else
        			masterCache.del(msg.getKey());
        	}
        	else {
        		decision = new KVMessage(KVConstants.ABORT);
        	}
        	
        	for (int i = 0; i < 2; i++) {        		
        		int index;
        		index = (findFirstReplicaIndex(hashTo64bit(msg.getKey())) + i) % slaves.size();
        		for (;;) {
        			sock = null;
        			resp = null;
        			TPCSlaveInfo slave = slaves.get(index); // may not be the same object reps[i] due to failure
        			
        			try {
        				sock = slave.connectHost(TIMEOUT);
        				decision.sendMessage(sock);
        				resp = new KVMessage(sock);
        			}
        			catch (Exception ex) {
        				continue;
        			}
        			finally {
        				if (sock != null) {
        					slave.closeHost(sock);
        				}
        			}
        			
        			if (resp.getMsgType().equals(KVConstants.ACK)) {
        				break;
        			}
        			else {
        				System.err.println("Not ACK error: " + resp.getMsgType() + " / " + resp.getMessage());
        				throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        			}
        		}
        	}
        	
        	// slave may vote abort due to
        	// -- key doesn't exist for DEL
        	// -- oversized key/value for PUT
        	// need to throw
        	if (!commit) { 
        		throw new KVException(errMsg);
        	}
        }
        finally {
        	lock.unlock();
        }
    }

    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVExceptions from both replicas
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from either slave for any reason
     */
    public String handleGet(KVMessage msg) throws KVException {
    	// wait until all slaves register before servicing any requests
    	while (slaves.size() < numSlaves) {
        	try {
        		Thread.sleep(BLOCK_TIME);
        	}
        	catch (Exception ex) {        		
        	}
        }
    	
    	Lock lock = masterCache.getLock(msg.getKey());
    	String key = msg.getKey();
    	String value = null;
    	
    	try {
    		lock.lock();
    		
    		value = masterCache.get(msg.getKey());
    		
    		if (value == null) {
    			TPCSlaveInfo slave = findFirstReplica(key);
    			value = handleGetBySlave(msg , slave);
    			
    			if (value == null) {
    				slave = findSuccessor(slave);
    				value = handleGetBySlave(msg , slave);
    			}    		
    		}
    		
    		if (value != null) {
    			masterCache.put(key , value);
    		}
    	}
    	finally {
    		lock.unlock();
    	}
    	
    	if (value == null) {
    		throw new KVException(KVConstants.ERROR_NO_SUCH_KEY);
    	}
    	
        return value;
    }
    
    public String handleGetBySlave(KVMessage msg , TPCSlaveInfo slave) {
    	String value = null;
    	Socket sock = null;
    	KVMessage resp = null;
    	
    	try {
    		sock = slave.connectHost(TIMEOUT);
    		msg.sendMessage(sock);
    		resp = new KVMessage(sock);
    		
    		if (resp.getMsgType().equals(KVConstants.RESP) && resp.getValue() != null &&
    			resp.getValue().length() > 0)
    			value = resp.getValue();
    	}
    	catch (Exception ex) {    		
    	}
    	finally {
    		if (sock != null) {
    			slave.closeHost(sock);
    		}
    	}
    	return value;
    }

}
