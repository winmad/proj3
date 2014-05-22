package kvstore;

import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on
 * the eviction policy.
 */
public class KVCache implements KeyValueInterface {
	
	private int numSets = 100;
	private int maxElemsPerSet = 10;
	
	private LinkedList<KVCacheSet> data;
	
	public class KVCacheEntry {
		public String key;
		public String value;
		public boolean isRef;
		
		public KVCacheEntry() {
			key = value = null;
			isRef = false;
		}
		
		public KVCacheEntry(String key , String value , boolean isRef) {
			this.key = key;
			this.value = value;
			this.isRef = isRef;
		}
	}
	
	public class KVCacheSet {
		public LinkedList<KVCacheEntry> entry;
		public Lock lock;
		
		public KVCacheSet() {
			entry = new LinkedList<KVCacheEntry>();
			lock = new ReentrantLock();
		}
	}
	
    /**
     * Constructs a second-chance-replacement cache.
     *
     * @param numSets the number of sets this cache will have
     * @param maxElemsPerSet the size of each set
     */
    @SuppressWarnings("unchecked")
    public KVCache(int numSets, int maxElemsPerSet) {
        this.numSets = numSets;
        this.maxElemsPerSet = maxElemsPerSet;
        
        data = new LinkedList<KVCacheSet>();
        for (int i = 0; i < numSets; i++) {
        	KVCacheSet cacheSet = new KVCacheSet();
        	data.add(cacheSet);
        }
    }

    /**
     * Retrieves an entry from the cache.
     * Assumes access to the corresponding set has already been locked by the
     * caller of this method.
     *
     * @param  key the key whose associated value is to be returned.
     * @return the value associated to this key or null if no value is
     *         associated with this key in the cache
     */
    @Override
    public String get(String key) {
        int setId = getSetId(key);
        for (KVCacheEntry e : data.get(setId).entry) {
        	if (e.key.equals(key)) {
        		e.isRef = true;
        		return e.value;
        	}
        }
        return null;
    }

    /**
     * Adds an entry to this cache.
     * If an entry with the specified key already exists in the cache, it is
     * replaced by the new entry. When an entry is replaced, its reference bit
     * will be set to True. If the set is full, an entry is removed from
     * the cache based on the eviction policy. If the set is not full, the entry
     * will be inserted behind all existing entries. For this policy, we suggest
     * using a LinkedList over an array to keep track of entries in a set since
     * deleting an entry in an array will leave a gap in the array, likely not
     * at the end. More details and explanations in the spec. Assumes access to
     * the corresponding set has already been locked by the caller of this
     * method.
     *
     * @param key the key with which the specified value is to be associated
     * @param value a value to be associated with the specified key
     */
    @Override
    public void put(String key, String value) {
        int setId = getSetId(key);
        for (KVCacheEntry e : data.get(setId).entry) {
        	if (e.key.equals(key)) {
        		e.value = value;
        		e.isRef = true;
        		return;
        	}
        }
        // not in cache
        if (data.get(setId).entry.size() == maxElemsPerSet) {
        	// is full
        	for (;;) {
	        	for (KVCacheEntry e : data.get(setId).entry) {
	        		if (e.isRef == false) {
	        			e.key = key;
	        			e.value = value;
	        			e.isRef = false;
	        			return;
	        		}
	        		else
	        			e.isRef = false;
	        	}
        	}
        }
        else {
        	data.get(setId).entry.add(new KVCacheEntry(key , value , false));
        }
    }

    /**
     * Removes an entry from this cache.
     * Assumes access to the corresponding set has already been locked by the
     * caller of this method. Does nothing if called on a key not in the cache.
     *
     * @param key key with which the specified value is to be associated
     */
    @Override
    public void del(String key) {
        int setId = getSetId(key);
        for (KVCacheEntry e : data.get(setId).entry) {
        	if (e.key.equals(key)) {
        		data.get(setId).entry.remove(e);
        		return;
        	}
        }
    }

    /**
     * Get a lock for the set corresponding to a given key.
     * The lock should be used by the caller of the get/put/del methods
     * so that different sets can be modified in parallel.
     *
     * @param  key key to determine the lock to return
     * @return lock for the set that contains the key
     */
    public Lock getLock(String key) {
        return data.get(getSetId(key)).lock;
    }

    /**
     * Get the id of the set for a particular key.
     *
     * @param  key key of interest
     * @return set of the key
     */
    private int getSetId(String key) {
        return Math.abs(key.hashCode()) % numSets;
    }

    /**
     * Serialize this store to XML. See spec for details on output format.
     * This method is best effort. Any exceptions that arise can be dropped.
     */
    public String toXML() {
        try {
        	DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        	Document doc = docBuilder.newDocument();
        	
        	Element kvc = doc.createElement("KVCache");
        	doc.appendChild(kvc);
        	
        	for (int i = 0; i < numSets; i++) {
        		Element s = doc.createElement("Set");
        		s.setAttribute("Id" , Integer.toString(i));
        		
        		kvc.appendChild(s);
        		for (KVCacheEntry e : data.get(i).entry) {
        			Element entryElem = doc.createElement("CacheEntry");
        			entryElem.setAttribute("isReferenced" , Boolean.toString(e.isRef));;
        			s.appendChild(entryElem);
        			
        			Element keyElem = doc.createElement("Key");
        			keyElem.appendChild(doc.createTextNode(e.key));
        			entryElem.appendChild(keyElem);
        			
        			Element valueElem = doc.createElement("Value");
        			valueElem.appendChild(doc.createTextNode(e.value));
        			entryElem.appendChild(valueElem);
        		}
        	}
        	return KVMessage.printDoc(doc);
        }
        catch (Exception ex) {
        	return null;
        }
    }

    @Override
    public String toString() {
        return this.toXML();
    }

}
