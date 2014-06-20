package kvstore;

import static kvstore.KVConstants.*;

import java.io.*;
import java.net.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This is the object that is used to generate the XML based messages
 * for communication between clients and servers.
 */
public class KVMessage implements Serializable {

    private String msgType;
    private String key;
    private String value;
    private String message;

    public static final long serialVersionUID = 6473128480951955693L;

    /**
     * Construct KVMessage with only a type.
     *
     * @param msgType the type of this KVMessage
     */
    public KVMessage(String msgType) {
        this(msgType, null);
    }

    /**
     * Construct KVMessage with type and message.
     *
     * @param msgType the type of this KVMessage
     * @param message the content of this KVMessage
     */
    public KVMessage(String msgType, String message) {
        this.msgType = msgType;
        this.message = message;
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * Parse XML from the InputStream with unlimited timeout.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock) throws KVException {
        this(sock, 0);
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * This constructor parses XML from the InputStream within a certain timeout
     * or with an unlimited timeout if the provided argument is 0.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @param  timeout total allowable receipt time, in milliseconds
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock, int timeout) throws KVException {
        try {
        	sock.setSoTimeout(timeout);
        }
        catch (SocketException ex) {
        	throw new KVException(KVConstants.ERROR_SOCKET_TIMEOUT);
        }
        
        Document doc = null;
        try {
        	DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        	doc = docBuilder.parse(new NoCloseInputStream(sock.getInputStream()));	
        }
        catch (IOException ex) {
        	throw new KVException(KVConstants.ERROR_COULD_NOT_RECEIVE_DATA);
        }
        catch (Exception ex) {
        	throw new KVException(KVConstants.ERROR_PARSER);
        }
        
        try {
	        Element root = doc.getDocumentElement();
	        root.normalize();
	        
	        msgType = root.getAttribute("type");
	        NodeList keyNode = doc.getElementsByTagName("Key");
	        NodeList valueNode = doc.getElementsByTagName("Value");
	        NodeList msgNode = doc.getElementsByTagName("Message");
	        
	        if (msgType.equals(KVConstants.PUT_REQ)) {
	        	if (keyNode.getLength() > 0)
	        		key = keyNode.item(0).getTextContent();
	        	else
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        	
	        	if (valueNode.getLength() > 0)
	        		value = valueNode.item(0).getTextContent();
	        	else
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        	
	        	if (key == null || key.length() == 0 ||
	        		value == null || value.length() == 0)
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        }
	        else if (msgType.equals(KVConstants.GET_REQ)) {
	        	if (keyNode.getLength() > 0)
	        		key = keyNode.item(0).getTextContent();
	        	else
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        	
	        	if (key == null || key.length() == 0)
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        	
	        }
	        else if (msgType.equals(KVConstants.DEL_REQ)) {
	        	if (keyNode.getLength() > 0)
	        		key = keyNode.item(0).getTextContent();
	        	else
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        	
	        	if (key == null || key.length() == 0)
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        }
	        else if (msgType.equals(KVConstants.READY)) {
	        }
	        else if (msgType.equals(KVConstants.ABORT)) {
	        	if (msgNode.getLength() > 0) {
	        		message = msgNode.item(0).getTextContent();
	        		if (message == null)
	        			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        	}
	        }
	        else if (msgType.equals(KVConstants.COMMIT)) {
	        }
	        else if (msgType.equals(KVConstants.ACK)) {
	        }
	        else if (msgType.equals(KVConstants.REGISTER)) {
	        	if (msgNode.getLength() > 0)
	        		message = msgNode.item(0).getTextContent();
	        	if (message == null || !message.contains("@") ||
	        		!message.contains(":"))
	        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        }
	        else if (msgType.equals(KVConstants.RESP)) {
	        	if (msgNode.getLength() > 0)
	        		message = msgNode.item(0).getTextContent();
	        	if (keyNode.getLength() > 0)
	        		key = keyNode.item(0).getTextContent();
	        	if (valueNode.getLength() > 0)
	        		value = valueNode.item(0).getTextContent();
	        	
	        	if (message == null) {
	        		if (key == null || key.length() == 0 ||
	        			value == null || value.length() == 0)
	        			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        	}
	        }
	        else {
	        	throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
	        }
        }
        catch (Exception ex) {
        	throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
    }

    /**
     * Constructs a KVMessage by copying another KVMessage.
     *
     * @param kvm KVMessage with fields to copy
     */
    public KVMessage(KVMessage kvm) {
        msgType = kvm.getMsgType();
        key = kvm.getKey();
        value = kvm.getValue();
        message = kvm.getMessage();
    }

    /**
     * Generate the serialized XML representation for this message. See
     * the spec for details on the expected output format.
     *
     * @return the XML string representation of this KVMessage
     * @throws KVException with ERROR_INVALID_FORMAT or ERROR_PARSER
     */
    public String toXML() throws KVException {
        DocumentBuilder docBuilder = null;
        try {
        	docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        }
        catch (ParserConfigurationException ex) {
        	throw new KVException(KVConstants.ERROR_PARSER);
        }
        
        Document doc = docBuilder.newDocument();
        
        Element kvm = doc.createElement("KVMessage");
        doc.appendChild(kvm);
        
        if (this.msgType == null)
        	throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        
        kvm.setAttribute("type" , msgType);
        
        if (msgType.equals(KVConstants.RESP)) {
        	if (this.message != null) {
        		if (this.key != null || this.value != null)
        			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        		
        		Element messageElement = doc.createElement("Message");
        		messageElement.appendChild(doc.createTextNode(this.message));
        		kvm.appendChild(messageElement);
        	}
        	else {
        		if (this.key == null || this.value == null)
        			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        		
        		Element keyElement = doc.createElement("Key");
        		keyElement.appendChild(doc.createTextNode(this.key));
        		kvm.appendChild(keyElement);
        		
        		Element valueElement = doc.createElement("Value");
        		valueElement.appendChild(doc.createTextNode(this.value));
        		kvm.appendChild(valueElement);
        	}
        }
        else if (msgType.equals(KVConstants.PUT_REQ) ||
        		msgType.equals(KVConstants.GET_REQ) ||
        		msgType.equals(KVConstants.DEL_REQ)) {     	
        	if (this.key == null)
        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        	Element keyElement = doc.createElement("Key");
        	keyElement.appendChild(doc.createTextNode(this.key));
        	kvm.appendChild(keyElement);
        	
        	if (this.msgType.equals(KVConstants.PUT_REQ)) {
        		if (this.value == null)
        			throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        		
        		Element valueElement = doc.createElement("Value");
        		valueElement.appendChild(doc.createTextNode(this.value));
        		kvm.appendChild(valueElement);
        	}
        }
        else if (msgType.equals(KVConstants.READY)) {
        }
        else if (msgType.equals(KVConstants.ABORT)) {
        	if (message != null) {
        		Element messageElement = doc.createElement("Message");
        		messageElement.appendChild(doc.createTextNode(this.message));
        		kvm.appendChild(messageElement);
        	}
        }
        else if (msgType.equals(KVConstants.COMMIT)) {
        }
        else if (msgType.equals(KVConstants.ACK)) {        	
        }
        else if (msgType.equals(KVConstants.REGISTER)) {
        	if (message == null) 
        		throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        	Element messageElement = doc.createElement("Message");
    		messageElement.appendChild(doc.createTextNode(this.message));
    		kvm.appendChild(messageElement);
        }
        else {
        	throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
        
        String res = null;
        try {
        	res = printDoc(doc);
        }
        catch (Exception ex) {
        	throw new KVException(KVConstants.ERROR_PARSER);
        }
        return res;
    }


    /**
     * Send serialized version of this KVMessage over the network.
     * You must call sock.shutdownOutput() in order to flush the OutputStream
     * and send an EOF (so that the receiving end knows you are done sending).
     * Do not call close on the socket. Closing a socket closes the InputStream
     * as well as the OutputStream, preventing the receipt of a response.
     *
     * @param  sock Socket to send XML through
     * @throws KVException with ERROR_INVALID_FORMAT, ERROR_PARSER, or
     *         ERROR_COULD_NOT_SEND_DATA
     */
    public void sendMessage(Socket sock) throws KVException {
    	PrintWriter p = null;
        try {
        	p = new PrintWriter(sock.getOutputStream());
        	p.write(this.toXML());
        	p.flush();
        	
        	sock.shutdownOutput();
        }
        catch (IOException ex) {
        	throw new KVException(KVConstants.ERROR_COULD_NOT_SEND_DATA);
        }
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMsgType() {
        return msgType;
    }


    @Override
    public String toString() {
        try {
            return this.toXML();
        } catch (KVException e) {
            // swallow KVException
            return e.toString();
        }
    }

    /*
     * InputStream wrapper that allows us to reuse the corresponding
     * OutputStream of the socket to send a response.
     * Please read about the problem and solution here:
     * http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html
     */
    private class NoCloseInputStream extends FilterInputStream {
        public NoCloseInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() {} // ignore close
    }

    /* http://stackoverflow.com/questions/2567416/document-to-string/2567428#2567428 */
    public static String printDoc(Document doc) {
        try {
            StringWriter sw = new StringWriter();
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");

            transformer.transform(new DOMSource(doc), new StreamResult(sw));
            return sw.toString();
        } catch (Exception ex) {
            throw new RuntimeException("Error converting to String", ex);
        }
    }


}
