package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;


public class DefaultBytesMessage implements BytesMessage {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }


    public static byte[] intToBytes(int value) {
        byte[] src = new byte[4];
        src[0] = (byte) ((value>>24) & 0xFF);
        src[1] = (byte) ((value>>16)& 0xFF);
        src[2] = (byte) ((value>>8)&0xFF);
        src[3] = (byte) (value & 0xFF);
        return src;
    }

    public static int bytesToInt(byte[] src) {
        int value;
        value = (int) ( ((src[0] & 0xFF)<<24)
                |((src[1] & 0xFF)<<16)
                |((src[2] & 0xFF)<<8)
                |(src[3] & 0xFF));
        return value;
    }


    public byte[] transferToBytes() {
        DefaultKeyValue header = (DefaultKeyValue) this.headers();
        DefaultKeyValue properties = (DefaultKeyValue) this.properties();
        int messageLength = 0;
        int headerAndPropertiesLength = 0;
        String headerString = "";
        String propertieString = "";

        int topicOrQueueHash = 0;
        String topic = header.getString(MessageHeader.QUEUE);
        String queue = header.getString(MessageHeader.TOPIC);
        topicOrQueueHash = (topic == null ? queue : topic).hashCode();


        for(String key : header.keySet()) {
            if(header.getObject(key) instanceof Integer) {
                headerString = headerString + key + ":" + "I" + ":"+ header.getString(key) + " ";
            } else if(header.getObject(key) instanceof Long) {
                headerString = headerString + key + ":" + "L" + ":"+ header.getString(key) + " ";
            } else if(header.getObject(key) instanceof Double) {
                headerString = headerString + key + ":" + "D" + ":"+ header.getString(key) + " ";
            } else {
                headerString = headerString + key + ":" + "S" + ":"+ header.getString(key) + " ";
            }
        }

        headerString += ";";

        if (properties != null) {
            for (String key : properties.keySet()) {
                if (properties.getObject(key) instanceof Integer) {
                    propertieString = propertieString + key + ":" + "I" + ":" + properties.getString(key) + " ";
                } else if (properties.getObject(key) instanceof Long) {
                    propertieString = propertieString + key + ":" + "L" + ":" + properties.getString(key) + " ";
                } else if (properties.getObject(key) instanceof Double) {
                    propertieString = propertieString + key + ":" + "D" + ":" + properties.getString(key) + " ";
                } else {
                    propertieString = propertieString + key + ":" + "S" + ":" + properties.getString(key) + " ";
                }
            }
        }

        byte[] headerBytes = headerString.getBytes();
        byte[] propertiesBytes = propertieString.getBytes();
        messageLength = headerBytes.length + propertiesBytes.length + this.body.length + 4;
        headerAndPropertiesLength = headerBytes.length + propertiesBytes.length;

        byte[] messageLengthBytes = intToBytes(messageLength);
        byte[] topicOrQueueHashBytes = intToBytes(topicOrQueueHash);
        byte[] headerAndPropertiesLengthBytes = intToBytes(headerAndPropertiesLength);

//        System.out.println(bytesToInt(messageLengthBytes));

        byte[] all = new byte[messageLength + 8];

        System.arraycopy(messageLengthBytes, 0, all, 0, 4);
        System.arraycopy(topicOrQueueHashBytes, 0, all, 4, 4);
        System.arraycopy(headerAndPropertiesLengthBytes, 0, all, 4 + 4, 4);
        System.arraycopy(headerBytes, 0, all, 4 + 4 + 4, headerBytes.length);
        System.arraycopy(propertiesBytes, 0, all, headerBytes.length + 4 + 4 + 4, propertiesBytes.length);
        System.arraycopy(this.body, 0, all, headerBytes.length + propertiesBytes.length + 4 + 4 + 4, this.body.length);

        return all;
    }


    public static Message retransferToMessage(byte[] messageBytes) {
        byte[] headerAndPropertiesLengthBytes = new byte[4];
        System.arraycopy(messageBytes, 0, headerAndPropertiesLengthBytes, 0, 4);
        int headerAndPropertiesLength = bytesToInt(headerAndPropertiesLengthBytes);

        byte[] body = new byte[messageBytes.length - 4 - headerAndPropertiesLength];
        byte[] headerAndPropertiesBytes = new byte[headerAndPropertiesLength];
        System.arraycopy(messageBytes, headerAndPropertiesLength + 4, body, 0, body.length);
        System.arraycopy(messageBytes, 4, headerAndPropertiesBytes, 0, headerAndPropertiesLength);

        Message message = new DefaultBytesMessage(body);

        String[] headerAndProperties = (new String(headerAndPropertiesBytes).split(";"));

        String[] headers = headerAndProperties[0].split(" ");
        String[] properties = null;
        if(headerAndProperties.length == 2) {
            properties = headerAndProperties[2].split(" ");
        }

        for (String header : headers) {
            if (header == "") continue;
            String[] headerItems = header.split(":");

            if (headerItems[1] == "I") {
                message.putHeaders(headerItems[0], Integer.parseInt(headerItems[2]));
            } else if (headerItems[1] == "L") {
                message.putHeaders(headerItems[0], Long.parseLong(headerItems[2]));
            } else if (headerItems[1] == "D") {
                message.putHeaders(headerItems[0], Double.parseDouble(headerItems[2]));
            } else {
                message.putHeaders(headerItems[0], headerItems[2]);
            }
        }

        if (properties != null) {
            for (String propertie : properties) {
                if (propertie == "") break;
                String[] propertyItems = propertie.split(":");

                if (propertyItems[1] == "I") {
                    message.putProperties(propertyItems[0], Integer.parseInt(propertyItems[2]));
                } else if (propertyItems[1] == "L") {
                    message.putProperties(propertyItems[0], Long.parseLong(propertyItems[2]));
                } else if (propertyItems[1] == "D") {
                    message.putProperties(propertyItems[0], Double.parseDouble(propertyItems[2]));
                } else {
                    message.putProperties(propertyItems[0], propertyItems[2]);
                }
            }
        }

        return message;
    }
}
