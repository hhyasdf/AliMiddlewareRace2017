package io.openmessaging.demo;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class DefaultProducer  implements Producer {

    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();

    private String messageFolder = null;
    private MappedByteBuffer messageFileMap;
    private int fileIndex = 1;
    private int filePosition = 0;

    private KeyValue properties;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;

        try {
            String folderName = String.format("%s", this);
            File f = new File(properties.getString("STORE_PATH") + "/" + folderName);

            if (!f.exists()) {
                f.mkdir();
            }

            this.messageFolder = f.getAbsolutePath();

        } catch (Exception e) {
            System.out.println("Cannot write file: " + e);
        }

        this.properties = properties;
    }


    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");

        byte[] messageBytes = ((DefaultBytesMessage)message).transferToBytes();

        if(messageBytes.length * 2 + this.filePosition > MessageStore.FILESIZE || this.messageFileMap == null) {
            if(this.messageFileMap != null) {
                this.messageFileMap.put(DefaultBytesMessage.intToBytes(0));
            }
            try {
                RandomAccessFile messageFile = new RandomAccessFile(this.messageFolder + "/" + this.fileIndex++, "rw");
                this.messageFileMap = messageFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, MessageStore.FILESIZE);
                this.filePosition = 0;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.messageFileMap.put(messageBytes);
        this.filePosition += messageBytes.length;

    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

}
