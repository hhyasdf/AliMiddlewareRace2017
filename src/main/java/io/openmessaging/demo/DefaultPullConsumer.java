package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
//    public volatile int consumerCounter = 0;

//    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    private Set<Integer> buckets = new HashSet<>();

    private List<FileChannel> messageFileQueue = null;
    private MappedByteBuffer mappedByteBuffer = null;

//    private List<Message> messageBuffer = new ArrayList<>();

//    private int id = 0;

//    private static Lock idLock = new ReentrantLock();


    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
//        this.messageStore.initMessageFileList(properties.getString("STORE_PATH"));

        String storagePath = properties.getString("STORE_PATH");

        if (this.messageFileQueue == null) {
            this.messageFileQueue = new ArrayList<>();
            File storageFolder = new File(storagePath);
            for (String forder : storageFolder.list()) {
                for (String file : (new File(storagePath + "/" + forder)).list()) {
                    try {
                        this.messageFileQueue.add(new RandomAccessFile(storagePath + "/" + forder + "/" + file, "rw").getChannel());
                    } catch (Exception e) {
                        System.out.println("Cannot read file: " + e);
                    }
                }
            }
        }

//        idLock.lock();
//        try {
//            this.id  = consumerCounter++;
//        } finally {
//            idLock.unlock();
//        }
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {

        while (true) {
            if (this.mappedByteBuffer == null) {
                if (this.messageFileQueue.size() == 0) break;
                try {
                    this.mappedByteBuffer = this.messageFileQueue.get(0).map(FileChannel.MapMode.READ_ONLY, 0, MessageStore.FILESIZE);
                    this.messageFileQueue.remove(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            byte[] messageLengthBytes = new byte[4];
            this.mappedByteBuffer.get(messageLengthBytes);
            int messageLength = DefaultBytesMessage.bytesToInt(messageLengthBytes);

            if(messageLength == 0) {
                this.mappedByteBuffer = null;
                continue;
            }

            byte[] topicOrQueueBytes = new byte[4];
            this.mappedByteBuffer.get(topicOrQueueBytes);
            int topicOrQueue = DefaultBytesMessage.bytesToInt(topicOrQueueBytes);

            if (this.buckets.contains(topicOrQueue)) {
                byte[] messageBytes = new byte[messageLength];
                this.mappedByteBuffer.get(messageBytes);
                Message m = DefaultBytesMessage.retransferToMessage(messageBytes);
                return m;
            } else {
                this.mappedByteBuffer.position(this.mappedByteBuffer.position() + messageLength);
            }
        }

        return null;
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        buckets.add(queueName.hashCode());
        for (String topic : topics) {
            buckets.add(topic.hashCode());
        }
    }

}
