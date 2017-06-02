package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static final int FILESIZE = 1024 * 1024 * 100;

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private List<FileChannel> messageFileQueue = null;
    private volatile List<CheckUnit<MappedByteBuffer>> mappedFile = new ArrayList<>();

    private volatile int consumerNum = 0;

    private static Lock fileQueueLock = new ReentrantLock();

    public void initMessageFileList(String storagePath) {
        fileQueueLock.lock();
        try {
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

            this.consumerNum ++;

        } finally {
            fileQueueLock.unlock();
        }
    }
}