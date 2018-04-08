package meaningless;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;

public class DataCollector {

    static final int CLIENT_NUM = 2;
    static final int MAX_GAP = 10;
    static final int MESSAGE_NUM = 10;
    static final long MAX_SLEEP_INTERVAL = 5;
    static AtomicLong token = new AtomicLong(0);
    static AtomicLong step = new AtomicLong(0);
    static Random rand = new Random();
    static List<LinkedTransferQueue<Data>> queue = new ArrayList<>();

    public static class Data {
        String kind;
        long prepare;
        long commit;
        public String toString() {
            return String.format("%7s: %5d:%5d", kind, prepare, commit);
        }
    }

    static private long incrementToken() {
        return token.addAndGet(Math.abs(rand.nextLong()) % MAX_GAP + 1);
    }

    static private void sleep(long factor) {
        long interval = step.addAndGet(3) % factor + 1;
        long waitTime = Math.abs(rand.nextLong()) % interval;
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static private void sendPrepare(int channel, long pHandle) {
        Data prepare = new Data();
        prepare.kind = "prepare";
        prepare.prepare = pHandle;
        queue.get(channel).offer(prepare);
    }

    static private void sendCommit(int channel, long pHandle, long cHandle) {
        Data commit = new Data();
        commit.kind = "commit";
        commit.prepare = pHandle;
        commit.commit = cHandle;
        queue.get(channel).offer(commit);
    }

    static private void generateData(int index) {
        for (int i = 0; i < MESSAGE_NUM; i++) {
            long prepare = incrementToken();
            sleep(MAX_SLEEP_INTERVAL);
            sendPrepare(index, prepare);
            sleep(MAX_SLEEP_INTERVAL);
            long commit = incrementToken();
            sleep(MAX_SLEEP_INTERVAL);
            sendCommit(index, prepare, commit);
            sleep(10 * MAX_SLEEP_INTERVAL);
        }
    }

    static private void collect() {
        while (true) {
            for (int i = 0; i < CLIENT_NUM; i++) {
                try {
                    Data packet = queue.get(i).take();
                    System.out.println(packet);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    static public void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(2 * CLIENT_NUM + 1);

        for (int i = 0; i < CLIENT_NUM; i++) {
            queue.add(new LinkedTransferQueue<Data>());
        }

        for (int i = 0; i < CLIENT_NUM; i++) {
            final int index = i;
            Thread sender = new Thread(() -> {
                generateData(index);
                latch.countDown();
            });
            Thread anotherSender = new Thread(() -> {
                generateData(index);
                latch.countDown();
            });
            sender.start();
            anotherSender.start();
        }

        Thread receiver = new Thread(() -> {
            collect();
            latch.countDown();
        });
        receiver.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
