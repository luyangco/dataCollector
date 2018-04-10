package meaningless;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DataCollector {

    static final int CLIENT_NUM = 2;
    static final int MAX_GAP = 10;
    static final int MESSAGE_NUM = 20;
    static final long MAX_SLEEP_INTERVAL = 5;
    static final int MAX_MESSAGE_INFLIGHT = 100;
    static final int PREREAD_THRESHOLD = 10;
    AtomicLong token = new AtomicLong(0);
    AtomicLong step = new AtomicLong(0);
    Random rand = new Random();
    List<LinkedBlockingQueue<Data>> queue = new ArrayList<>();
    // Generate 2 permits, one for each receiver thread
    Semaphore mutex = new Semaphore(CLIENT_NUM);
    // This lock is used for flow control
    Semaphore flowControl = new Semaphore(0);
    AtomicBoolean hasBackPressure = new AtomicBoolean(false);

    public static class Data {
        String kind;
        long prepare;
        long commit;
        public String toString() {
            return String.format("%7s: %5d:%5d", kind, prepare, commit);
        }
    }

    public DataCollector() {
        for (int i = 0; i < CLIENT_NUM; i++) {
            queue.add(new LinkedBlockingQueue<Data>(MAX_MESSAGE_INFLIGHT));
        }
    }

    private long incrementToken() {
        return token.addAndGet(Math.abs(rand.nextLong()) % MAX_GAP + 1);
    }

    private void sleep(long factor) {
        long interval = step.addAndGet(3) % factor + 1;
        long waitTime = Math.abs(rand.nextLong()) % interval;
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void sendPrepare(int channel, long pHandle) {
        Data prepare = new Data();
        prepare.kind = "prepare";
        prepare.prepare = pHandle;
        queue.get(channel).offer(prepare);
    }

    private void sendCommit(int channel, long pHandle, long cHandle) {
        Data commit = new Data();
        commit.kind = "commit";
        commit.prepare = pHandle;
        commit.commit = cHandle;
        queue.get(channel).offer(commit);
    }

    private void generateData(int index) {
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

    // Consolidate map into another map.
    private void doConsolidate(TreeMap<Long, Data> map, TreeMap<Long, Data> anotherMap) {
        Long cutoff = map.lastKey();
        anotherMap.putAll(map);
        map.clear();
        // Retrieve all the entries less than or equal to the cutoff value
        NavigableMap<Long, Data> entries = anotherMap.headMap(cutoff, true);
        Entry<Long, Data> entry = null;
        // export the globally sorted commit packet
        while ((entry = entries.pollFirstEntry()) != null) {
            System.out.println(entry);
        }
    }

    // Try to consolidate two maps, if there is gap between maps,
    // return the map with greater keys for back pressure tracking.
    private TreeMap<Long, Data>
    consolidate(TreeMap<Long, Data> map, TreeMap<Long, Data> anotherMap)
    {
        try {
            mutex.acquire(CLIENT_NUM);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        try {
            if (map.isEmpty() || anotherMap.isEmpty()) {
                // Ends back pressure
                if (hasBackPressure.get()) {
                    hasBackPressure.set(false);
                    flowControl.release();
                }
                return null;
            }
            // Two maps overlap with each other
            if (map.lastKey() < anotherMap.lastKey() && map.lastKey() >= anotherMap.firstKey()) {
                doConsolidate(map, anotherMap);
                // Ends back pressure
                if (hasBackPressure.get()) {
                    hasBackPressure.set(false);
                    flowControl.release();
                }
                return null;
            } else if (anotherMap.lastKey() < map.lastKey() && anotherMap.lastKey() >= map.firstKey()) {
                doConsolidate(anotherMap, map);
                // Ends back pressure
                if (hasBackPressure.get()) {
                    hasBackPressure.set(false);
                    flowControl.release();
                }
                return null;
            }
            // There is gap between two maps, return the map with greater keys
            if (map.lastKey() < anotherMap.firstKey()) {
                return anotherMap;
            } else if (anotherMap.lastKey() < map.firstKey()) {
                return map;
            }
            // Shouldn't enter here
            return null;
        } finally {
            mutex.release(CLIENT_NUM);
        }
    }

    /*
     * For each queue, read N packets
     */
    private void collect()
    {
        List<TreeMap<Long, Data>> sortTrees = new ArrayList<>();
        List<Deque<Data>> buffers = new ArrayList<>();
        Set<Long> toBeMatched = new HashSet<Long>();
        // initialize book keeping data structures
        for (int i = 0; i < CLIENT_NUM; i++) {
            buffers.add(new ArrayDeque<Data>());
            sortTrees.add(new TreeMap<Long, Data>());
        }

        for (int i = 0; i < CLIENT_NUM; i++) {
            final int index = i;
            Thread receiver = new Thread(() -> {
                Deque<Data> stream = buffers.get(index);
                TreeMap<Long, Data> sortedData = sortTrees.get(index);
                while (true) {
                    boolean shouldStop = false;
                    int bufferReads = 0;
                    while (!shouldStop) {
                        Data packet = null;
                        try {
                            packet = queue.get(index).take();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        bufferReads++;
                        System.out.println("From stream " + index + ": " + packet);
                        stream.add(packet);
                        if (bufferReads >= PREREAD_THRESHOLD) {
                            Data data = null;
                            while((data = stream.pollFirst()) != null) {
                                // save the handle for prepare packet
                                if (data.kind.equals("prepare")) {
                                    toBeMatched.add(data.prepare);
                                }
                                if (data.kind.equals("commit")) {
                                    // remove matching pair from the set
                                    if (toBeMatched.contains(data.prepare)) {
                                        toBeMatched.remove(data.prepare);
                                    }
                                    try {
                                        mutex.acquire();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                        System.exit(-1);
                                    }
                                    // use treemap to sort
                                    sortedData.put(data.commit, data);
                                    mutex.release();
                                }
                            }
                            // Detect orphaned prepare packets, keep reading from stream
                            if (toBeMatched.size() == 0) {
                                shouldStop = true;
                            }
                        }
                    }
                    TreeMap<Long, Data> map = consolidate(sortTrees.get(0), sortTrees.get(1));
                    if (map == sortedData) {
                        try {
                            // Back pressure starts, pause current thread.
                            hasBackPressure.set(true);
                            flowControl.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                }
            }, "receiver " + index);
            receiver.start();
        }
    }


    static public void main(String[] args) {
        DataCollector dc = new DataCollector();
        CountDownLatch latch = new CountDownLatch(2 * CLIENT_NUM + 1);


        for (int i = 0; i < CLIENT_NUM; i++) {
            final int index = i;
            Thread sender = new Thread(() -> {
                dc.generateData(index);
                latch.countDown();
            });
            Thread anotherSender = new Thread(() -> {
                dc.generateData(index);
                latch.countDown();
            });
            sender.start();
            anotherSender.start();
        }

        Thread receiver = new Thread(() -> {
            dc.collect();
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
