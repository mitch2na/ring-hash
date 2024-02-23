package com.nick;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RingHashTest {

    private static final long POSITIVE_HASH_WINDOW = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE + 1L;

    @Test
    public void testRingHash() {

        int nodeWeight = 500;
        int dataCount = 10_000_000;
        char[] nodes = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P'};
        char[] newNodes = {'Q'};
        TreeSet<RingEntry<?>> ringEntries = createInitialNodes(nodes, nodeWeight);
        int total = addDataToRing(dataCount, ringEntries);
        assertNodesAndData(total, ringEntries);
        calcStats(ringEntries);

        addNewNodes(newNodes, nodeWeight, dataCount, ringEntries);
        calcStats(ringEntries);
        assertNodesAndData(total, ringEntries);
        printLayout(ringEntries);
    }

    /**
     * Create the initial nodes into the hash ring. Takes the weight and randomly assigns each vnode with a window range
     * throughout the circle.
     *
     * @param nodes       - actual nodes in the cluster
     * @param nodeWeight  - node weight to apply (aka how distributed each node will be on the ring)
     */
    private TreeSet<RingEntry<?>> createInitialNodes(char[] nodes, int nodeWeight) {
        TreeSet<RingEntry<?>> ringEntries = new TreeSet<>();
        double incrementBy = 360D / (double) nodeWeight;
        for (char c : nodes) {
            String nodeName = "Node" + c;
            double max = incrementBy;
            for (int j = 0; j < nodeWeight; j++) {
                String nodeNameWeight = nodeName + j;
                double min = (max - incrementBy);
                RingEntry<RingNode> nodeRingEntry = createNodeRingEntry(ringEntries, nodeName, max, min, nodeNameWeight);
                ringEntries.add(nodeRingEntry);
                max += incrementBy;
            }
        }
        return ringEntries;
    }



    /**
     * Assert that the ring hash has all the data properly stored in each appropriate vnode.
     *
     * @param total
     * @param ringEntries
     */
    private void assertNodesAndData(int total, TreeSet<RingEntry<?>> ringEntries) {
        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicBoolean isFirstNode = new AtomicBoolean(true);
        ringEntries.forEach(it -> {
            RingNode ringNode = (RingNode) it.getData();
            ringNode.getData().forEach(it2 -> {
                double nodeAngle = it.getAngle();
                assertTrue(nodeAngle >= 0D && nodeAngle <= 360D);
                double dataAngle = it2.getAngle();
                assertTrue(dataAngle >= 0D && dataAngle <= 360D);
                // assert is different for first node since it can encompass behind itself on the ring
                if (isFirstNode.get()) {
                    if (dataAngle > nodeAngle) {
                        RingEntry<?> lastNode = ringEntries.last();
                        assertTrue("Node :" + it + "\n Data:" + it2, dataAngle > lastNode.getAngle());
                    } else {
                        assertTrue("Node :" + it + "\n Data:" + it2, nodeAngle >= dataAngle);
                    }
                } else {
                    assertTrue("Node :" + it + "\n Data:" + it2, nodeAngle >= dataAngle);
                }
                totalCount.set(totalCount.get() + 1);
            });
            isFirstNode.set(false);
        });
        assertEquals(totalCount.get(), total);
    }


    /**
     * Print out some stats of our ring.
     * <p>
     * 1) Node count
     * 2) Relative standard deviation of the nodes.
     *
     * @param ringEntries
     */
    private void calcStats(TreeSet<RingEntry<?>> ringEntries) {
        Map<String, Integer> entries = new HashMap<>();
        for (RingEntry<?> ringEntry : ringEntries) {
            if (ringEntry.getData() instanceof RingNode) {
                RingNode ringNode = (RingNode) ringEntry.getData();
                String node = ringNode.getNode();
                Integer count = entries.get(node);
                if (count != null) {
                    entries.put(node, count + ringNode.size());
                } else {
                    entries.put(node, ringNode.size());
                }
                // System.out.println("Ring Node: " + ringNode.getName() + " has size " + ringNode.size());
            }
        }
        List<Double> numbers = ringEntries.stream().map(RingEntry::getAngle).collect(Collectors.toList());
        // System.out.println("RSD of nodes on ring " + calculateRSD(numbers));
        numbers = entries.entrySet().stream().map((entry) -> {
            System.out.println("Node: " + entry.getKey() + " has count " + entry.getValue());
            return (double) entry.getValue();
        }).collect(Collectors.toList());
        System.out.println("RSD of data between nodes " + calculateRSD(numbers));
    }

    /**
     * This is the beauty of the hash ring! You dont need to adjust all the vnodes since each new vnode will only
     * take the entries from next highest vnode that is less than or equal to itself.
     *
     * @param newNodes    - new node names
     * @param nodeWeight  - node weight to apply (aka how distributed this node will be on the ring)
     * @param dataCount   - used to print out the percentage of entries that had to be moved.
     * @param ringEntries
     */
    private void addNewNodes(char[] newNodes, int nodeWeight, int dataCount, TreeSet<RingEntry<?>> ringEntries) {
        int moveOperations = 0;
        double startRange = 360D / (double) nodeWeight;
        for (char c : newNodes) {
            String nodeName = "Node" + c;
            double max = startRange;
            for (int j = 0; j < nodeWeight; j++) {
                String nodeNameWeight = nodeName + j;
                double min = (max - startRange);
                RingEntry<RingNode> nodeRingEntry = createNodeRingEntry(ringEntries, nodeName, max, min, nodeNameWeight);
                RingEntry<?> ringEntryHigher = ringEntries.higher(nodeRingEntry);
                boolean isFirst = false;
                if (ringEntryHigher == null) {
                    ringEntryHigher = ringEntries.first();
                } else {
                    isFirst = ringEntryHigher.getAngle() == ringEntries.first().getAngle();
                }
                ringEntries.add(nodeRingEntry);
                RingNode ringNode = (RingNode) ringEntryHigher.getData();
                /*
                 * TODO: this portion can be optimized further since its using a linked list we dont keep order of the data entries.
                 *  Faster retrieval could be used by using a binary search for this current vnode's angle
                 *  and every data entry below it.
                 */
                Iterator<RingEntry<?>> iterator = ringNode.getData().iterator();
                while (iterator.hasNext()) {
                    RingEntry<?> ringEntry = iterator.next();
                    double nodeAngle = nodeRingEntry.getAngle();
                    double entryAngle = ringEntry.getAngle();
                    if (nodeAngle >= entryAngle ||
                            (isFirst && entryAngle > ringEntryHigher.getAngle())) {
                        moveOperations = moveOperations + 1;
                        nodeRingEntry.getData().add(ringEntry);
                        iterator.remove();
                    }
                }
                ringEntries.add(nodeRingEntry);
                max += startRange;
            }
        }
        System.out.println("Moved around " + ((double) moveOperations / (double) dataCount) * 100D + "%");
    }

    private int addDataToRing(int dataCount, TreeSet<RingEntry<?>> ringEntries) {
        int totalCountAdded = 0;
        for (int i = 0; i < dataCount; i++) {
            String uuid = UUID.randomUUID().toString();
            int hash = uuid.hashCode();
            long hashLong = hash;
            // we need to adjust the hash to be a positive value by converting the - / + into a long window of +
            if (hash == Integer.MIN_VALUE)
                hashLong = (long) Integer.MAX_VALUE + ((long) Integer.MAX_VALUE + 1L);
            else if (hash < 0)
                hashLong = (long) Integer.MAX_VALUE + Math.abs(hash);

            double angle = ((double) hashLong / (double) POSITIVE_HASH_WINDOW) * 360D;
            // add some clustered data based off the uuid, mimics same sys_id
            if (i % 100_000 == 0) {
                for (int j = 0; j < 5; j++) {
                    addDataEntry("Data " + i + " " + j, hash, angle, ringEntries);
                    totalCountAdded = totalCountAdded + 1;
                }
            }
            addDataEntry("Data " + i, hash, angle, ringEntries);
            totalCountAdded = totalCountAdded + 1;
        }
        return totalCountAdded;
    }

    private void addDataEntry(String dataName, int hash, double angle, TreeSet<RingEntry<?>> ringEntries) {
        RingEntry<Integer> ringEntry = new RingEntry<>(dataName, hash, angle, 1);
        // System.out.println("Data entry: " + ringEntry.toString());
        RingEntry<?> nodeEntry = ringEntries.higher(ringEntry);
        if (nodeEntry == null) {
            nodeEntry = ringEntries.first();
        }
        // System.out.println("Node assigned: " + ringEntry1.toString());
        if (nodeEntry.getData() instanceof RingNode) {
            RingNode ringNode = (RingNode) nodeEntry.getData();
            ringNode.add(ringEntry);
        }
    }

    private RingEntry<RingNode> createNodeRingEntry(TreeSet<RingEntry<?>> ringEntries, String nodeName,
                                                    double max, double min, String nodeNameWeight) {
        RingEntry<RingNode> vnodeEntry;
        double angle = -1;
        do {
            if (angle != -1) {
                System.out.println("Clash with angle " + angle + ", trying again.");
            }
            angle = ThreadLocalRandom.current().nextDouble(min, max);
            vnodeEntry = new RingEntry<>(nodeNameWeight, UUID.randomUUID().toString().hashCode(), angle,
                    new RingNode(nodeName, nodeNameWeight));
        } while (ringEntries.contains(vnodeEntry));
        return vnodeEntry;
    }

    private void printLayout(TreeSet<RingEntry<?>> ringEntries) {
        for (RingEntry<?> ringEntry : ringEntries) {
            System.out.println("Node: " + ringEntry.toString());
        }
    }

    public static double calculateRSD(List<Double> allNumbers) {
        double sum = 0.0, standardDeviation = 0.0;
        int length = allNumbers.size();

        for (double num : allNumbers) {
            sum += num;
        }

        double mean = sum / length;

        for (double num : allNumbers) {
            standardDeviation += Math.pow(num - mean, 2);
        }

        double sd = Math.sqrt(standardDeviation / length);
        double num = sd * 100;
        return num / Math.abs(mean);
    }


    private static class RingNode {
        private final String fNode;
        private final String fName;
        private final List<RingEntry<?>> fData = new LinkedList<>();

        private RingNode(String node, String name) {
            fNode = node;
            fName = name;
        }

        public String getNode() {
            return fNode;
        }

        public String getName() {
            return fName;
        }

        public List<RingEntry<?>> getData() {
            return fData;
        }

        public void add(RingEntry<?> data) {
            fData.add(data);
        }

        public void remove(RingEntry<?> data) {
            fData.remove(data);
        }

        public int size() {
            return fData.size();
        }

    }

    private static class RingEntry<T> implements Comparable<RingEntry<T>> {
        private final String fName;
        private final int fHash;
        private final double fAngle;
        private final T fData;

        public RingEntry(String name, int hash, double angle, T data) {
            fName = name;
            fHash = hash;
            fAngle = angle;
            fData = data;
        }

        public String getName() {
            return fName;
        }

        public int getHash() {
            return fHash;
        }

        public double getAngle() {
            return fAngle;
        }

        public T getData() {
            return fData;
        }

        @Override
        public String toString() {
            return "name='" + fName + '\'' +
                    ", hash=" + fHash +
                    ", angle=" + fAngle;
        }

        @Override
        public int compareTo(RingEntry o) {
            return Double.compare(this.fAngle, o.getAngle());
        }

    }
}
