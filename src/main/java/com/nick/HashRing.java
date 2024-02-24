package com.nick;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashRing<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashRing.class);
    private static final long POSITIVE_HASH_WINDOW = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE + 1L;
    private final Node<K, V> EMPTY_RING_NODE = new Node<>("", "");
    private final TreeSet<Entry<Node<K, V>>> ringEntries;
    /**
     * node weight to apply (aka how distributed each node will be on the ring)
     */
    private final int nodeWeight;

    private final Map<String, List<Entry<Node<K, V>>>> nodes;
    private int nodeSize;
    private long dataCount;


    public HashRing(int nodeSize, int nodeWeight) {
        this.nodeWeight = nodeWeight;
        this.nodeSize = nodeSize;
        this.nodes = new HashMap<>();
        List<String> newNodes = IntStream.range(0, nodeSize).sequential().mapToObj(it -> "Node " + it).collect(Collectors.toList());
        this.ringEntries = createInitialNodes(newNodes);
    }

    public void addNode(int nodeSize) {
        List<String> newNodes = IntStream.range(this.nodeSize, this.nodeSize + nodeSize).sequential().mapToObj(it -> "Node " + it).collect(Collectors.toList());
        this.nodeSize += nodeSize;
        addNewNodes(newNodes);
    }

    public void addNode(String nodeName) {
        this.nodeSize += 1;
        addNewNodes(Collections.singletonList(nodeName));
    }

    public void removeNode(String nodeName) {
        final List<Entry<Node<K, V>>> nodes = this.nodes.get(nodeName);
        if (nodes == null) {
            return;
        }
        for (final Entry<Node<K, V>> node : nodes) {
            Entry<Node<K, V>> entryHigher = ringEntries.higher(node);
            if (entryHigher == null) {
                entryHigher = ringEntries.first();
            }
            Node<K, V> ringNode = entryHigher.getData();
            for (final Map.Entry<K, Entry<V>> data : node.getData().getData().entrySet()) {
                ringNode.put(data.getKey(), data.getValue());
            }
            ringEntries.remove(node);
        }
    }


    public V put(K key, V value) {
        dataCount += 1;
        int hash = key.hashCode();
        long hashLong = hash;
        // we need to adjust the hash to be a positive value by converting the - / + into a long window of +
        if (hash == Integer.MIN_VALUE)
            hashLong = (long) Integer.MAX_VALUE + ((long) Integer.MAX_VALUE + 1L);
        else if (hash < 0)
            hashLong = (long) Integer.MAX_VALUE + Math.abs(hash);

        double angle = ((double) hashLong / (double) POSITIVE_HASH_WINDOW) * 360D;
        Entry<Node<K, V>> entry = ringEntries.higher(new Entry<>(hash, angle, EMPTY_RING_NODE));
        if (entry == null) {
            entry = ringEntries.first();
        }
        // System.out.println("Node assigned: " + ringEntry.toString());
        Node<K, V> data = entry.getData();
        Entry<V> put = data.put(key, new Entry<>("Data " + key, hash, angle, value));
        if (put != null) {
            return put.getData();
        }
        return null;
    }

    TreeSet<Entry<Node<K, V>>> getRingEntries() {
        return this.ringEntries;
    }

    /**
     * Create the initial nodes into the hash ring. Takes the weight and randomly assigns each vnode with a window range
     * throughout the circle.
     *
     * @param newNodes - actual nodes in the cluster
     */
    private TreeSet<Entry<Node<K, V>>> createInitialNodes(List<String> newNodes) {
        TreeSet<Entry<Node<K, V>>> ringEntries = new TreeSet<>();
        double incrementBy = 360D / (double) nodeWeight;
        for (String node : newNodes) {
            List<Entry<Node<K, V>>> nodeWeights = new LinkedList<>();
            this.nodes.put(node, nodeWeights);
            double max = incrementBy;
            for (int j = 0; j < nodeWeight; j++) {
                String nodeNameWeight = node + "-" + j;
                double min = (max - incrementBy);
                Entry<Node<K, V>> nodeEntry = createNodeRingEntry(ringEntries, node, max, min, nodeNameWeight);
                ringEntries.add(nodeEntry);
                nodeWeights.add(nodeEntry);
                max += incrementBy;
            }
        }
        return ringEntries;
    }

    private Entry<Node<K, V>> createNodeRingEntry(TreeSet<Entry<Node<K, V>>> ringEntries, String nodeName,
                                                  double max, double min, String nodeNameWeight) {
        Entry<Node<K, V>> vnodeEntry;
        double angle = -1;
        do {
            if (angle != -1) {
                LOGGER.warn("Clash with angle " + angle + ", trying again.");
            }
            angle = ThreadLocalRandom.current().nextDouble(min, max);
            vnodeEntry = new Entry<>(nodeNameWeight, UUID.randomUUID().toString().hashCode(), angle,
                    new Node<>(nodeName, nodeNameWeight));
        } while (ringEntries.contains(vnodeEntry));
        return vnodeEntry;
    }

    /**
     * This is the beauty of the hash ring! You dont need to adjust all the vnodes since each new vnode will only
     * take the entries from next highest vnode that is less than or equal to itself.
     *
     * @param newNodes - new node names
     */
    private void addNewNodes(List<String> newNodes) {
        int moveOperations = 0;
        double startRange = 360D / (double) nodeWeight;
        for (String nodeName : newNodes) {
            List<Entry<Node<K, V>>> nodeWeights = new LinkedList<>();
            this.nodes.put(nodeName, nodeWeights);
            double max = startRange;
            for (int j = 0; j < nodeWeight; j++) {
                String nodeNameWeight = nodeName + "-" + j;
                double min = (max - startRange);
                Entry<Node<K, V>> nodeEntry = createNodeRingEntry(ringEntries, nodeName, max, min, nodeNameWeight);
                Entry<Node<K, V>> ringEntryHigher = ringEntries.higher(nodeEntry);
                boolean isFirst = false;
                if (ringEntryHigher == null) {
                    ringEntryHigher = ringEntries.first();
                } else {
                    isFirst = ringEntryHigher.getAngle() == ringEntries.first().getAngle();
                }
                Node<K, V> node = ringEntryHigher.getData();
                /*
                 * TODO: this portion can be optimized further since its using a linked list we dont keep order of the data entries.
                 *  Faster retrieval could be used by using a binary search for this current vnode's angle
                 *  and every data entry below it.
                 */
                Iterator<Map.Entry<K, Entry<V>>> iterator = node.getData().entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<K, Entry<V>> ringEntry = iterator.next();
                    double nodeAngle = nodeEntry.getAngle();
                    double entryAngle = ringEntry.getValue().getAngle();
                    if (nodeAngle >= entryAngle ||
                            (isFirst && entryAngle > ringEntryHigher.getAngle())) {
                        moveOperations = moveOperations + 1;
                        nodeEntry.getData().put(ringEntry.getKey(), ringEntry.getValue());
                        iterator.remove();
                    }
                }
                ringEntries.add(nodeEntry);
                nodeWeights.add(nodeEntry);
                max += startRange;
            }
        }
        LOGGER.info("Moved around " + ((double) moveOperations / (double) dataCount) * 100D + "%");
    }

    static class Node<K, V> {
        private final String node;
        private final String name;
        private final Map<K, Entry<V>> data = new HashMap<>();

        private Node(String node, String name) {
            this.node = node;
            this.name = name;
        }

        public String getNode() {
            return node;
        }

        public String getName() {
            return name;
        }

        public Map<K, Entry<V>> getData() {
            return data;
        }

        public Entry<V> put(K key, Entry<V> data) {
            return this.data.put(key, data);
        }

        public void remove(K key) {
            this.data.remove(key);
        }

        public int size() {
            return data.size();
        }

    }


    static class Entry<V> implements Comparable<Entry<V>> {
        private final String name;
        private final int hash;
        private final double angle;
        private final V data;

        public Entry(String name, int hash, double angle, V data) {
            this.name = name;
            this.hash = hash;
            this.angle = angle;
            this.data = data;
        }

        public Entry(int hash, double angle, V data) {
            this(null, hash, angle, data);
        }

        public String getName() {
            return name;
        }

        public int getHash() {
            return hash;
        }

        public double getAngle() {
            return angle;
        }

        public V getData() {
            return data;
        }

        @Override
        public String toString() {
            return "name='" + name + '\'' +
                    ", hash=" + hash +
                    ", angle=" + angle;
        }

        @Override
        public int compareTo(Entry o) {
            return Double.compare(this.angle, o.getAngle());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry<?> entry = (Entry<?>) o;
            return Double.compare(angle, entry.angle) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(angle);
        }
    }
}
