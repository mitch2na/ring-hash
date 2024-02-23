package com.nick;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashRing<K, V> {
    private static final long POSITIVE_HASH_WINDOW = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE + 1L;
    private final RingNode<K, V> EMPTY_RING_NODE = new RingNode<>("", "");
    private final TreeSet<RingEntry<RingNode<K, V>>> ringEntries;
    private final int nodeWeight;

    private final Map<String, List<RingEntry<RingNode<K, V>>>> nodes;
    private int nodeSize;
    private long dataCount;


    public HashRing(int nodeSize, int nodeWeight) {
        this.nodeWeight = nodeWeight;
        this.nodeSize = nodeSize;
        this.nodes = new HashMap<>();
        List<String> newNodes = IntStream.range(this.nodeSize, nodeSize).sequential().mapToObj(it -> "Node " + it).collect(Collectors.toList());
        this.ringEntries = createInitialNodes(newNodes);
    }

    public void addNode(int nodeSize) {
        List<String> newNodes = IntStream.range(this.nodeSize, nodeSize).sequential().mapToObj(it -> "Node " + it).collect(Collectors.toList());
        this.nodeSize += nodeSize;
        addNewNodes(newNodes);
    }

    public void addNode(String nodeName) {
        this.nodeSize += 1;
        addNewNodes(Collections.singletonList(nodeName));
    }

    public void removeNode(String nodeName) {
        final List<RingEntry<RingNode<K, V>>> nodes = this.nodes.get(nodeName);
        for (final RingEntry<RingNode<K, V>> node : nodes) {
            RingEntry<RingNode<K, V>> ringEntryHigher = ringEntries.higher(node);
            if (ringEntryHigher == null) {
                ringEntryHigher = ringEntries.first();
            }
            RingNode<K, V> ringNode = ringEntryHigher.getData();
            for (final Map.Entry<K, RingEntry<V>> data : node.getData().getData().entrySet()) {
                ringNode.put(data.getKey(), data.getValue());
            }
            ringEntries.remove(node);
            Iterator<Map.Entry<K, RingEntry<V>>> iterator = ringNode.getData().entrySet().iterator();
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
        RingEntry<RingNode<K, V>> ringEntry = ringEntries.higher(new RingEntry<>(hash, angle, EMPTY_RING_NODE));
        if (ringEntry == null) {
            ringEntry = ringEntries.first();
        }
        // System.out.println("Node assigned: " + ringEntry.toString());
        return ringEntry.getData().put(key, new RingEntry<>("Data " + key, hash, angle, value)).getData();
    }

    private TreeSet<RingEntry<RingNode<K, V>>> createInitialNodes(List<String> newNodes) {
        TreeSet<RingEntry<RingNode<K, V>>> ringEntries = new TreeSet<>();
        double incrementBy = 360D / (double) nodeWeight;
        for (String node : newNodes) {
            String nodeName = "Node" + node;
            List<RingEntry<RingNode<K, V>>> nodeWeights = new LinkedList<>();
            this.nodes.put(nodeName, nodeWeights);
            double max = incrementBy;
            for (int j = 0; j < nodeWeight; j++) {
                String nodeNameWeight = nodeName + j;
                double min = (max - incrementBy);
                RingEntry<RingNode<K, V>> nodeRingEntry = createNodeRingEntry(ringEntries, nodeName, max, min, nodeNameWeight);
                ringEntries.add(nodeRingEntry);
                nodeWeights.add(nodeRingEntry);
                max += incrementBy;
            }
        }
        return ringEntries;
    }

    private RingEntry<RingNode<K, V>> createNodeRingEntry(TreeSet<RingEntry<RingNode<K, V>>> ringEntries, String nodeName,
                                                          double max, double min, String nodeNameWeight) {
        RingEntry<RingNode<K, V>> vnodeEntry;
        double angle = -1;
        do {
            if (angle != -1) {
                System.out.println("Clash with angle " + angle + ", trying again.");
            }
            angle = ThreadLocalRandom.current().nextDouble(min, max);
            vnodeEntry = new RingEntry<>(nodeNameWeight, UUID.randomUUID().toString().hashCode(), angle,
                    new RingNode<>(nodeName, nodeNameWeight));
        } while (ringEntries.contains(vnodeEntry));
        return vnodeEntry;
    }

    private void addNewNodes(List<String> newNodes) {
        int moveOperations = 0;
        double startRange = 360D / (double) nodeWeight;
        for (String nodeName : newNodes) {
            List<RingEntry<RingNode<K, V>>> nodeWeights = new LinkedList<>();
            this.nodes.put(nodeName, nodeWeights);
            double max = startRange;
            for (int j = 0; j < nodeWeight; j++) {
                String nodeNameWeight = nodeName + j;
                double min = (max - startRange);
                RingEntry<RingNode<K, V>> nodeRingEntry = createNodeRingEntry(ringEntries, nodeName, max, min, nodeNameWeight);
                RingEntry<RingNode<K, V>> ringEntryHigher = ringEntries.higher(nodeRingEntry);
                boolean isFirst = false;
                if (ringEntryHigher == null) {
                    ringEntryHigher = ringEntries.first();
                } else {
                    isFirst = ringEntryHigher.getAngle() == ringEntries.first().getAngle();
                }
                RingNode<K, V> ringNode = ringEntryHigher.getData();
                /*
                 * TODO: this portion can be optimized further since its using a linked list we dont keep order of the data entries.
                 *  Faster retrieval could be used by using a binary search for this current vnode's angle
                 *  and every data entry below it.
                 */
                Iterator<Map.Entry<K, RingEntry<V>>> iterator = ringNode.getData().entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<K, RingEntry<V>> ringEntry = iterator.next();
                    double nodeAngle = nodeRingEntry.getAngle();
                    double entryAngle = ringEntry.getValue().getAngle();
                    if (nodeAngle >= entryAngle ||
                            (isFirst && entryAngle > ringEntryHigher.getAngle())) {
                        moveOperations = moveOperations + 1;
                        nodeRingEntry.getData().put(ringEntry.getKey(), ringEntry.getValue());
                        iterator.remove();
                    }
                }
                ringEntries.add(nodeRingEntry);
                nodeWeights.add(nodeRingEntry);
                max += startRange;
            }
        }
        System.out.println("Moved around " + ((double) moveOperations / (double) dataCount) * 100D + "%");
    }

    static class RingNode<K, V> {
        private final String node;
        private final String name;
        private final Map<K, RingEntry<V>> data = new HashMap<>();

        private RingNode(String node, String name) {
            this.node = node;
            this.name = name;
        }

        public String getNode() {
            return node;
        }

        public String getName() {
            return name;
        }

        public Map<K, RingEntry<V>> getData() {
            return data;
        }

        public RingEntry<V> put(K key, RingEntry<V> data) {
            return this.data.put(key, data);
        }

        public void remove(K key) {
            this.data.remove(key);
        }

        public int size() {
            return data.size();
        }

    }


    static class RingEntry<V> implements Comparable<RingEntry<V>> {
        private final String name;
        private final int hash;
        private final double angle;
        private final V data;

        public RingEntry(String name, int hash, double angle, V data) {
            this.name = name;
            this.hash = hash;
            this.angle = angle;
            this.data = data;
        }

        public RingEntry(int hash, double angle, V data) {
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
        public int compareTo(RingEntry o) {
            return Double.compare(this.angle, o.getAngle());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RingEntry<?> ringEntry = (RingEntry<?>) o;
            return Double.compare(angle, ringEntry.angle) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(angle);
        }
    }
}
