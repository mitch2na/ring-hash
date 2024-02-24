package com.nick;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashRingMap<K, V> implements Map<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashRingMap.class);
    private static final long POSITIVE_HASH_WINDOW = (long) Integer.MAX_VALUE + (long) Integer.MAX_VALUE + 1L;
    private final Node<K, V> EMPTY_NODE = new Node<>(null, null);
    private final TreeSet<Entry<Node<K, V>>> ringEntries;
    /**
     * node weight to apply (aka how distributed each node will be on the ring)
     */
    private final int nodeWeight;

    private final Map<String, List<Entry<Node<K, V>>>> nodes;
    private int nodeSize;
    private long dataCount;


    public HashRingMap(int nodeSize, int nodeWeight) {
        this.nodeWeight = nodeWeight;
        this.nodeSize = nodeSize;
        this.nodes = new HashMap<>();
        this.ringEntries = new TreeSet<>();
        List<String> newNodes = IntStream.range(0, nodeSize).sequential().mapToObj(it -> "Node " + it).collect(Collectors.toList());
        createInitialNodes(newNodes);
    }

    public void addNode(int nodeSize) {
        List<String> newNodes = IntStream.range(this.nodeSize, this.nodeSize + nodeSize).sequential().mapToObj(it -> "Node " + it).collect(Collectors.toList());
        addNewNodes(newNodes);

    }

    public void addNode(String nodeName) {
        addNewNodes(Collections.singletonList(nodeName));
    }

    public void removeNode(String nodeName) {
        final List<Entry<Node<K, V>>> vnodes = this.nodes.get(nodeName);
        if (vnodes == null) {
            return;
        }
        for (final Entry<Node<K, V>> vnode : vnodes) {
            Entry<Node<K, V>> entryHigher = getNodeEntry(vnode);
            Node<K, V> nodeHigherData = entryHigher.getData();
            for (final Map.Entry<K, Entry<V>> data : vnode.getData().getData().entrySet()) {
                nodeHigherData.put(data.getKey(), data.getValue());
            }
            ringEntries.remove(vnode);
        }
    }


    @Override
    public int size() {
        return (int) dataCount;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        double angle = getAngle(key);
        Entry<Node<K, V>> nodeEntry = getNodeEntry(new Entry<>(angle, EMPTY_NODE));
        return nodeEntry.getData().getData().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        double angle = getAngle(key);
        Entry<Node<K, V>> nodeEntry = getNodeEntry(new Entry<>(angle, EMPTY_NODE));
        Entry<V> vEntry = nodeEntry.getData().getData().get(key);
        if (vEntry != null) {
            return vEntry.getData();
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        double angle = getAngle(key);
        Entry<Node<K, V>> nodeEntry = getNodeEntry(new Entry<>(angle, EMPTY_NODE));
        // System.out.println("Node assigned: " + ringEntry.toString());
        Node<K, V> data = nodeEntry.getData();
        Entry<V> put = data.put(key, new Entry<>("Data " + key, angle, value));
        dataCount += 1;
        if (put != null) {
            return put.getData();
        }
        return null;
    }


    @Override
    public V remove(Object key) {
        double angle = getAngle(key);
        Entry<Node<K, V>> nodeEntry = getNodeEntry(new Entry<>(angle, EMPTY_NODE));
        Entry<V> remove = nodeEntry.getData().getData().remove(key);
        if (remove != null) {
            return remove.getData();
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {
        for (Entry<Node<K, V>> ringEntry : ringEntries) {
            ringEntry.getData().getData().clear();
        }
        dataCount = 0;
    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return null;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return null;
    }

    TreeSet<Entry<Node<K, V>>> getRingEntries() {
        return this.ringEntries;
    }

    private Entry<Node<K, V>> getNodeEntry(Entry<Node<K, V>> hash) {
        Entry<Node<K, V>> entry = ringEntries.higher(hash);
        if (entry == null) {
            entry = ringEntries.first();
        }
        return entry;
    }

    private double getAngle(Object key) {
        int hash = key.hashCode();
        long hashLong = hash;
        // we need to adjust the hash to be a positive value by converting the - / + into a long window of +
        if (hash == Integer.MIN_VALUE)
            hashLong = (long) Integer.MAX_VALUE + ((long) Integer.MAX_VALUE + 1L);
        else if (hash < 0)
            hashLong = (long) Integer.MAX_VALUE + Math.abs(hash);

        return ((double) hashLong / (double) POSITIVE_HASH_WINDOW) * 360D;
    }

    /**
     * Create the initial nodes into the hash ring. Takes the weight and randomly assigns each vnode with a window range
     * throughout the circle.
     *
     * @param newNodes - actual nodes in the cluster
     */
    private void createInitialNodes(List<String> newNodes) {
        double incrementBy = 360D / (double) nodeWeight;
        for (String node : newNodes) {
            List<Entry<Node<K, V>>> nodeWeights = new LinkedList<>();
            this.nodes.put(node, nodeWeights);
            double max = incrementBy;
            for (int j = 0; j < nodeWeight; j++) {
                String nodeNameWeight = node + "-" + j;
                double min = (max - incrementBy);
                Entry<Node<K, V>> nodeEntry = createNodeRingEntry(node, nodeNameWeight, max, min);
                ringEntries.add(nodeEntry);
                nodeWeights.add(nodeEntry);
                max += incrementBy;
            }
        }
    }

    private Entry<Node<K, V>> createNodeRingEntry(String node, String vnode, double max, double min) {
        Entry<Node<K, V>> vnodeEntry;
        double angle = -1;
        do {
            if (angle != -1) {
                LOGGER.warn("Clash with angle " + angle + ", trying again.");
            }
            angle = ThreadLocalRandom.current().nextDouble(min, max);
            vnodeEntry = new Entry<>(vnode, angle, new Node<>(node, vnode));
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
                Entry<Node<K, V>> nodeEntry = createNodeRingEntry(nodeName, nodeNameWeight, max, min);
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
        this.nodeSize += newNodes.size();
        LOGGER.info("Moved around " + ((double) moveOperations / (double) dataCount) * 100D + "%");
    }

    static class Node<K, V> {
        private final String node;
        private final String vnode;
        private final Map<K, Entry<V>> data = new HashMap<>();

        private Node(String node, String vnode) {
            this.node = node;
            this.vnode = vnode;
        }

        public String getVnode() {
            return vnode;
        }

        public String getNode() {
            return node;
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
        private final double angle;
        private final V data;

        public Entry(String name, double angle, V data) {
            this.name = name;
            this.angle = angle;
            this.data = data;
        }

        public Entry(double angle, V data) {
            this(null, angle, data);
        }

        public String getName() {
            return name;
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
