package com.nick;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HashRingTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashRingTest.class);

    @Test
    public void testRingHash() {
        int nodeWeight = 500;
        int dataCount = 1_000_000;
        final HashRing<String, String> hashRing = new HashRing<>(10, nodeWeight);
        for (int i = 0; i < dataCount; i++) {
            String uuid = UUID.randomUUID().toString();
            hashRing.put(uuid, null);
        }
        assertNodesAndData(dataCount, hashRing);
        calcStats(hashRing);
        hashRing.addNode(2);
        calcStats(hashRing);
        assertNodesAndData(dataCount, hashRing);
        String firstNodeName = hashRing.getRingEntries().first().getName();
        String nodeToRemove = firstNodeName.substring(0, firstNodeName.indexOf("-"));
        hashRing.removeNode(nodeToRemove);
        assertNodesAndData(dataCount, hashRing);
        firstNodeName = hashRing.getRingEntries().last().getName();
        nodeToRemove = firstNodeName.substring(0, firstNodeName.indexOf("-"));
        hashRing.removeNode(nodeToRemove);
        assertNodesAndData(dataCount, hashRing);
        printLayout(hashRing);
    }


    /**
     * Assert that the ring hash has all the data properly stored in each appropriate vnode.
     *
     * @param total
     * @param hashRing
     */
    private void assertNodesAndData(int total, HashRing<String, String> hashRing) {
        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicBoolean isFirstNode = new AtomicBoolean(true);
        TreeSet<HashRing.Entry<HashRing.Node<String, String>>> ringEntries = hashRing.getRingEntries();
        ringEntries.forEach(it -> {
            HashRing.Node<String, String> node = it.getData();
            node.getData().entrySet().forEach(it2 -> {
                double nodeAngle = it.getAngle();
                assertTrue(nodeAngle >= 0D && nodeAngle <= 360D);
                double dataAngle = it2.getValue().getAngle();
                assertTrue(dataAngle >= 0D && dataAngle <= 360D);
                // assert is different for first node since it can encompass behind itself on the ring
                if (isFirstNode.get()) {
                    if (dataAngle > nodeAngle) {
                        HashRing.Entry<HashRing.Node<String, String>> lastNode = ringEntries.last();
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
     * @param hashRing
     */
    private void calcStats(HashRing<String, String> hashRing) {
        Map<String, Integer> entries = new HashMap<>();
        TreeSet<HashRing.Entry<HashRing.Node<String, String>>> ringEntries = hashRing.getRingEntries();
        for (HashRing.Entry<HashRing.Node<String, String>> ringEntry : ringEntries) {
            HashRing.Node<String, String> vnode = ringEntry.getData();
            String node = vnode.getNode();
            Integer count = entries.get(node);
            if (count != null) {
                entries.put(node, count + vnode.size());
            } else {
                entries.put(node, vnode.size());
            }
        }
        List<Double> numbers = ringEntries.stream().map(HashRing.Entry::getAngle).collect(Collectors.toList());
        LOGGER.info("RSD of nodes on ring " + calculateRSD(numbers));
        numbers = entries.entrySet().stream().map((entry) -> {
            LOGGER.info("Node: " + entry.getKey() + " has count " + entry.getValue());
            return (double) entry.getValue();
        }).collect(Collectors.toList());
        LOGGER.info("RSD of data between nodes " + calculateRSD(numbers));
    }


    private void printLayout(HashRing<String, String> hashRing) {
        for (HashRing.Entry<?> entry : hashRing.getRingEntries()) {
            LOGGER.info("Entry: " + entry.toString());
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

}
