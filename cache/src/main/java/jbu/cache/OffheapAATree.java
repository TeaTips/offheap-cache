package jbu.cache;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import jbu.exception.CannotDeserializeException;
import jbu.offheap.Allocator;
import jbu.serializer.unsafe.UnsafePrimitiveBeanSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class OffheapAATree<V> {

    private static Logger LOGGER = LoggerFactory.getLogger(OffheapAATree.class);

    private static final Boolean LOGGER_IS_TRACE_ENABLED = LOGGER.isTraceEnabled();

    private UnsafePrimitiveBeanSerializer serializer = new UnsafePrimitiveBeanSerializer();

    private final Allocator allocatorObject;
    private final Allocator allocatorTree;

    private Node rootNode;

    private Map<Long, Node> nodeCache;

    public OffheapAATree(Allocator allocatorTree, Allocator allocatorObject) {
        this.allocatorTree = allocatorTree;
        this.allocatorObject = allocatorObject;
        nodeCache = new LinkedHashMap<Long, Node>(10000) {

            private static final int MAX_ENTRY = 10000;

            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Node> eldest) {
                return size() > MAX_ENTRY;
            }
        };
    }

    // ** public API **//
    public Node put(long key, V value) {
        long valueAddr = allocatorObject.alloc();
        serializer.serialize(value, allocatorObject.getStoreContext(valueAddr));
        this.rootNode = put(this.rootNode, key, valueAddr);
        return this.rootNode;
    }

    public V get(long key) {
        try {
            return (V) serializer.deserialize(allocatorObject.getLoadContext(get(rootNode, key)));
        } catch (CannotDeserializeException e) {
            LOGGER.error("Cannot deserialize.", e);
            return null;
        }
    }

    public Node getRootNode() {
        return rootNode;
    }

    Node loadNode(long addr) {
        try {
            Node node = (Node) this.serializer.deserialize(allocatorTree.getLoadContext(addr));
            if (LOGGER_IS_TRACE_ENABLED) {
                LOGGER.trace("node_loaded: {}", node);
            }
            return node;
        } catch (CannotDeserializeException e) {
            LOGGER.error("Cannot deserialize node", e);
            return null;
        }
    }

    void saveNode(Node node) {
        nodeCache.put(node.addr, node);
        this.serializer.serialize(node, allocatorTree.getStoreContext(node.addr));
    }

    Node getLeftNode(Node node) {
        if (node.leftnode >= 0) {
            Node left;
            if ((left = this.nodeCache.get(node.leftnode)) == null) {
                left = loadNode(node.leftnode);
                nodeCache.put(left.addr, left);
            }
            return left;

        } else {
            return null;
        }
    }

    Node getRightNode(Node node) {
        if (node.rightnode >= 0) {
            Node right;
            if ((right = this.nodeCache.get(node.rightnode)) == null) {
                right = loadNode(node.rightnode);
                nodeCache.put(right.addr, right);
            }
            return right;
        } else {
            return null;
        }
    }

    Node getRootNode(Node node) {
        if (node.rootnode >= 0) {
            Node root;
            if ((root = this.nodeCache.get(node.rootnode)) == null) {
                root = loadNode(node.rootnode);
                nodeCache.put(root.addr, root);
            }
            return root;
        } else {
            return null;
        }
    }

    Node skew(Node node) {
        if (node == null) {
            return null;
        } else if (getLeftNode(node) == null) {
            return node;
        } else if (getLeftNode(node).level == node.level) {
            //Swap the pointers of horizontal left links.
            Node nodeToSwap = getLeftNode(node);
            // inv root node
            Node rootNode = getRootNode(node);
            node.rootnode = nodeToSwap.addr;
            if (rootNode != null) {
                nodeToSwap.rootnode = rootNode.addr;
                // Change children of root node
                if (rootNode.leftnode == node.addr) {
                    rootNode.leftnode = nodeToSwap.addr;
                } else {
                    rootNode.rightnode = nodeToSwap.addr;
                }
                saveNode(rootNode);
            } else {
                nodeToSwap.rootnode = -1;
            }

            // Exchange right node
            Node rightChildOfSwap = getRightNode(nodeToSwap);
            if (rightChildOfSwap != null) {
                node.leftnode = rightChildOfSwap.addr;
                rightChildOfSwap.rootnode = node.addr;
                saveNode(rightChildOfSwap);
            } else {
                node.leftnode = -1;
            }
            nodeToSwap.rightnode = node.addr;
            // node modified : nodeToSwap, node, rightChildOfSwap
            saveNode(nodeToSwap);
            saveNode(node);
            return nodeToSwap;
        } else {
            return node;
        }
    }

    Node split(Node node) {
        if (node == null) {
            return null;
        } else if (getRightNode(node) == null || getRightNode(getRightNode(node)) == null) {
            return node;
        } else if (node.level == getRightNode(getRightNode(node)).level) {
            //We have two horizontal right links.  Take the middle node, elevate it, and return it.
            Node nodeToElevate = getRightNode(node);
            // change root
            Node rootNode = getRootNode(node);
            if (rootNode != null) {
                nodeToElevate.rootnode = rootNode.addr;
                if (rootNode.leftnode == node.addr) {
                    rootNode.leftnode = nodeToElevate.addr;
                } else {
                    rootNode.rightnode = nodeToElevate.addr;
                }
                saveNode(rootNode);
            } else {
                nodeToElevate.rootnode = -1;
            }

            nodeToElevate.level = nodeToElevate.level + 1;

            Node nodeToSwap = getLeftNode(nodeToElevate);
            if (nodeToSwap != null) {
                node.rightnode = nodeToSwap.addr;
                nodeToSwap.rootnode = node.addr;
                saveNode(nodeToSwap);
            } else {
                node.rightnode = -1;
            }

            nodeToElevate.leftnode = node.addr;
            node.rootnode = nodeToElevate.addr;
            // node modified : nodeToElevate, node, nodeToSwap

            saveNode(node);
            saveNode(nodeToElevate);
            return nodeToElevate;
        } else {
            return node;
        }
    }

    private Node put(Node rootNode, long key, long value) {
        Node previousNode = null;
        boolean left = true;
        while (rootNode != null) {
            if (key < rootNode.key) {
                previousNode = rootNode;
                rootNode = getLeftNode(rootNode);
                left = true;
            } else if (key > rootNode.key) {
                previousNode = rootNode;
                rootNode = getRightNode(rootNode);
                left = false;
            } else {
                // replace case
                return rootNode;
            }
        }
        // Find place to take
        Node newNode = new Node(allocatorTree.alloc(), -1, -1, -1, 1, key, value);
        if (previousNode != null) {
            newNode.rootnode = previousNode.addr;
            if (left) {
                previousNode.leftnode = newNode.addr;
            } else {
                previousNode.rightnode = newNode.addr;
            }
        }
        // save new node and previous
        saveNode(newNode);
        if (previousNode != null) {
            saveNode(previousNode);
        }

        rootNode = newNode;
        // Now split and skew until root
        // goback to root with split(skew(node)) each time
        split(skew(rootNode));
        while (getRootNode(rootNode) != null) {
            split(skew(getRootNode(rootNode)));
            if (rootNode.rootnode >= 0) {
                rootNode = getRootNode(rootNode);
            } else {
                break;
            }
        }
        return rootNode;
    }

    private long get(Node rootNode, long key) {
        if (rootNode.key == key) {
            return rootNode.valueAddr;
        } else if (rootNode.key < key && rootNode.rightnode >= 0) {
            return get(getRightNode(rootNode), key);
        } else if (rootNode.key > key && rootNode.leftnode >= 0) {
            return get(getLeftNode(rootNode), key);
        }
        // not find
        return -1;
    }

    private Node delete(Node rootNode, Node newNode) {
        return null;
    }

    private Node decreaseLevel(Node node) {
        return null;
    }

    void clean() {
        nodeCache.clear();
        allocatorTree.freeAll();
        allocatorObject.freeAll();
        this.rootNode = null;
    }
}

