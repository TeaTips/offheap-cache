package jbu.cache;

import jbu.exception.CannotDeserializeException;
import jbu.offheap.Allocator;
import jbu.offheap.LoadContext;
import jbu.offheap.StoreContext;
import jbu.serializer.unsafe.UnsafePrimitiveBeanSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node<V> {

    private static Logger LOGGER = LoggerFactory.getLogger(Node.class);

    private static final Boolean LOGGER_IS_TRACE_ENABLED = LOGGER.isTraceEnabled();

    private static UnsafePrimitiveBeanSerializer SERIALIZER = new UnsafePrimitiveBeanSerializer();

    private Allocator allocator;
    private long addr;
    private long leftnode = -1;
    private long rightnode = -1;
    private long rootnode = -1;
    private long level;
    private long key;
    private long value;

    public Node(Allocator allocator) {
        this.allocator = allocator;
        addr = this.allocator.alloc();
        saveNode(this.allocator.getStoreContext(addr), this);
    }

    /**
     * Used by serializer. Don't use it *
     */
    public Node() {
    }

    public Node(Allocator allocator, int level, Long key, V value) {
        this.allocator = allocator;
        this.level = level;
        setKey(key);
        setValue(value);
        addr = this.allocator.alloc();
        saveNode(allocator.getStoreContext(addr), this);
    }


    /**
     * Get Deserialized key
     *
     * @return
     */
    public Long getKey() {
        try {
            return (Long) SERIALIZER.deserialize(allocator.getLoadContext(key));
        } catch (CannotDeserializeException e) {
            LOGGER.error("Cannot deserialize node", e);
            return null;
        }
    }

    /**
     * Get Deserialized value
     *
     * @return
     */
    public V getValue() {
        try {
            return (V) SERIALIZER.deserialize(allocator.getLoadContext(value));
        } catch (CannotDeserializeException e) {
            LOGGER.error("Cannot deserialize node", e);
            return null;
        }
    }

    private void setKey(Long deserKey) {
        this.key = allocator.alloc();
        SERIALIZER.serialize(deserKey, allocator.getStoreContext(this.key));
    }

    private void setValue(V deserValue) {
        this.value = allocator.alloc();
        SERIALIZER.serialize(deserValue, allocator.getStoreContext(this.value));
    }

    private static Node loadNode(LoadContext loadContext, Node rootnode) {
        try {
            Node node = (Node) SERIALIZER.deserialize(loadContext);
            if (LOGGER_IS_TRACE_ENABLED) {
                LOGGER.trace("node_loaded: {}", node);
            }
            node.allocator = rootnode.allocator;
            return node;
        } catch (CannotDeserializeException e) {
            LOGGER.error("Cannot deserialize node", e);
            return null;
        }
    }

    private static void saveNode(StoreContext storeContext, Node node) {
        SERIALIZER.serialize(node, storeContext);
    }

    Node getLeftNode() {
        if (leftnode >= 0) {
            return loadNode(allocator.getLoadContext(leftnode), this);
        } else {
            return null;
        }
    }

    long getLeftNodeAddr() {
        return leftnode;
    }

    Node getRightNode() {
        if (rightnode >= 0) {
            return loadNode(allocator.getLoadContext(rightnode), this);
        } else {
            return null;
        }
    }

    long getRightNodeAddr() {
        return rightnode;
    }

    Node getRootNode() {
        if (rootnode >= 0) {
            return loadNode(allocator.getLoadContext(rootnode), this);
        } else {
            return null;
        }
    }

    long getRootNodeAddr() {
        return rootnode;
    }

    long getAddr() {
        return addr;
    }

    long getLevel() {
        return level;
    }

    void setLevel(long level) {
        this.level = level;
        saveNode(allocator.getStoreContext(this.addr), this);
    }

    void setLeftNode(long leftNodeAddr) {
        this.leftnode = leftNodeAddr;
        saveNode(allocator.getStoreContext(this.addr), this);
    }

    void setRightNode(long rightNodeAddr) {
        this.rightnode = rightNodeAddr;
        saveNode(allocator.getStoreContext(this.addr), this);
    }

    void setRootNode(long rootnode) {
        this.rootnode = rootnode;
        saveNode(allocator.getStoreContext(this.addr), this);
    }

    @Override
    public String toString() {
        return "Node{" +
                ", addr=" + addr +
                ", leftnode=" + leftnode +
                ", rightnode=" + rightnode +
                ", rootnode=" + rootnode +
                ", level=" + level +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}