package jbu.cache;

import jbu.offheap.Allocator;
import jbu.offheap.LoadContext;
import jbu.offheap.StoreContext;

public class Node {

    private Allocator allocator;
    private long addr;
    private long leftnode;
    private long rightnode;
    private long level;
    private long key;
    private long value;

    public Node(Allocator allocator) {
        this.allocator = allocator;
        addr = this.allocator.alloc();
        saveNode(this.allocator.getStoreContext(addr), this);
    }

    private static void loadNode(LoadContext loadContext, Node node) {
    }

    private static void saveNode(StoreContext storeContext, Node node) {
    }

    Node getLeftNode() {
        Node left = new Node(allocator);
        loadNode(allocator.getLoadContext(leftnode), left);
        return left;
    }

    Node getRightNode() {
        Node right = new Node(allocator);
        loadNode(allocator.getLoadContext(rightnode), right);
        return right;
    }

    long getAddr() {
        return addr;
    }

    long getLevel() {
        return level;
    }

    void setLevel(long level) {
        this.level = level;
    }

    void setLeftNode(long leftNodeAddr) {
        this.leftnode = leftNodeAddr;
    }

    void setRightNode(long rightNodeAddr) {
        this.rightnode = rightNodeAddr;
    }
}