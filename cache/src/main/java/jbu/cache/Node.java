package jbu.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node {

    private static Logger LOGGER = LoggerFactory.getLogger(Node.class);

    private static final Boolean LOGGER_IS_TRACE_ENABLED = LOGGER.isTraceEnabled();

    public long addr;
    public long leftnode;
    public long rightnode;
    public long rootnode;
    public long level;
    public long key;
    public long valueAddr;

    public Node(long addr, long leftnode, long rightnode, long rootnode, long level, long key, long valueAddr) {
        this.addr = addr;
        this.leftnode = leftnode;
        this.rightnode = rightnode;
        this.rootnode = rootnode;
        this.level = level;
        this.key = key;
        this.valueAddr = valueAddr;
    }

    public Node(long addr, long key, long valueAddr) {
        this.addr = addr;
        this.key = key;
        this.valueAddr = valueAddr;
        this.leftnode = -1;
        this.rightnode = -1;
        this.rootnode = -1;
        this.level = 1;
    }

    public Node() {

    }
}