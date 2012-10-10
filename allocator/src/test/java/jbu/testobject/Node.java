package jbu.testobject;

public class Node {
    private long addr;
    private long leftnode;
    private long rightnode;
    private long level;
    private long key;
    private long value;

    public Node(long addr, long leftnode, long rightnode, long level, long key, long value) {
        this.addr = addr;
        this.leftnode = leftnode;
        this.rightnode = rightnode;
        this.level = level;
        this.key = key;
        this.value = value;
    }

    public Node() {
    }

    @Override
    public String toString() {
        return "Node{" +
                "addr=" + addr +
                ", leftnode=" + leftnode +
                ", rightnode=" + rightnode +
                ", level=" + level +
                ", key=" + key +
                ", value=" + value +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        if (addr != node.addr) return false;
        if (key != node.key) return false;
        if (leftnode != node.leftnode) return false;
        if (level != node.level) return false;
        if (rightnode != node.rightnode) return false;
        if (value != node.value) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (addr ^ (addr >>> 32));
        result = 31 * result + (int) (leftnode ^ (leftnode >>> 32));
        result = 31 * result + (int) (rightnode ^ (rightnode >>> 32));
        result = 31 * result + (int) (level ^ (level >>> 32));
        result = 31 * result + (int) (key ^ (key >>> 32));
        result = 31 * result + (int) (value ^ (value >>> 32));
        return result;
    }
}
