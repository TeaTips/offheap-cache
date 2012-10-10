package jbu.cache;

import jbu.offheap.Allocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestOffheapAATree {

    @Test
    public void skew_null_node_should_be_null() {
        OffheapAATree tree = new OffheapAATree(null);
        Node res = tree.skew(null);
        assertNull(res);
    }

    @Test
    public void skew_node_with_left_null_should_return_same_node() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all);

        Node l = new Node(all);
        l.setLevel(1);
        Node res = tree.skew(l);
        assertEquals(l.getAddr(), res.getAddr());
    }

    @Test
    public void skew_node_with_left_node() {
        // Test example of Wikipedia http://en.wikipedia.org/wiki/AA_tree
        Allocator all = new Allocator(1l * 1024l * 1024l);
        Node l = new Node(all);
        Node a = new Node(all);
        Node b = new Node(all);
        Node t = new Node(all);
        Node r = new Node(all);
        // a b r are leaf
        a.setLevel(1);
        b.setLevel(1);
        r.setLevel(1);
        // l and t are upper level
        l.setLevel(2);
        t.setLevel(2);

        // l have two children a and b
        l.setLeftNode(a.getAddr());
        l.setRightNode(b.getAddr());

        // l is leftnode of t
        t.setLeftNode(l.getAddr());
        // r is rightnode of t
        t.setRightNode(r.getAddr());

        OffheapAATree tree = new OffheapAATree(all);
        Node res = tree.skew(t);

        // the returned node should be L
        assertEquals(l.getAddr(), res.getAddr());
        // L right node should be T
        assertEquals(res.getRightNode().getAddr(), t.getAddr());
        // L left node should be A
        assertEquals(res.getLeftNode().getAddr(), a.getAddr());
        // T left node should be B
        assertEquals(res.getRightNode().getLeftNode().getAddr(), b.getAddr());
        // T right node should be R
        assertEquals(res.getRightNode().getRightNode().getAddr(), r.getAddr());
    }

    @Test
    public void split_null_node_should_return_null() {
        OffheapAATree tree = new OffheapAATree(null);
        Node res = tree.split(null);
        assertNull(res);
    }

    @Test
    public void split_node_with_null_rigth_node_should_return_same() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all);
        Node l = new Node(all);
        l.setLevel(1);
        Node res = tree.split(l);
        assertEquals(l.getAddr(), res.getAddr());
    }

    @Test
    public void split_node_with_rigth_of_rigth_null_node_should_return_same() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all);
        Node l = new Node(all);
        Node r = new Node(all);
        l.setLevel(1);
        r.setLevel(2);
        l.setRightNode(r.getAddr());
        Node res = tree.split(l);
        assertEquals(l.getAddr(), res.getAddr());
    }

    @Test
    public void split_node_need_to_be_rebalanced() {
        // Test example of Wikipedia http://en.wikipedia.org/wiki/AA_tree
        Allocator all = new Allocator(1l * 1024l * 1024l);
        Node t = new Node(all);
        Node r = new Node(all);
        Node x = new Node(all);
        Node a = new Node(all);
        Node b = new Node(all);
        // a b are leaf
        a.setLevel(1);
        b.setLevel(1);

        // r,x and t are upper level
        r.setLevel(2);
        t.setLevel(2);
        x.setLevel(2);
        // l have two children a and b
        t.setLeftNode(a.getAddr());
        r.setLeftNode(b.getAddr());

        // l is leftnode of t
        t.setRightNode(r.getAddr());
        // r is rightnode of t
        r.setRightNode(x.getAddr());

        OffheapAATree tree = new OffheapAATree(all);
        Node res = tree.split(t);

        // the returned node should be L
        assertEquals(r.getAddr(), res.getAddr());
        // t is left of r
        assertEquals(t.getAddr(), res.getLeftNode().getAddr());
        // a is left of t
        assertEquals(a.getAddr(), res.getLeftNode().getLeftNode().getAddr());
        // b is right of t
        assertEquals(b.getAddr(), res.getLeftNode().getRightNode().getAddr());
        // x is right of r
        assertEquals(x.getAddr(), res.getRightNode().getAddr());
    }

    @Test
    public void insert_in_null_node_create_a_new_node() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all);
        Node res = tree.put(null, 1l, "");

        assertNotNull(res);
        // Check key of node is 1
        assertEquals(1l, res.getKey().longValue());
    }

    @Test
    public void insert_with_key_inf_should_inserted_at_left() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all);
        Node firstNode = tree.put(null, 10l, "");
        Node newRoot = tree.put(firstNode, 5l, "");
        assertNotNull(newRoot);
        // Check newRoot is last insertedNode
        assertEquals(5l, newRoot.getKey().longValue());

        // Check level is 2
        assertEquals(1l, newRoot.getLevel());

        // Check left node is null
        assertNull(newRoot.getLeftNode());

        // Check rigth node is firstnode
        assertEquals(10l, newRoot.getRightNode().getKey().longValue());
    }
}
