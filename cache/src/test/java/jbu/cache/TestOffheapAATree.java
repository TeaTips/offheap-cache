package jbu.cache;

import jbu.offheap.Allocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestOffheapAATree {

    @Test
    public void skew_null_node_should_be_null() {
        OffheapAATree tree = new OffheapAATree();
        Node res = tree.skew(null);
        assertNull(res);
    }

    @Test
    public void skew_node_with_left_null_should_return_same_node() {
        OffheapAATree tree = new OffheapAATree();
        Allocator all = new Allocator(1l * 1024l * 1024l);
        Node l = new Node(all);
        Node res = tree.skew(l);
        assertEquals(l, res);
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

        OffheapAATree tree = new OffheapAATree();
        Node res = tree.skew(t);

        // the returned node should be L
        assertEquals(l.getAddr(), res.getAddr());
        // L right node should be T
        assertEquals(l.getRightNode().getAddr(), r.getAddr());
        // L left node should be A
        assertEquals(l.getLeftNode().getAddr(), a.getAddr());
        // T left node should be B
        assertEquals(t.getLeftNode().getAddr(), b.getAddr());
        // T right node should be R
        assertEquals(t.getRightNode().getAddr(), r.getAddr());
    }

}
