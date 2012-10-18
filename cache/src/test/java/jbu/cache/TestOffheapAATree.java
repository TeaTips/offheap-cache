package jbu.cache;

import jbu.offheap.Allocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestOffheapAATree {
    @Test
    public void put_node_should_be_get() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        tree.put(1, "");
        assertEquals("", tree.get(1));
    }

    @Test
    public void put_two_node_should_be_get() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        tree.put(1, "1");
        tree.put(2, "2");
        assertEquals("2", tree.get(2));
    }

    @Test
    public void skew_null_node_should_be_null() {
        OffheapAATree tree = new OffheapAATree(null, null);
        Node res = tree.skew(null);
        assertNull(res);
    }

    @Test
    public void skew_node_with_left_null_should_return_same_node() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);

        Node l = new Node(all.alloc(), -1, -1);
        Node res = tree.skew(l);
        assertEquals(l.addr, res.addr);
    }

    @Test
    public void skew_node_with_left_node() {
        // Test example of Wikipedia http://en.wikipedia.org/wiki/AA_tree
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node l = new Node(all.alloc(), -1, -1);
        Node a = new Node(all.alloc(), -1, -1);
        Node b = new Node(all.alloc(), -1, -1);
        Node t = new Node(all.alloc(), -1, -1);
        Node r = new Node(all.alloc(), -1, -1);

        // a b r are leaf
        a.rootnode = l.addr;
        b.rootnode = l.addr;
        r.rootnode = t.addr;
        // l and t are upper level
        l.level = 2;
        t.level = 2;
        l.rootnode = t.addr;

        // l have two children a and b
        l.leftnode = a.addr;
        l.rightnode = b.addr;

        // l is leftnode of t
        t.leftnode = l.addr;
        // r is rightnode of t
        t.rightnode = r.addr;

        tree.saveNode(l);
        tree.saveNode(a);
        tree.saveNode(b);
        tree.saveNode(t);
        tree.saveNode(r);

        Node res = tree.skew(t);

        // the returned node should be L
        assertEquals(l.addr, res.addr);
        l = res;
        // L right node should be T
        assertEquals(l.rightnode, t.addr);
        t = tree.getRightNode(l);
        // L left node should be A
        assertEquals(l.leftnode, a.addr);
        a = tree.getRightNode(l);
        // T left node should be B
        assertEquals(t.leftnode, b.addr);
        b = tree.getLeftNode(t);
        // T right node should be R
        assertEquals(t.rightnode, r.addr);
        r = tree.getRightNode(t);
        // T rootnode is L
        assertEquals(t.rootnode, l.addr);
        // B rootnode is T
        assertEquals(b.rootnode, t.addr);
        // R rootnode is T
        assertEquals(r.rootnode, t.addr);
        // A rootnode is L
        assertEquals(a.rootnode, l.addr);
        // L has no rootnode
        assertEquals(l.rootnode, -1);
    }

    @Test
    public void basic_split() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node a = new Node(all.alloc(), 1, -1);
        Node b = new Node(all.alloc(), 2, -1);
        Node c = new Node(all.alloc(), 3, -1);
        Node d = new Node(all.alloc(), 4, -1);


        a.level = 2;
        a.rightnode = b.addr;
        b.rootnode = a.addr;
        b.rightnode = c.addr;
        c.rootnode = b.addr;
        c.rightnode = d.addr;
        d.rootnode = c.addr;

        tree.saveNode(a);
        tree.saveNode(b);
        tree.saveNode(c);
        tree.saveNode(d);

        Node newRoot = tree.split(b);

        assertNotNull(newRoot);

        assertEquals(newRoot.key, 3l);

        assertEquals(tree.getLeftNode(newRoot).key, 2l);
        assertEquals(tree.getLeftNode(newRoot).rootnode, newRoot.addr);

        assertEquals(tree.getRightNode(newRoot).key, 4l);
        assertEquals(tree.getRightNode(newRoot).rootnode, newRoot.addr);

        assertEquals(tree.getRootNode(newRoot).key, 1l);
        assertEquals(tree.getRootNode(newRoot).rightnode, newRoot.addr);
    }

    @Test
    public void split_null_node_should_return_null() {
        OffheapAATree tree = new OffheapAATree(null,null);
        Node res = tree.split(null);
        assertNull(res);
    }

    @Test
    public void split_node_with_null_rigth_node_should_return_same() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node l = new Node(all.alloc(), -1, -1);
        l.level = 1;
        Node res = tree.split(l);
        assertEquals(l.addr, res.addr);
    }

    @Test
    public void split_node_with_rigth_of_rigth_null_node_should_return_same() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node l = new Node(all.alloc(), -1, -1);
        Node r = new Node(all.alloc(), -1, -1);
        l.level = 1;
        r.level = 2;
        l.rightnode = r.addr;

        tree.saveNode(l);
        tree.saveNode(r);

        Node res = tree.split(l);
        assertEquals(l.addr, res.addr);
    }

    @Test
    public void split_node_need_to_be_rebalanced() {
        // Test example of Wikipedia http://en.wikipedia.org/wiki/AA_tree
        Allocator all = new Allocator(1l * 1024l * 1024l);
        Node t = new Node(all.alloc(), -1, -1);
        Node r = new Node(all.alloc(), -1, -1);
        Node x = new Node(all.alloc(), -1, -1);
        Node a = new Node(all.alloc(), -1, -1);
        Node b = new Node(all.alloc(), -1, -1);
        // a b are leaf
        a.level = 1;
        b.level = 1;
        a.rootnode = t.addr;
        b.rootnode = r.addr;
        x.rootnode = r.addr;
        r.rootnode = t.addr;

        // r,x and t are upper level
        r.level = 2;
        t.level = 2;
        x.level = 2;
        // l have two children a and b
        t.leftnode = a.addr;
        r.leftnode = b.addr;

        // l is leftnode of t
        t.rightnode = r.addr;
        // r is rightnode of t
        r.rightnode = x.addr;

        OffheapAATree tree = new OffheapAATree(all, all);
        tree.saveNode(t);
        tree.saveNode(r);
        tree.saveNode(x);
        tree.saveNode(a);
        tree.saveNode(b);

        Node res = tree.split(t);

        // the returned node should be r
        assertEquals(r.addr, res.addr);
        r = res;
        // t is left of r
        assertEquals(t.addr, res.leftnode);
        t = tree.getLeftNode(r);
        // a is left of t
        assertEquals(a.addr, t.leftnode);
        a = tree.getLeftNode(t);
        // b is right of t
        assertEquals(b.addr, t.rightnode);
        b = tree.getRightNode(t);
        // x is right of r
        assertEquals(x.addr, res.rightnode);
        x = tree.getRightNode(r);

        // r rootnode is empty
        assertEquals(r.rootnode, -1);
        // t rootnode is r
        assertEquals(t.rootnode, r.addr);
        // a rootnode is t
        assertEquals(a.rootnode, t.addr);
        // b rootnode is t
        assertEquals(b.rootnode, t.addr);
        // x rootnode is r
        assertEquals(x.rootnode, r.addr);
    }

    @Test
    public void insert_in_null_node_create_a_new_node() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree<String> tree = new OffheapAATree<String>(all, all);
        Node res = tree.put(1l, "");

        assertNotNull(res);
        // Check key of node is 1
        assertEquals(1l, res.key);
    }

    @Test
    public void insert_with_key_inf_should_inserted_at_left() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(10l, "");
        Node newRoot = tree.put(5l, "");
        // before assert the tree should be rebalanced...

        assertNotNull(newRoot);
        // Check root not change
        assertEquals(5l, newRoot.key);

        // Check left node is null
        assertNull(tree.getLeftNode(newRoot));

        // Check right node is not null
        assertNotNull(tree.getRightNode(newRoot));

        // Check right node is the new node
        assertEquals(10l, tree.getRightNode(newRoot).key);

        // Check level of new node is 1
        assertEquals(1, tree.getRightNode(newRoot).level);
    }

    @Test
    public void insert_with_key_sup_should_inserted_at_right() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(10l, "");
        Node newRoot = tree.put(15l, "");
        assertNotNull(newRoot);
        // Check root not change
        assertEquals(firstNode.addr, newRoot.addr);

        // Check level of root is 2
        // assertEquals(2, newRoot.getLevel());

        // Check right node is not null
        assertNotNull(tree.getRightNode(newRoot));

        // Check right node is the new node
        assertEquals(15l, tree.getRightNode(newRoot).key);

        // Check level of new node is 1
        assertEquals(1, tree.getRightNode(newRoot).level);
    }

    @Test
    public void insert_two_node_at_right() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(10l, "1");
        Node newRoot = tree.put(15l, "2");
        newRoot = tree.put(20l, "3");

        assertNotNull(newRoot);
        // root should be 15
        assertEquals(15l, newRoot.key);

        // with level 2
        assertEquals(2, newRoot.level);

        // with left 5 and level 1
        assertEquals(10l, tree.getLeftNode(newRoot).key);
        assertEquals(1, tree.getLeftNode(newRoot).level);

        // with right 20 and level 1
        assertEquals(20l, tree.getRightNode(newRoot).key);
        assertEquals(1, tree.getRightNode(newRoot).level);

    }

    @Test
    public void insert_three_node_at_left() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(10l, "1");
        Node newRoot = tree.put(9l, "2");
        newRoot = tree.put(8l, "3");

        assertNotNull(newRoot);
        // root should be 15
        assertEquals(9l, newRoot.key);

        // with level 2
        assertEquals(2, newRoot.level);

        // with left 5 and level 1
        assertEquals(8l, tree.getLeftNode(newRoot).key);
        assertEquals(1, tree.getLeftNode(newRoot).level);

        // with right 20 and level 1
        assertEquals(10, tree.getRightNode(newRoot).key);
        assertEquals(1, tree.getRightNode(newRoot).level);

    }

    @Test
    public void insert_four_node_at_left() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(10l, "1");
        Node newRoot = tree.put(9l, "2");
        newRoot = tree.put(8l, "3");
        newRoot = tree.put(7l, "4");

        print_tree(tree);

        assertNotNull(newRoot);
        // root should be 15
        assertEquals(9l, newRoot.key);

        // with level 2
        assertEquals(2, newRoot.level);

        // with left 7 and level 1
        assertEquals(7l, tree.getLeftNode(newRoot).key);
        assertEquals(1, tree.getLeftNode(newRoot).level);

        // with right 20 and level 1
        assertEquals(10l, tree.getRightNode(newRoot).key);
        assertEquals(1, tree.getRightNode(newRoot).level);

    }

    @Test
    public void insert_three_node() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(10l, "1");
        Node newRoot = tree.put(5l, "2");
        newRoot = tree.put(20l, "3");

        assertNotNull(newRoot);
        // root should be 10
        assertEquals(10l, newRoot.key);

        // with level 2
        assertEquals(2, newRoot.level);

        // with left 5 and level 1
        assertEquals(5l, tree.getLeftNode(newRoot).key);
        assertEquals(1, tree.getLeftNode(newRoot).level);

        // with right 20 and level 1
        assertEquals(20l, tree.getRightNode(newRoot).key);
        assertEquals(1, tree.getRightNode(newRoot).level);

    }

    @Test
    public void insert_five_node() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(10l, "1");
        Node newRoot = tree.put(5l, "2");
        newRoot = tree.put(20l, "3");
        newRoot = tree.put(30l, "4");
        newRoot = tree.put(4l, "5");

        print_tree(tree);

        assertNotNull(newRoot);
        // root should be 10
        assertEquals(10l, newRoot.key);

        // with level 2
        assertEquals(2, newRoot.level);

        // with left 5 and level 1
        assertEquals(4l, tree.getLeftNode(newRoot).key);
        assertEquals(1, tree.getLeftNode(newRoot).level);

        // with right 20 and level 1
        assertEquals(20l, tree.getRightNode(newRoot).key);
        assertEquals(1, tree.getRightNode(newRoot).level);

    }

    @Test
    public void insert_four_node_right() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node firstNode = tree.put(1l, "1");
        Node newRoot = tree.put(2l, "2");
        newRoot = tree.put(3l, "3");
        newRoot = tree.put(4l, "4");
        newRoot = tree.put(5l, "5");

        print_tree(tree);

        assertNotNull(newRoot);
        // root should be 10
        assertEquals(2l, newRoot.key);

        // with level 2
        assertEquals(2, newRoot.level);

        // with left 5 and level 1
        assertEquals(1l, tree.getLeftNode(newRoot).key);
        assertEquals(1, tree.getLeftNode(newRoot).level);

        // with right 20 and level 1
        assertEquals(4l, tree.getRightNode(newRoot).key);
        assertEquals(2, tree.getRightNode(newRoot).level);

    }

    @Test
    public void test_with_some_node() {
        Allocator all = new Allocator(1l * 1024l * 1024l);
        OffheapAATree tree = new OffheapAATree(all, all);
        Node newRoot = tree.put(10l, "1");
        newRoot = tree.put(15l, "2");
        newRoot = tree.put(20l, "3");
        newRoot = tree.put(25l, "4");
        newRoot = tree.put(30l, "5");
        newRoot = tree.put(35l, "6");
        newRoot = tree.put(40l, "7");
        newRoot = tree.put(5l, "8");
        newRoot = tree.put(0l, "9");
        newRoot = tree.put(45l, "10");
        newRoot = tree.put(50l, "11");

        assertNotNull(newRoot);
        assertEquals("9", tree.get(0l));
    }

    private void print_tree(OffheapAATree tree) {
        Node rootNode = tree.getRootNode();
        print_tree(tree, rootNode);
    }

    private void print_tree(OffheapAATree tree, Node rootnode) {
        System.out.println("l:" + rootnode.level + " k:" + rootnode.key);
        if (rootnode.leftnode >= 0) {
            System.out.print("At left ");
            print_tree(tree, tree.getLeftNode(rootnode));
        }
        if (rootnode.rightnode >= 0) {
            System.out.print("At right ");
            print_tree(tree, tree.getRightNode(rootnode));
        }
    }
}
