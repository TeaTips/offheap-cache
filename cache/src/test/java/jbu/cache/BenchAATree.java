package jbu.cache;

import jbu.offheap.Allocator;
import jbu.serializer.unsafe.UnsafePrimitiveBeanSerializer;
import jbu.testobject.LotOfBoolean;
import jbu.testobject.LotOfPrimitiveAndArrayAndString;
import org.junit.Test;

import java.util.Random;

public class BenchAATree {
    @Test
    public void bench_put_get() {

        // Put n object in map
        // Get them all
        // Remove them
        // etc...
        int NB_OBJ = 50000;
        int NB_ITER = 10000;

        long putTime = 0;
        long put = 0;
        long getTime = 0;
        long get = 0;

        Allocator allocatorTree = new Allocator(3000l * 1024l * 1024l, 80);
        Allocator allocatorObject = new Allocator(500l * 1024l * 1024l, 32);
        OffheapAATree<Long> cache = new OffheapAATree<>(allocatorTree, allocatorObject);
        Long cachedObject = new Long(0);
        int estimSize = new UnsafePrimitiveBeanSerializer().calculateSerializedSize(cachedObject);
        long objectSizeInMemory = ((long) estimSize * (long) NB_OBJ) / 1024l / 1024l;
        System.out.println("Need to cache  : " + objectSizeInMemory + " MB");
        System.out.println("Store : " + NB_OBJ);
        for (int j = 0; j < NB_ITER; j++) {
            long start = System.nanoTime();
            for (long i = 0; i < NB_OBJ; i++) {
                cache.put(i, i);
            }
            putTime += System.nanoTime() - start;
            put += NB_OBJ;

            start = System.nanoTime();
            //print_tree(cache);
            for (long i = 0; i < NB_OBJ; i++) {
                cache.get(i);
            }
            getTime += System.nanoTime() - start;
            get += NB_OBJ;


            System.out.println("Iteration : " + j);
            double getTimeSecond = getTime / (double) (1000 * 1000 * 1000);
            double putTimeSecond = putTime / (double) (1000 * 1000 * 1000);


            System.out.println("Memory allocated : " + allocatorTree.getAllocatedMemory() / 1024 / 1024 + " MB");
            System.out.println("Memory used : " + allocatorTree.getUsedMemory() / 1024 / 1024 + " MB");
            System.out.println("Puts : " + put / putTimeSecond + " object/s");
            System.out.println("Gets : " + get / getTimeSecond + " object/s");
            System.out.println("Puts : " + (putTime / 1000) / put + " us per insert");
            System.out.println("Gets : " + (getTime / 1000) / put + " us per get");
            System.out.println("Puts : " + (put * estimSize / 1024 / 1024) / putTimeSecond + " MB/s");
            System.out.println("Gets : " + (get * estimSize / 1024 / 1024) / getTimeSecond + " MB/s");
            System.out.println("");
            System.out.println("");
            cache.clean();
        }


    }

    @Test
    public void bench_random_put() {

        // Put n object in map
        // Get them all
        // Remove them
        // etc...
        int NB_OBJ = 100000;
        int NB_ITER = 5;

        long putTime = 0;
        long put = 0;

        Allocator allocator = new Allocator(2000l * 1024l * 1024l);
        OffheapAATree<LotOfPrimitiveAndArrayAndString> cache = new OffheapAATree<>(allocator, allocator);
        LotOfPrimitiveAndArrayAndString cachedObject = new LotOfPrimitiveAndArrayAndString();
        int estimSize = new UnsafePrimitiveBeanSerializer().calculateSerializedSize(cachedObject);
        long objectSizeInMemory = ((long) estimSize * (long) NB_OBJ) / 1024l / 1024l;
        System.out.println("Need to cache  : " + objectSizeInMemory + " MB");
        System.out.println("Store : " + NB_OBJ);
        Random r = new Random();
        for (int j = 0; j < NB_ITER; j++) {
            long start = System.nanoTime();
            for (long i = 0; i < NB_OBJ; i++) {
                cache.put(r.nextLong(), cachedObject);
            }
            putTime += System.nanoTime() - start;
            put += NB_OBJ;

            System.out.println("Iteration : " + j);
            double putTimeSecond = putTime / (double) (1000 * 1000 * 1000);


            System.out.println("Memory allocated : " + allocator.getAllocatedMemory() / 1024 / 1024 + " MB");
            System.out.println("Memory used : " + allocator.getUsedMemory() / 1024 / 1024 + " MB");
            System.out.println("Puts : " + put / putTimeSecond + " object/s");
            System.out.println("Puts : " + (putTime / 1000) / put + " us per insert");
            System.out.println("Puts : " + (put * estimSize / 1024 / 1024) / putTimeSecond + " MB/s");
            System.out.println("");
            cache.clean();
        }


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
