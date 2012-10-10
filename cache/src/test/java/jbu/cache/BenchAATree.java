package jbu.cache;

import jbu.offheap.Allocator;
import jbu.serializer.unsafe.UnsafePrimitiveBeanSerializer;
import jbu.testobject.LotOfBoolean;
import jbu.testobject.LotOfPrimitiveAndArrayAndString;
import org.junit.Test;

public class BenchAATree {
    @Test
    public void bench_put_get() {

        // Put n object in map
        // Get them all
        // Remove them
        // etc...
        int NB_OBJ = 1000000;
        int NB_ITER = 10000;

        long putTime = 0;
        long put = 0;
        long getTime = 0;
        long get = 0;

        Allocator allocator = new Allocator(1024l * 1024l * 1024l);
        OffheapAATree<LotOfBoolean> cache = new OffheapAATree<>(allocator);
        LotOfBoolean cachedObject = new LotOfBoolean();
        int estimSize = new UnsafePrimitiveBeanSerializer().calculateSerializedSize(cachedObject);
        long objectSizeInMemory = ((long) estimSize * (long) NB_OBJ) / 1024l / 1024l;
        System.out.println("Need to cache  : " + objectSizeInMemory + " MB");
        System.out.println("Store : " + NB_OBJ);
        Node<LotOfBoolean> root = null;
        for (int j = 0; j < NB_ITER; j++) {
            long start = System.nanoTime();
            for (long i = 0; i < NB_OBJ; i++) {
                root = cache.put(root, i, cachedObject);
            }
            putTime += System.nanoTime() - start;
            put += NB_OBJ;

            start = System.nanoTime();
            for (long i = 0; i < NB_OBJ; i++) {
                cache.get(root, i);
            }
            getTime += System.nanoTime() - start;
            get += NB_OBJ;


            System.out.println("Iteration : " + j);
            double getTimeSecond = getTime / (double) (1000 * 1000 * 1000);
            double putTimeSecond = putTime / (double) (1000 * 1000 * 1000);


            System.out.println("Memory allocated : " + allocator.getAllocatedMemory() / 1024 / 1024 + " MB");
            System.out.println("Memory used : " + allocator.getUsedMemory() / 1024 / 1024 + " MB");
            System.out.println("Puts : " + put / putTimeSecond + " object/s");
            System.out.println("Gets : " + get / getTimeSecond + " object/s");
            System.out.println("Puts : " + (put * estimSize / 1024 / 1024) / putTimeSecond + " MB/s");
            System.out.println("Gets : " + (get * estimSize / 1024 / 1024) / getTimeSecond + " MB/s");
            System.out.println("");
            System.out.println("");
            cache.clean();
            root = null;
        }


    }
}
