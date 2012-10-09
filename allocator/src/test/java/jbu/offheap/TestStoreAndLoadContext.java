package jbu.offheap;


import jbu.UnsafeReflection;
import jbu.UnsafeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestStoreAndLoadContext {

    @Test
    public void store_int() {
        Allocator a = new Allocator(256, 256);
        long addr = a.alloc(256);
        StoreContext sc = a.getStoreContext(addr);
        sc.storeInt(42);
        LoadContext lc = a.getLoadContext(addr);
        assertEquals(42, lc.loadInt());
    }

    @Test
    public void store_array_from_object() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(a.a, aprime.a);
    }

    @Test
    public void store_int_between_two_buffer_1() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        sc.storeInt(Integer.MAX_VALUE);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(Integer.MAX_VALUE, lc.loadInt());
    }

    @Test
    public void store_int_between_two_buffer_2() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        sc.storeInt(42);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(42, lc.loadInt());
    }

    @Test
    public void store_int_between_two_buffer_3() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        sc.storeInt(0);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(0, lc.loadInt());
    }

    @Test
    public void store_int_between_two_buffer_4() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        sc.storeInt(-42);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(-42, lc.loadInt());
    }



    @Test
    public void store_long_between_two_buffer() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        sc.storeLong(42l);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(42l, lc.loadLong());
    }

    @Test
    public void store_short_between_two_buffer() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 239);
        sc.storeShort((short)42);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 239);
        assertEquals(42, lc.loadShort());
    }

    @Test
    public void store_char_between_two_buffer() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 239);
        sc.storeChar('a');
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 239);
        assertEquals('a', lc.loadChar());
    }

    @Test
    public void store_float_between_two_buffer() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        sc.storeFloat(42f);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(42f, lc.loadFloat(), 0);
    }

    @Test
    public void store_double_between_two_buffer() throws NoSuchFieldException {
        Allocator alloc = new Allocator(1024, 256);
        long addr = alloc.alloc(256);
        StoreContext sc = alloc.getStoreContext(addr);
        A a = new A();
        sc.storeSomething(a.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        sc.storeDouble(42d);
        LoadContext lc = alloc.getLoadContext(addr);
        A aprime = new A();
        lc.loadArray(aprime.a, UnsafeReflection.arrayBaseOffset(a.a), 238);
        assertEquals(42d, lc.loadDouble(), 0);
    }


}

class A {
    public byte[] a = new byte[238];
}