package com.company;// sestoft@itu.dk * 2016-11-18, 2017-01-08

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class TestMSQueueNeater extends Tests {
    public static void main(String[] args) throws Exception {
        // Exercise 5.1
        MSQueueNeater<Integer> queue = new MSQueueNeater<Integer>();
//        sequentialTest(queue);
        // Exercise 5.2
        parallelTest(new MSQueueNeater<Integer>(), 10, 1000);
    }

    private static void parallelTest(MSQueueNeater<Integer> queue, int pairs, int trials) {
        final ExecutorService pool = Executors.newCachedThreadPool();
        new QueueTests(queue, pairs, trials).test(pool);
        pool.shutdown();
        System.out.println("%nparallelTest ... Passed!");
    }

    private static void sequentialTest(UnboundedQueue<Integer> queue) throws Exception {
        System.out.println("%nsequentialTest " + queue.getClass());
        Tests.assertTrue(queue.dequeue() == null); // tests if queue is empty
        queue.enqueue(3);
        queue.enqueue(5);
        queue.enqueue(4);
        Tests.assertTrue(queue.dequeue() == 3);
        Tests.assertTrue(queue.dequeue() == 5);
        queue.enqueue(8);
        Tests.assertTrue(queue.dequeue() == 4);
        Tests.assertTrue(queue.dequeue() == 8);
        queue.enqueue(12);
        queue.enqueue(8);
        queue.enqueue(12);
        Tests.assertTrue(queue.dequeue() == 12); // tests right order of the dequeued item
        Tests.assertTrue(queue.dequeue() == 8);
        Tests.assertTrue(queue.dequeue() == 12);
        Tests.assertTrue(queue.dequeue() == null); // tests if queue is empty
        Tests.assertTrue(queue.dequeue() == null); // tests if queue is empty
        for(int i = 1; i <= 10; i++) {
            queue.enqueue(i);
        }
        for(int i = 1; i <= 10; i++) {
            Tests.assertTrue(queue.dequeue() == i);
        }
        System.out.println("%nsequentialTest ... Passed!");
    }
}

class Tests {
    public static void assertEquals(int x, int y) throws Exception {
        if (x != y)
            throw new Exception(String.format("ERROR: %d not equal to %d%n", x, y));
    }

    public static void assertTrue(boolean b) throws Exception {
        if (!b)
            throw new Exception(String.format("ERROR: assertTrue"));
    }
}

interface UnboundedQueue<T> {
    void enqueue(T item);

    T dequeue();
}

// Unbounded non-blocking list-based lock-free queue by Michael and
// Scott 1996.  This version inspired by suggestions from Niels
// Abildgaard Roesen.

class MSQueueNeater<T> implements UnboundedQueue<T> {
    private final AtomicReference<Node<T>> head, tail;

    public MSQueueNeater() {
        Node<T> dummy = new Node<T>(null, null);
        head = new AtomicReference<Node<T>>(dummy);
        tail = new AtomicReference<Node<T>>(dummy);
    }

    public void enqueue(T item) { // at tail
        Node<T> node = new Node<T>(item, null);
        while (true) {
            final Node<T> last = tail.get(), next = last.next.get();
            if (next != null)
                tail.compareAndSet(last, next);
            else if (last.next.compareAndSet(null, node)) {
                tail.compareAndSet(last, node);
                return;
            }
        }
    }

    public T dequeue() { // from head
        while (true) {
            final Node<T> first = head.get(), last = tail.get(), next = first.next.get();
            if (next == null)
                return null;
            else if (first == last) // Injection fault: removing this does not change anything
                tail.compareAndSet(last, next);
            else if (head.compareAndSet(first, next))
                return next.item;
        }
    }

    private static class Node<T> {
        final T item;
        final AtomicReference<Node<T>> next;

        public Node(T item, Node<T> next) {
            this.item = item;
            this.next = new AtomicReference<Node<T>>(next);
        }
    }
}
