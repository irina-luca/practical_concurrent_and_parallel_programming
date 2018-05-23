package com.company;

import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;


public class QueueTests extends Tests {
    // We could use one CyclicBarrier for both starting and stopping,
    // precisely because it is cyclic, but the code becomes clearer by
    // separating them:
    protected CyclicBarrier startBarrier, stopBarrier;
    protected final MSQueueNeater<Integer> queue;
    protected final int nTrials, nPairs;
    protected final AtomicInteger enqueueSum = new AtomicInteger(0);
    protected final AtomicInteger dequeueSum = new AtomicInteger(0);

    public QueueTests(MSQueueNeater<Integer> queue, int npairs, int ntrials) {
        this.queue = queue;
        this.nTrials = ntrials;
        this.nPairs = npairs;
        this.startBarrier = new CyclicBarrier(npairs * 2 + 1);
        this.stopBarrier = new CyclicBarrier(npairs * 2 + 1);
    }

    void test(ExecutorService pool) {
        try {
            for (int i = 0; i < nPairs; i++) {
                pool.execute(new Producer());
                pool.execute(new Consumer());
            }
            startBarrier.await(); // wait for all threads to be ready
            stopBarrier.await();  // wait for all threads to finish
            assertEquals(enqueueSum.get(), dequeueSum.get());
//            System.out.println("enqueueSum.get(): " + enqueueSum.get());
//            System.out.println("dequeueSum.get(): " + dequeueSum.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class Producer implements Runnable {
        public void run() {
            try {
                Random random = new Random();
                int sum = 0;
                startBarrier.await();
                for (int i = nTrials; i > 0; --i) {
                    int item = random.nextInt();
                    // Can enqueue all the time,
                    // because it is unbounded
                    queue.enqueue(item);
                    sum += item;
                }
                enqueueSum.getAndAdd(sum);
                stopBarrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    class Consumer implements Runnable {
        public void run() {
            try {
                startBarrier.await();
                int sum = 0;
                for (int i = nTrials; i > 0; --i) {
                    Integer dequeuedItem = null;
                    // Try to deque until an actual item succeeds and is not be null,
                    // so the local sum can be updated
                    while(dequeuedItem == null) {
                        dequeuedItem = queue.dequeue();
                    }
                    sum += dequeuedItem;
                }
                dequeueSum.getAndAdd(sum);
                stopBarrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}