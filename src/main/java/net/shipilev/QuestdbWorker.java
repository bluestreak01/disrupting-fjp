package net.shipilev;

import com.questdb.mp.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class QuestdbWorker extends Workload {

    private ExecutorService executor;
    private PiResultReclaimer result;
    private Sequence pubSeq;
    private RingQueue<PiJob> queue;
    private PiEventProcessor[] procs;

    @Benchmark
    public double run() throws InterruptedException, BrokenBarrierException {
        int ts = getThreads();
        int slices = getSlices();
        int partitionId = 0;
        int i = 0;
        while (i < slices) {
            long cursor = pubSeq.next();
            if (cursor < 0) {
                continue;
            }
            PiJob piJob = queue.get(cursor);
            piJob.sliceNr = i++;
            piJob.result = 0;
            piJob.partitionId = partitionId;
            pubSeq.done(cursor);
            partitionId = (partitionId == (ts - 1)) ? 0 : partitionId + 1;
        }
        return result.get();
    }

    @Setup(Level.Iteration)
    public void setup() {
        this.queue = new RingQueue<>(new PiEventFac(), Integer.highestOneBit(getSlices()));
        this.pubSeq = new SPSequence(queue.getCapacity());
        SCSequence resultSequence = new SCSequence();
        MCSequence processorSequence = new MCSequence(queue.getCapacity(), null);
        pubSeq.followedBy(processorSequence).followedBy(resultSequence).followedBy(pubSeq);

        executor = Executors.newCachedThreadPool();

        procs = new PiEventProcessor[getThreads()];
        for (int i = 0; i < procs.length; i++) {
            executor.submit(procs[i] = new PiEventProcessor(i, queue, processorSequence));
        }
        executor.submit(result = new PiResultReclaimer(getSlices(), queue, resultSequence));
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        result.halt();
        for (PiEventProcessor p : procs) {
            p.halt();
        }
        executor.shutdownNow();
    }

    public static class PiJob {
        public double result;
        public int sliceNr;
        public int partitionId;

        public void calculatePi() {
            result = doCalculatePi(sliceNr);
        }
    }

    public static class PiEventFac implements com.questdb.std.ObjectFactory<PiJob> {
        @Override
        public PiJob newInstance() {
            return new PiJob();
        }
    }

    public static class PiEventProcessor implements Runnable {
        private final RingQueue<PiJob> queue;
        private final Sequence sequence;
        private final int index;
        private volatile boolean running = true;

        public PiEventProcessor(int index, RingQueue<PiJob> queue, Sequence sequence) {
            this.index = index;
            this.queue = queue;
            this.sequence = sequence;
        }

        @Override
        public void run() {
            while (running) {
                final long cursor = sequence.next();
                if (cursor < 0) {
                    LockSupport.parkNanos(1);
                    continue;
                }

                PiJob event = queue.get(cursor);
                if (index == event.partitionId) {
                    event.calculatePi();
                }

                sequence.done(cursor);
            }
        }

        void halt() {
            running = false;
        }
    }

    public static class PiResultReclaimer implements Runnable {
        private final int numSlice;
        private final CountDownLatch latch;
        private final RingQueue<PiJob> queue;
        private final Sequence sequence;
        private double result;
        private long seq;
        private volatile boolean running = true;

        public PiResultReclaimer(final int numSlice, RingQueue<PiJob> queue, Sequence sequence) {
            this.numSlice = numSlice;
            latch = new CountDownLatch(1);
            this.queue = queue;
            this.sequence = sequence;
        }

        public double get() throws InterruptedException {
            latch.await();
            return result;
        }

        void halt() {
            running = false;
        }

        @Override
        public void run() {
            while (running) {
                long cursor = sequence.next();
                if (cursor < 0) {
                    LockSupport.parkNanos(1);
                    continue;
                }

                long available = sequence.available();
                while (cursor < available) {
                    result += queue.get(cursor++).result;
                    seq++;
                }
                sequence.done(available - 1);

                if (seq >= numSlice) {
                    latch.countDown();
                }
            }
        }
    }
}
