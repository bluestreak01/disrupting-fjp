package net.shipilev;

import com.questdb.mp.*;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class QuestdbFanOut extends Workload {

    private ExecutorService executor;
    private PiResultReclaimer result;
    private Sequence pubSeq;
    private RingQueue<PiJob> queue;
    private PiEventProcessor[] procs;

    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {
        QuestdbFanOut qdb = new QuestdbFanOut();
        QuestdbFanOut.slicesK = 100000;
        QuestdbFanOut.threads = 1;
        qdb.setup();
        long t = System.currentTimeMillis();
        qdb.run();
        System.out.println("time: " + (System.currentTimeMillis() - t));
        qdb.tearDown();
    }

//    @Benchmark
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
            partitionId = (partitionId == (ts - 1)) ? 0 : partitionId + 1;
            pubSeq.done(cursor);
        }
        return result.get();
    }

    @Setup(Level.Iteration)
    public void setup() {
        this.queue = new RingQueue<>(new PiEventFac(), Integer.highestOneBit(getSlices()));
        FanOut fanOut = new FanOut();
        pubSeq = new SPSequence(queue.getCapacity());
        SCSequence reclaimerSequence = new SCSequence();
        pubSeq.followedBy(fanOut).followedBy(reclaimerSequence).followedBy(pubSeq);

        executor = Executors.newCachedThreadPool();
        procs = new PiEventProcessor[getThreads()];
        for (int i = 0; i < procs.length; i++) {
            executor.submit(procs[i] = new PiEventProcessor(i, queue, fanOut.addAndGet(new SCSequence())));
        }
        executor.submit(result = new PiResultReclaimer(getSlices(), queue, reclaimerSequence));
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
                long cursor = sequence.next();
                if (cursor < 0) {
                    LockSupport.parkNanos(1);
                    continue;
                }

                long available = sequence.available();
//                batches.add((int) (available - cursor));
                while (cursor < available) {
                    PiJob event = queue.get(cursor++);
                    if (index == event.partitionId) {
                        event.calculatePi();
                    }
                }
                sequence.done(available - 1);
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
        private final SCSequence sequence;
        private double result;
        private long seq;
        private volatile boolean running = true;

        public PiResultReclaimer(final int numSlice, RingQueue<PiJob> queue, SCSequence sequence) {
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
