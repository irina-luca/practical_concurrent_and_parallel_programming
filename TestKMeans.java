package com.company;// Various implementations of k-means clustering
// sestoft@itu.dk * 2017-01-04

import org.multiverse.api.references.TxnDouble;
import org.multiverse.api.references.TxnInteger;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.function.IntFunction;
import java.util.function.Function;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.multiverse.api.StmUtils.atomic;
import static org.multiverse.api.StmUtils.newTxnDouble;
import static org.multiverse.api.StmUtils.newTxnInteger;

public class TestKMeans {
    public static void main(String[] args) {
        // There are n points and k clusters
        final int n = 200_000, k = 81;
        final Point[] points = GenerateData.randomPoints(n);
        final int[] initialPoints = GenerateData.randomIndexes(n, k);
        SystemInfo();
        for (int i = 0; i < 3; i++) {
//            timeKMeans(new KMeans1(points, k), initialPoints);
//            timeKMeans(new KMeans1P(points, k, 8), initialPoints);
//             timeKMeans(new KMeans2(points, k), initialPoints);
            timeKMeans(new KMeans2Q(points, k, 8), initialPoints);
            timeKMeans(new KMeans2P(points, k, 8), initialPoints);
            timeKMeans(new KMeans2Stm(points, k, 8), initialPoints);
//            timeKMeans(new KMeans3(points, k), initialPoints);
//            timeKMeans(new KMeans3P(points, k), initialPoints);
//            timeKMeans(new KMeans2PAssignmentPOnly(points, k, 8), initialPoints);
            // timeKMeans(new KMeans2Q(points, k), initialPoints);
            // timeKMeans(new KMeans2Stm(points, k), initialPoints);
            // timeKMeans(new KMeans3(points, k), initialPoints);
            // timeKMeans(new KMeans3P(points, k), initialPoints);
            System.out.println();
        }


    }


    public static void timeKMeans(KMeans km, int[] initialPoints) {
        Timer t = new Timer();
        km.findClusters(initialPoints);
        double time = t.check();
        // To avoid seeing the k computed clusters, comment out next line:
        km.print();
        System.out.printf("%-20s Real time: %9.3f%n", km.getClass(), time);
    }

    public static void SystemInfo() {
        System.out.printf("# OS:   %s; %s; %s%n",
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch"));
        System.out.printf("# JVM:  %s; %s%n",
                System.getProperty("java.vendor"),
                System.getProperty("java.version"));
        // The processor identifier works only on MS Windows:
        System.out.printf("# CPU:  %s; %d \"cores\"%n",
                System.getenv("PROCESSOR_IDENTIFIER"),
                Runtime.getRuntime().availableProcessors());
        java.util.Date now = new java.util.Date();
        System.out.printf("# Date: %s%n",
                new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(now));
    }
}

interface KMeans {
    void findClusters(int[] initialPoints);

    void print();
}

// ----------------------------------------------------------------------

class KMeans1 implements KMeans {
    // Sequential version 1.  A Cluster has an immutable mean field, and
    // a mutable list of immutable Points.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;

    public KMeans1(Point[] points, int k) {
        this.points = points;
        this.k = k;
    }

    public void findClusters(int[] initialPoints) {
        Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        boolean converged = false;
        while (!converged) {
            iterations++;
            { // Assignment step: put each point in exactly one cluster
                for (Point p : points) {
                    Cluster best = null;
                    for (Cluster c : clusters)
                        if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
                            best = c;
                    best.add(p);
                }
            }
            { // Update step: recompute mean of each cluster
                ArrayList<Cluster> newClusters = new ArrayList<>();
                converged = true;
                for (Cluster c : clusters) {
                    Point mean = c.computeMean();
                    if (!c.mean.almostEquals(mean))
                        converged = false;
                    if (mean != null)
                        newClusters.add(new Cluster(mean));
                    else
                        System.out.printf("===> Empty cluster at %s%n", c.mean);
                }
                clusters = newClusters.toArray(new Cluster[newClusters.size()]);
            }
        }
        this.clusters = clusters;
    }

    public void print() {
//        for (Cluster c : clusters)
//            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private final ArrayList<Point> points = new ArrayList<>();
        private final Point mean;

        public Cluster(Point mean) {
            this.mean = mean;
        }

        @Override
        public Point getMean() {
            return mean;
        }

        public void add(Point p) {
            points.add(p);
        }

        public Point computeMean() {
            double sumx = 0.0, sumy = 0.0;
            for (Point p : points) {
                sumx += p.x;
                sumy += p.y;
            }
            int count = points.size();
            return count == 0 ? null : new Point(sumx / count, sumy / count);
        }
    }
}

class KMeans1P implements KMeans {
    // Sequential version 1.  A Cluster has an immutable mean field, and
    // a mutable list of immutable Points.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;
    // Exercise 1.2: Part 1
    private final ExecutorService executor
            = Executors.newWorkStealingPool();
    private int P;
    private int perTask;

    public KMeans1P(Point[] points, int k, int P) { // P = # tasks
        this.points = points;
        this.k = k;
        this.P = P;
        this.perTask = points.length / P;
    }

    public void findClusters(int[] initialPoints) {

        Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        boolean converged = false;
        while (!converged) {
            iterations++;
            {
                // Exercise 1.2: Part 2 (Assignment step: put each point in exactly one cluster)
                List<Callable<Void>> assignmentTasks = new ArrayList<Callable<Void>>();
                final Cluster[] clustersF = clusters;
                for (int t = 0; t < this.P; t++) {
                    final int from = this.perTask * t,
                            to = (t + 1 == this.P) ? this.points.length : this.perTask * (t + 1);
                    assignmentTasks.add(() -> {
                        for (int i = from; i < to; i++) {
                            Cluster best = null;
                            for (Cluster c : clustersF)
                                if (best == null || points[i].sqrDist(c.mean) < points[i].sqrDist(best.mean)) {
                                    best = c;
                                }
                            best.add(points[i]);
                        }
                        return null;
                    });
                }
                try {
                    executor.invokeAll(assignmentTasks);
                } catch (InterruptedException exn) {
                    System.out.println("Interrupted: " + exn);
                }
                // End taskifying for Assignment step
//                for (Point p : points) {
//                    Cluster best = null;
//                    for (Cluster c : clusters)
//                        if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
//                            best = c;
//                    best.add(p);
//                }
            }
            {   // Exercise 1.3: (Update step: recompute mean of each cluster)
                ArrayList<Cluster> newClusters = new ArrayList<>();
                converged = true;
                List<Callable<Cluster>> tasksUpdate = new ArrayList<Callable<Cluster>>();
                int clustersTaskCount = clusters.length;
                final Cluster[] clustersF = clusters;
                final AtomicBoolean convergedF = new AtomicBoolean(true);
                for (int t = 0; t < clustersTaskCount; t++) {
                    final int tF = t;
                    tasksUpdate.add(() -> {
                        Cluster cluster = null; // Task local cluster
                        Point mean = clustersF[tF].computeMean();
                        if (!clustersF[tF].mean.almostEquals(mean))
                            convergedF.set(false);
                        if (mean != null) {
                            cluster = new Cluster(mean);
                        } else
                            System.out.printf("===> Empty cluster at %s%n", clustersF[tF].mean);
                        return cluster;
                    });
                }

                try {
                    List<Future<Cluster>> futures = executor.invokeAll(tasksUpdate);
                    for (Future<Cluster> fut : futures) {
                        Cluster resultedCluster = fut.get();
                        if (resultedCluster != null) {
                            newClusters.add(resultedCluster);
                        }
                    }
                } catch (InterruptedException exn) {
                    System.out.println("Interrupted: " + exn);
                } catch (ExecutionException exn) {
                    throw new RuntimeException(exn.getCause());
                } finally {
                    converged = convergedF.get(); // get the accumulated converged and assign it
                    clusters = newClusters.toArray(new Cluster[newClusters.size()]);
                }


                // End taskifying for Update step
//                for (Cluster c : clusters) {
//                    Point mean = c.computeMean();
//                    if (!c.mean.almostEquals(mean))
//                        converged = false;
//                    if (mean != null)
//                        newClusters.add(new Cluster(mean));
//                    else
//                        System.out.printf("===> Empty cluster at %s%n", c.mean);
//                }
            }
        }
        this.clusters = clusters;
    }

    public void print() {
//        for (Cluster c : clusters)
//            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private final ArrayList<Point> points = new ArrayList<>();
        private final Point mean;

        public Cluster(Point mean) {
            this.mean = mean;
        }

        @Override
        public Point getMean() {
            return mean;
        }

        public void add(Point p) {
            synchronized (this) { // intrinsic lock
                points.add(p);
            }
        }

        public Point computeMean() {
            double sumx = 0.0, sumy = 0.0;
            for (Point p : points) {
                sumx += p.x;
                sumy += p.y;
            }
            int count = points.size();
            return count == 0 ? null : new Point(sumx / count, sumy / count);
        }
    }
}


// ----------------------------------------------------------------------

class KMeans2 implements KMeans {
    // Sequential version 2.  Data represention: An array points of
    // Points and a same-index array myCluster of the Cluster to which
    // each point belongs, so that points[pi] belongs to myCluster[pi],
    // for each Point index pi.  A Cluster holds a mutable mean field
    // and has methods for aggregation of its value.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;

    public KMeans2(Point[] points, int k) {
        this.points = points;
        this.k = k;
    }

    public void findClusters(int[] initialPoints) {
        final Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        final Cluster[] myCluster = new Cluster[points.length];
        boolean converged = false;
        while (!converged) {
            iterations++;
            {
                // Assignment step: put each point in exactly one cluster
                for (int pi = 0; pi < points.length; pi++) {
                    Point p = points[pi];
                    Cluster best = null;
                    for (Cluster c : clusters)
                        if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
                            best = c;
                    myCluster[pi] = best;
                }
            }
            {
                // Update step: recompute mean of each cluster
                for (Cluster c : clusters)
                    c.resetMean();
                for (int pi = 0; pi < points.length; pi++)
                    myCluster[pi].addToMean(points[pi]);
                converged = true;
                for (Cluster c : clusters)
                    converged &= c.computeNewMean();
            }
            // System.out.printf("[%d]", iterations); // To diagnose infinite loops
        }
        this.clusters = clusters;
    }

    public void print() {
        for (Cluster c : clusters)
            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private Point mean;
        private double sumx, sumy;
        private int count;

        public Cluster(Point mean) {
            this.mean = mean;
        }

        public void addToMean(Point p) {
            sumx += p.x;
            sumy += p.y;
            count++;
        }

        // Recompute mean, return true if it stays almost the same, else false
        public boolean computeNewMean() {
            Point oldMean = this.mean;
            this.mean = new Point(sumx / count, sumy / count);
            return oldMean.almostEquals(this.mean);
        }

        public void resetMean() {
            sumx = sumy = 0.0;
            count = 0;
        }

        @Override
        public Point getMean() {
            return mean;
        }
    }
}


class KMeans2P implements KMeans {
    // Sequential version 2.  Data represention: An array points of
    // Points and a same-index array myCluster of the Cluster to which
    // each point belongs, so that points[pi] belongs to myCluster[pi],
    // for each Point index pi.  A Cluster holds a mutable mean field
    // and has methods for aggregation of its value.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;
    // Exercise 2.2: Part 1
    private final ExecutorService executor
            = Executors.newWorkStealingPool();
    private int P;
    private int perTask;

    public KMeans2P(Point[] points, int k, int P) {
        this.points = points;
        this.k = k;
        this.P = P;
        this.perTask = points.length / P;
    }

    public void findClusters(int[] initialPoints) {
        final Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        final Cluster[] myCluster = new Cluster[points.length];
        boolean converged = false;
        while (!converged) {
            iterations++;
            {
// Exercise 2.2 (the Assignment step)
                List<Callable<Void>> assignmentTasks = new ArrayList<Callable<Void>>();
                for (int t = 0; t < this.P; t++) {
                    final int from = this.perTask * t,
                            to = (t + 1 == this.P) ? this.points.length : this.perTask * (t + 1);
                    assignmentTasks.add(() -> {
                        for (int pi = from; pi < to; pi++) {
                            Point p = points[pi];
                            Cluster best = null;
                            for (Cluster c : clusters)
                                if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
                                    best = c;
                            myCluster[pi] = best;
                        }
                        return null;
                    });
                }
                try {
                    executor.invokeAll(assignmentTasks);
                } catch (InterruptedException exn) {
                    System.out.println("Interrupted: " + exn);
                }
                // End taskifying for Assignment step
                // Assignment step: put each point in exactly one cluster
//                for (int pi = 0; pi < points.length; pi++) {
//                    Point p = points[pi];
//                    Cluster best = null;
//                    for (Cluster c : clusters)
//                        if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
//                            best = c;
//                    myCluster[pi] = best;
//                }
            }
            {
// Update step: recompute mean of each cluster
                for (Cluster c : clusters) {
                    c.resetMean();
                }
// Start task parallelization for Update step
                List<Callable<Void>> updateTasks = new ArrayList<Callable<Void>>();
                for (int t = 0; t < this.P; t++) {
                    final int from = this.perTask * t,
                            to = (t + 1 == this.P) ? this.points.length : this.perTask * (t + 1);
                    updateTasks.add(() -> {
                        for (int pi = from; pi < to; pi++) {
                            myCluster[pi].addToMean(points[pi]);
                        }
                        return null;
                    });
                }
                try {
                    executor.invokeAll(updateTasks);
                } catch (InterruptedException exn) {
                    System.out.println("Interrupted: " + exn);
                }
// End task parallelization for Update step
                converged = true;
                for (Cluster c : clusters)
                    converged &= c.computeNewMean();
            }
//             System.out.printf("[%d]", iterations); // To diagnose infinite loops
        }
        this.clusters = clusters;
    }

    public void print() {
//        for (Cluster c : clusters)
//            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private Point mean;
        private double sumx, sumy;
        private int count;

        public Cluster(Point mean) {
            this.mean = mean;
        }

        public synchronized void addToMean(Point p) {
            sumx += p.x;
            sumy += p.y;
            count++;
        }

        // Recompute mean, return true if it stays almost the same, else false
        public boolean computeNewMean() {
            Point oldMean = this.mean;
            this.mean = new Point(sumx / count, sumy / count);
            return oldMean.almostEquals(this.mean);
        }

        public void resetMean() {
            sumx = sumy = 0.0;
            count = 0;
        }

        @Override
        public Point getMean() {
            return mean;
        }
    }
}


class KMeans2PAssignmentPOnly implements KMeans {
    // Sequential version 2.  Data represention: An array points of
    // Points and a same-index array myCluster of the Cluster to which
    // each point belongs, so that points[pi] belongs to myCluster[pi],
    // for each Point index pi.  A Cluster holds a mutable mean field
    // and has methods for aggregation of its value.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;
    // Exercise 2.2: Part 1
    private final ExecutorService executor
            = Executors.newWorkStealingPool();
    private int P;
    private int perTask;

    public KMeans2PAssignmentPOnly(Point[] points, int k, int P) {
        this.points = points;
        this.k = k;
        this.P = P;
        this.perTask = points.length / P;
    }

    public void findClusters(int[] initialPoints) {
        final Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        final Cluster[] myCluster = new Cluster[points.length];
        boolean converged = false;
        while (!converged) {
            iterations++;
            {
                // Exercise 2.2: Part 2
                // Reset the Mean before asigning!!! 2.8
                for (Cluster c : clusters) {
                    c.resetMean();
                }
                // Start taskifying for Assignment step
                List<Callable<Void>> assignmentTasks = new ArrayList<Callable<Void>>();
                for (int t = 0; t < this.P; t++) {
                    final int from = this.perTask * t,
                            to = (t + 1 == this.P) ? this.points.length : this.perTask * (t + 1);
                    assignmentTasks.add(() -> {
                        for (int pi = from; pi < to; pi++) {
                            Point p = points[pi];
                            Cluster best = null;
                            for (Cluster c : clusters)
                                if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
                                    best = c;
                            myCluster[pi] = best;
//                            best.addToMean(p);
                        }
                        return null;
                    });
                }
                try {
                    executor.invokeAll(assignmentTasks);
                } catch (InterruptedException exn) {
                    System.out.println("Interrupted: " + exn);
                }
                // End taskifying for Assignment step
                // Assignment step: put each point in exactly one cluster
//                for (int pi = 0; pi < points.length; pi++) {
//                    Point p = points[pi];
//                    Cluster best = null;
//                    for (Cluster c : clusters)
//                        if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
//                            best = c;
//                    myCluster[pi] = best;
//                }
            }
            {
                // Update step: recompute mean of each cluster
                for (Cluster c : clusters)
                    c.resetMean();
                for (int pi = 0; pi < points.length; pi++)
                    myCluster[pi].addToMean(points[pi]);
                converged = true;
                for (Cluster c : clusters)
                    converged &= c.computeNewMean();
            }
//             System.out.printf("[%d]", iterations); // To diagnose infinite loops
        }
        this.clusters = clusters;
    }

    public void print() {
        for (Cluster c : clusters)
            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private Point mean;
        private double sumx, sumy;
        private int count;

        public Cluster(Point mean) {
            this.mean = mean;
        }

        public synchronized void addToMean(Point p) {
            sumx += p.x;
            sumy += p.y;
            count++;
        }

        // Recompute mean, return true if it stays almost the same, else false
        public boolean computeNewMean() {
            Point oldMean = this.mean;
            this.mean = new Point(sumx / count, sumy / count);
            return oldMean.almostEquals(this.mean);
        }

        public void resetMean() {
            sumx = sumy = 0.0;
            count = 0;
        }

        @Override
        public Point getMean() {
            return mean;
        }
    }
}


class KMeans2Q implements KMeans {
    // Sequential version 2.  Data represention: An array points of
    // Points and a same-index array myCluster of the Cluster to which
    // each point belongs, so that points[pi] belongs to myCluster[pi],
    // for each Point index pi.  A Cluster holds a mutable mean field
    // and has methods for aggregation of its value.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;
    // Exercise 2.2: Part 1
    private final ExecutorService executor
            = Executors.newWorkStealingPool();
    private int P;
    private int perTask;

    public KMeans2Q(Point[] points, int k, int P) {
        this.points = points;
        this.k = k;
        this.P = P;
        this.perTask = points.length / P;
    }

public void findClusters(int[] initialPoints) {
    final Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
    boolean converged = false;
    while (!converged) {
        iterations++;
        {
            // Exercise 2.8-9: Reset the Mean before assigning
            for (Cluster c : clusters) {
                c.resetMean();
            }
            // START: Assignment step
            List<Callable<Void>> assignmentTasks = new ArrayList<Callable<Void>>();
            for (int t = 0; t < this.P; t++) {
                final int from = this.perTask * t,
                        to = (t + 1 == this.P) ? this.points.length : this.perTask * (t + 1);
                assignmentTasks.add(() -> {
                    for (int pi = from; pi < to; pi++) {
                        Point p = points[pi];
                        Cluster best = null;
                        for (Cluster c : clusters)
                            if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
                                best = c;
                        best.addToMean(p);
                    }
                    return null;
                });
            }
            try {
                executor.invokeAll(assignmentTasks);
            } catch (InterruptedException exn) {
                System.out.println("Interrupted: " + exn);
            }
            // END: Assignment step
        }
        {
            // START: Update step -- recompute mean of each cluster
            converged = true;
            for (Cluster c : clusters)
                converged &= c.computeNewMean();
            // END: Update step -- recompute mean of each cluster
        }
//             System.out.printf("[%d]", iterations); // To diagnose infinite loops
    }
    this.clusters = clusters;
}

    public void print() {
//        for (Cluster c : clusters)
//            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private Point mean;
        private double sumx, sumy;
        private int count;

        public Cluster(Point mean) {
            this.mean = mean;
        }

        public synchronized void addToMean(Point p) {
            sumx += p.x;
            sumy += p.y;
            count++;
        }

        // Recompute mean, return true if it stays almost the same, else false
        public boolean computeNewMean() {
            Point oldMean = this.mean;
            this.mean = new Point(sumx / count, sumy / count);
            return oldMean.almostEquals(this.mean);
        }

        public void resetMean() {
            sumx = sumy = 0.0;
            count = 0;
        }

        @Override
        public Point getMean() {
            return mean;
        }
    }
}


class KMeans2Stm implements KMeans {
    // Sequential version 2.  Data represention: An array points of
    // Points and a same-index array myCluster of the Cluster to which
    // each point belongs, so that points[pi] belongs to myCluster[pi],
    // for each Point index pi.  A Cluster holds a mutable mean field
    // and has methods for aggregation of its value.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;
    // Exercise 2.2: Part 1
    private final ExecutorService executor
            = Executors.newWorkStealingPool();
    private int P;
    private int perTask;

    public KMeans2Stm(Point[] points, int k, int P) {
        this.points = points;
        this.k = k;
        this.P = P;
        this.perTask = points.length / P;
    }

    public void findClusters(int[] initialPoints) {
        final Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        final Cluster[] myCluster = new Cluster[points.length];
        boolean converged = false;
        while (!converged) {
            iterations++;
            {

                // Start taskifying for Assignment step
                List<Callable<Void>> assignmentTasks = new ArrayList<Callable<Void>>();
                for (int t = 0; t < this.P; t++) {
                    final int from = this.perTask * t,
                            to = (t + 1 == this.P) ? this.points.length : this.perTask * (t + 1);
                    assignmentTasks.add(() -> {
                        for (int pi = from; pi < to; pi++) {
                            Point p = points[pi];
                            Cluster best = null;
                            for (Cluster c : clusters)
                                if (best == null || p.sqrDist(c.mean) < p.sqrDist(best.mean))
                                    best = c;
                            myCluster[pi] = best;
//                            best.addToMean(p);
                        }
                        return null;
                    });
                }
                try {
                    executor.invokeAll(assignmentTasks);
                } catch (InterruptedException exn) {
                    System.out.println("Interrupted: " + exn);
                }
            }
            {
                // Update step: recompute mean of each cluster
                for (Cluster c : clusters) {
                    c.resetMean();
                }
                // Start taskifying for Update step
                List<Callable<Void>> updateTasks = new ArrayList<Callable<Void>>();
                for (int t = 0; t < this.P; t++) {
                    final int from = this.perTask * t,
                            to = (t + 1 == this.P) ? this.points.length : this.perTask * (t + 1);
                    updateTasks.add(() -> {
                        for (int pi = from; pi < to; pi++) {
                            myCluster[pi].addToMean(points[pi]);
                        }
                        return null;
                    });
                }
                try {
                    executor.invokeAll(updateTasks);
                } catch (InterruptedException exn) {
                    System.out.println("Interrupted: " + exn);
                }
                converged = true;
                for (Cluster c : clusters)
                    converged &= c.computeNewMean();
            }
//             System.out.printf("[%d]", iterations); // To diagnose infinite loops
        }
        this.clusters = clusters;
    }

    public void print() {
        for (Cluster c : clusters)
            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private Point mean;
        private TxnDouble sumx, sumy;
        private TxnInteger count;

        public Cluster(Point mean) {
            this.mean = mean;
            sumx = newTxnDouble(0.0);
            sumy = newTxnDouble(0.0);
            count = newTxnInteger(0);
        }

        public void addToMean(Point p) {
            atomic(() -> {
                sumx.incrementAndGet(p.x);
                sumy.incrementAndGet(p.y);
                count.increment();
            });
        }

        // Recompute mean, return true if it stays almost the same, else false
        public boolean computeNewMean() {
            Point oldMean = this.mean;

            this.mean = new Point(sumx.atomicGet() / count.atomicGet(), sumy.atomicGet() / count.atomicGet());

            return oldMean.almostEquals(this.mean);
        }

        public void resetMean() {
            sumx = newTxnDouble(0.0);
            sumy = newTxnDouble(0.0);
            count = newTxnInteger(0);
        }

        @Override
        public Point getMean() {
            return mean;
        }
    }
}

// ----------------------------------------------------------------------

class KMeans3 implements KMeans {
    // Stream-based version. Representation (A2): Immutable Clusters of
    // immutable Points.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;

    public KMeans3(Point[] points, int k) {
        this.points = points;
        this.k = k;
    }

    public void findClusters(int[] initialPoints) {
        Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        boolean converged = false;
        while (!converged) {
            iterations++;
            { // Assignment step: put each point in exactly one cluster
                final Cluster[] clustersLocal = clusters;  // For capture in lambda
                Stream<Point> sPoints = Arrays.stream(points);
                Map<Cluster, List<Point>> groups = sPoints.collect(Collectors.groupingBy(point -> {
                    Cluster best = null;
                    for (Cluster c : clustersLocal)
                        if (best == null || point.sqrDist(c.mean) < point.sqrDist(best.mean))
                            best = c;

                    return best;
                }));
                clusters = groups.entrySet().stream().map(kv -> {
                    Cluster oldCluster = kv.getKey();
                    List<Point> points = kv.getValue();
                    return new Cluster(oldCluster.getMean(), points);
                }).toArray(size -> new Cluster[size]); // used StackOverflow as reference(http://stackoverflow.com/questions/23079003/how-to-convert-a-java-8-stream-to-an-array)

            }
            { // Update step: recompute mean of each cluster
                Cluster[] newClusters = Stream.of(clusters).map(cluster -> cluster.computeMean()).toArray(size -> new Cluster[size]);
                converged = Arrays.equals(clusters, newClusters);
                clusters = newClusters;
            }
        }
        this.clusters = clusters;
    }

    public void print() {
        for (Cluster c : clusters)
            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private final List<Point> points;
        private final Point mean;

        public Cluster(Point mean) {
            this(mean, new ArrayList<>());
        }

        public Cluster(Point mean, List<Point> points) {
            this.mean = mean;
            this.points = Collections.unmodifiableList(points);
        }

        @Override
        public Point getMean() {
            return mean;
        }

        public Cluster computeMean() {
            double sumx = points.stream().mapToDouble(p -> p.x).sum(),
                    sumy = points.stream().mapToDouble(p -> p.y).sum();
            Point newMean = new Point(sumx / points.size(), sumy / points.size());
            return new Cluster(newMean, points);
        }
    }
}


class KMeans3P implements KMeans {
    // Stream-based version. Representation (A2): Immutable Clusters of
    // immutable Points.

    private final Point[] points;
    private final int k;
    private Cluster[] clusters;
    private int iterations;

    public KMeans3P(Point[] points, int k) {
        this.points = points;
        this.k = k;
    }

    public void findClusters(int[] initialPoints) {
        Cluster[] clusters = GenerateData.initialClusters(points, initialPoints, Cluster::new, Cluster[]::new);
        boolean converged = false;
        while (!converged) {
            iterations++;
            { // Assignment step: put each point in exactly one cluster
                final Cluster[] clustersLocal = clusters;  // For capture in lambda
                Stream<Point> sPoints = Arrays.stream(points);
                Map<Cluster, List<Point>> groups = sPoints.parallel().collect(Collectors.groupingBy(point -> {
                    Cluster best = null;
                    for (Cluster c : clustersLocal)
                        if (best == null || point.sqrDist(c.mean) < point.sqrDist(best.mean))
                            best = c;

                    return best;
                }));
                clusters = groups.entrySet().stream().parallel().map(kv -> {
                    Cluster oldCluster = kv.getKey();
                    List<Point> points = kv.getValue();
                    return new Cluster(oldCluster.getMean(), points);
                }).toArray(size -> new Cluster[size]); // used StackOverflow as reference(http://stackoverflow.com/questions/23079003/how-to-convert-a-java-8-stream-to-an-array)

            }
            { // Update step: recompute mean of each cluster
                Cluster[] newClusters = Stream.of(clusters).parallel().map(cluster -> cluster.computeMean()).toArray(size -> new Cluster[size]);
                converged = Arrays.equals(clusters, newClusters);
                clusters = newClusters;
            }
        }
        this.clusters = clusters;
    }

    public void print() {
        for (Cluster c : clusters)
            System.out.println(c);
        System.out.printf("Used %d iterations%n", iterations);
    }

    static class Cluster extends ClusterBase {
        private final List<Point> points;
        private final Point mean;

        public Cluster(Point mean) {
            this(mean, new ArrayList<>());
        }

        public Cluster(Point mean, List<Point> points) {
            this.mean = mean;
            this.points = Collections.unmodifiableList(points);
        }

        @Override
        public Point getMean() {
            return mean;
        }

        public Cluster computeMean() {
            double sumx = points.stream().mapToDouble(p -> p.x).sum(),
                    sumy = points.stream().mapToDouble(p -> p.y).sum();
            Point newMean = new Point(sumx / points.size(), sumy / points.size());
            return new Cluster(newMean, points);
        }
    }
}


// ----------------------------------------------------------------------

// DO NOT MODIFY ANYTHING BELOW THIS LINE

// Immutable 2D points (x,y) with some basic operations

class Point {
    public final double x, y;

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    // The square of the Euclidean distance between this Point and that
    public double sqrDist(Point that) {
        return sqr(this.x - that.x) + sqr(this.y - that.y);
    }

    private static double sqr(double d) {
        return d * d;
    }

    // Reasonable when original point coordinates are integers.
    private static final double epsilon = 1E-10;

    // Approximate equality of doubles and Points.  There are better
    // ways to do this, but here we prefer simplicity.
    public static boolean almostEquals(double x, double y) {
        return Math.abs(x - y) <= epsilon;
    }

    public boolean almostEquals(Point that) {
        return almostEquals(this.x, that.x) && almostEquals(this.y, that.y);
    }

    @Override
    public String toString() {
        return String.format("(%17.14f, %17.14f)", x, y);
    }
}

// Printing and approximate comparison of clusters

abstract class ClusterBase {
    public abstract Point getMean();

    // Two Clusters are considered equal if their means are almost equal
    @Override
    public boolean equals(Object o) {
        return o instanceof ClusterBase
                && this.getMean().almostEquals(((ClusterBase) o).getMean());
    }

    @Override
    public String toString() {
        return String.format("mean = %s", getMean());
    }
}

// Generation of test data

class GenerateData {
    // Intentionally not really random, for reproducibility
    private static final Random rnd = new Random(42);

    // An array of means (centers) of future point clusters,
    // approximately arranged in a 9x9 grid
    private static final Point[] centers =
            IntStream.range(0, 9).boxed()
                    .flatMap(x -> IntStream.range(0, 9).mapToObj(y -> new Point(x * 10 + 4, y * 10 + 4)))
                    .toArray(Point[]::new);

    // Make a random point near a randomly chosen center
    private static Point randomPoint() {
        Point orig = centers[rnd.nextInt(centers.length)];
        return new Point(orig.x + rnd.nextDouble() * 8, orig.y + rnd.nextDouble() * 8);
    }

    // Make an array of n points near some of the centers
    public static Point[] randomPoints(int n) {
        return Stream.<Point>generate(GenerateData::randomPoint).limit(n).toArray(Point[]::new);
    }

    // Make an array of k distinct random numbers of the range [0...n-1]
    public static int[] randomIndexes(int n, int k) {
        final HashSet<Integer> initial = new HashSet<>();
        while (initial.size() < k)
            initial.add(rnd.nextInt(n));
        return initial.stream().mapToInt(i -> i).toArray();
    }

    // Select k distinct Points as cluster centers, passing in functions
    // to create appropriate Cluster objects and Cluster arrays.
    public static <C extends ClusterBase>
    C[] initialClusters(Point[] points, int[] pointIndexes,
                        Function<Point, C> makeC, IntFunction<C[]> makeCArray) {
        C[] initial = makeCArray.apply(pointIndexes.length);
        for (int i = 0; i < pointIndexes.length; i++)
            initial[i] = makeC.apply(points[pointIndexes[i]]);
        return initial;
    }
}

// Crude wall clock timing utility, measuring time in seconds

class Timer {
    private long start = 0, spent = 0;

    public Timer() {
        play();
    }

    public double check() {
        return (start == 0 ? spent : System.nanoTime() - start + spent) / 1e9;
    }

    public void pause() {
        if (start != 0) {
            spent += System.nanoTime() - start;
            start = 0;
        }
    }

    public void play() {
        if (start == 0) start = System.nanoTime();
    }
}
