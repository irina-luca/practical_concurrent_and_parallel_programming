package com.company;

import java.io.*;
import akka.actor.*;

// -- MESSAGES --------------------------------------------------
class InitMessage implements Serializable {
    private ActorRef odd, even, collector;
    public InitMessage(ActorRef odd, ActorRef even, ActorRef collector) {
        this.odd = odd;
        this.even = even;
        this.collector = collector;
    }
    public InitMessage(ActorRef collector) {
        this.collector = collector;
    }

    public ActorRef getOdd() {
        return odd;
    }

    public ActorRef getEven() {
        return even;
    }

    public ActorRef getCollector() {
        return collector;
    }
}
class NumMessage implements Serializable {
    private final int number;
    public NumMessage(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
}
// -- ACTORS --------------------------------------------------
class DispatcherActor extends UntypedActor {
    private ActorRef odd, even;
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof InitMessage) {
            InitMessage initMessage = (InitMessage) message;
            ActorRef collector = initMessage.getCollector();
            this.even = initMessage.getEven();
            this.odd = initMessage.getOdd();

            this.odd.tell(new InitMessage(collector), ActorRef.noSender());
            this.even.tell(new InitMessage(collector), ActorRef.noSender());

        } else if(message instanceof NumMessage) {
            NumMessage numMessage = (NumMessage) message;
            int number = numMessage.getNumber();
            if(number % 2 == 0) {
                this.even.tell(new NumMessage(number), ActorRef.noSender());
            } else {
                this.odd.tell(new NumMessage(number), ActorRef.noSender());
            }
        }
    }
}
class WorkerActor extends UntypedActor {
    private ActorRef collector;
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof InitMessage) {
            InitMessage initMessage = (InitMessage) message;
            this.collector = initMessage.getCollector();
        } else if(message instanceof NumMessage) {
            NumMessage numMessage = (NumMessage) message;
            int number = numMessage.getNumber();
            int res = number * number;
            this.collector.tell(new NumMessage(res), ActorRef.noSender());
        }
    }
}
class CollectorActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof NumMessage) {
            NumMessage numMessage = (NumMessage) message;
            System.out.println(numMessage.getNumber());
        }

    }
}
// -- MAIN --------------------------------------------------
public class StartAkka {
    public static void main(String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("StartAkka");

        // -- SPAWN PHASE ----------
        final ActorRef Dispatcher = system.actorOf(Props.create(DispatcherActor.class), "Dispatcher");
        final ActorRef Odd = system.actorOf(Props.create(WorkerActor.class), "Odd");
        final ActorRef Even = system.actorOf(Props.create(WorkerActor.class), "Even");
        final ActorRef Collector = system.actorOf(Props.create(CollectorActor.class), "Collector");

        // --- INIT PHASE ----------
        Dispatcher.tell(new InitMessage(Odd, Even, Collector),ActorRef.noSender());

        // -- COMPUTE PHASE ----------
        for(int i = 1; i <= 10; i++) {
            Dispatcher.tell(new NumMessage(i),ActorRef.noSender());
        }

        try {
            System.out.println("Press return to inspect...");
            System.in.read();
            System.out.println("Press return to terminate...");
            System.in.read();
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            system.shutdown();
        }
    }
}

