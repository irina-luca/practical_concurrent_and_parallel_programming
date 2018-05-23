import java.util.Date;
import java.util.Random;
import java.io.*;
import akka.actor.*;
/**
 * Created by irilu on 12/1/2016.
 */

// -- MESSAGES --------------------------------------------------
class StartTransferMessage implements Serializable {
    public final ActorRef bank;
    public final ActorRef from;
    public final ActorRef to;
    public StartTransferMessage(ActorRef bank, ActorRef from, ActorRef to) {
        this.bank = bank;
        this.from = from;
        this.to = to;
    }
    /* TODO */
}
class TransferMessage implements Serializable {
    public final int amount;
    public final ActorRef from;
    public final ActorRef to;
    public TransferMessage(int amount, ActorRef from, ActorRef to) {
        this.amount = amount;
        this.from = from;
        this.to = to;
    }
    /* TODO */
}
class DepositMessage implements Serializable {
    public final int amount;
    public DepositMessage(int amount) { // , ActorRef from, ActorRef to
        this.amount = amount;
    }
    /* TODO */
}
class PrintBalanceMessage implements Serializable {
    public final String message;
    public PrintBalanceMessage(String message) {
        this.message = message;
    }
    /* TODO */
}
// -- ACTORS --------------------------------------------------
class AccountActor extends UntypedActor {
    private int balance = 0;
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof DepositMessage) {
            DepositMessage depositMessage = (DepositMessage) message;
            this.balance += depositMessage.amount;
        } else if(message instanceof PrintBalanceMessage) {
            PrintBalanceMessage printBalanceMessage = (PrintBalanceMessage) message;
            System.out.println(printBalanceMessage.message + this.balance);
        }
    } /* TODO */ }
class BankActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof TransferMessage) {
            TransferMessage transferMessage = (TransferMessage) message;
            transferMessage.from.tell(new DepositMessage((-1) * transferMessage.amount), getSelf()); // maybe getSelf() here, not sure. check out on that later
            transferMessage.to.tell(new DepositMessage(transferMessage.amount), getSelf());
        }
    } /* TODO */ }
class ClerkActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Throwable {
        if(message instanceof StartTransferMessage) {
            StartTransferMessage startMessage = (StartTransferMessage) message;
            Random random = new Random();

            for(int i = 0; i < 100; i++) {
                int randInt = random.nextInt(48);
//                System.out.println("Random sent is:" + randInt);
                startMessage.bank.tell(new TransferMessage(randInt, startMessage.from, startMessage.to), getSelf());
            }
        }

    } /* TODO */ }
// -- MAIN --------------------------------------------------
public class ABC { // Demo showing how things work:
    public static void main(String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("ABCSystem");
 /* TODO (CREATE ACTORS AND SEND START MESSAGES) */
        // Create the actors:
        final ActorRef A1 = system.actorOf(Props.create(AccountActor.class), "A1");
        final ActorRef A2 = system.actorOf(Props.create(AccountActor.class), "A2");
        final ActorRef B1 = system.actorOf(Props.create(BankActor.class), "B1");
        final ActorRef B2 = system.actorOf(Props.create(BankActor.class), "B2");
        final ActorRef C1 = system.actorOf(Props.create(ClerkActor.class), "C1");
        final ActorRef C2 = system.actorOf(Props.create(ClerkActor.class), "C2");

        // Start messages
        C1.tell(new StartTransferMessage(B1, A1, A2), ActorRef.noSender());
        C2.tell(new StartTransferMessage(B2, A2, A1), ActorRef.noSender());

        try {
            System.out.println("Press return to inspect...");
            System.in.read();
            /* TODO (INSPECT FINAL BALANCES) */
            A1.tell(new PrintBalanceMessage("Balance of A1:"), ActorRef.noSender());
            A2.tell(new PrintBalanceMessage("Balance of A2:"), ActorRef.noSender());
            System.out.println("Press return to terminate...");
            System.in.read();
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            system.shutdown();
        }
    }
}
