package com.hortonworks;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;

import com.hortonworks.streaming.impl.domain.transport.TruckConfiguration;
import com.hortonworks.streaming.impl.messages.StartSimulation;
import com.hortonworks.streaming.listeners.SimulatorListener;
import com.hortonworks.streaming.masters.SimulationMaster;

@SuppressWarnings("deprecation")
public class App {
  public static void main(String[] args1) {

    final String DEFAULT_EMITTER = "com.hortonworks.streaming.impl.domain.transport.Truck";
    final String DEFAULT_COLLECTOR= "com.hortonworks.streaming.impl.collectors.KafkaEventCollector";
String[] args={ "6" ,"250" ,"com.hortonworks.streaming.impl.domain.transport.Truck", "com.hortonworks.streaming.impl.collectors.StdOutEventCollector" ,"1000" ,"/etc/storm_demo/routes/midwest","1000"};

    try {

      final Class<? extends Object> eventEmitterClass = args[2] == null ?
          Class.forName(DEFAULT_EMITTER) : Class.forName(args[2]);
      final Class<? extends Object> eventCollectorClass = args[3] == null ?
          Class.forName(DEFAULT_COLLECTOR) : Class.forName(args[3]);
      final long demoId = Long.parseLong(args[4]);
      final String routesDirectory = args[5];
      final Integer messageDelay = new Integer(args[6]);
System.out.println("routesDirectory:"+routesDirectory);
      TruckConfiguration.initialize(routesDirectory);
      final int numberOfEventEmitters = TruckConfiguration.freeRoutePool.size();

      System.out.println("Number of Event Emitters = " + numberOfEventEmitters);

      final int numberOfEvents = Integer.parseInt(args[1]);

      ActorSystem system = ActorSystem.create("EventSimulator");


      final ActorRef listener = system.actorOf(
          Props.create(SimulatorListener.class), "listener");
      final ActorRef eventCollector = system.actorOf(
          Props.create(eventCollectorClass), "eventCollector");
      System.out.println(eventCollector.path());



	final ActorRef master = system.actorOf(new Props(
          new UntypedActorFactory() {
          
			private static final long serialVersionUID = -3621148210901309380L;

			public UntypedActor create() {
              return new SimulationMaster(
                  numberOfEventEmitters,
                  eventEmitterClass, listener, numberOfEvents, demoId,
                  messageDelay);
            }
          }), "master");

      master.tell(new StartSimulation(), master);
    } catch (NumberFormatException e) {
      System.err.println("Invalid number of emitters: "
          + e.getMessage());
    } catch (ClassNotFoundException e) {
      System.err.println("Cannot find classname: " + e.getMessage());
    }

  }
}
