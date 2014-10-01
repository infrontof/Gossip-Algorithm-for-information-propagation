Gossip-Algorithm-for-information-propagation
============================================
A participant(actor) it told a rumor by the main process. Each actor selects a random neighbor and tells it the rumor. Each actor keeps track of rumors and how many times it has heard the rumor. It stops transmitting once it has heard the rumor certain times. There are four kinds of network topologic can be used.
 1.Full Network Every actor is a neighboor of all other actors. That is, every actor can talk directly to any other actor.
 2. 2D Grid: Actors form a 2D grid. The actors can only talk to the grid neigboors.
 3. Line: Actors are arranged in a line. Each actor has only 2 neighboors (one left and one right, unless you are the first or last actor).
 4.Imperfect 2D Grid: Grid arrangement but one random other neighboor is selected from the list of all actors (4+1 neighboors).
 
 Nofailure_gossip.scala is asumpte that there is no node fail in the network.
 Failure_gossip.scala is assupte thate there are several node dead in the network.
