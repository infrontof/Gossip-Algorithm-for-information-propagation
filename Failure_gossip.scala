import akka.actor._
import scala.math._
import scala.util.Random
import akka.actor.Actor._

object project2bonus {
     
    val system =ActorSystem("pyproject2bonus")
     val b = System.currentTimeMillis               
     var Numofnode =0; 
    var failNumofnode:Long=0
    def main(args: Array[String]) { 
               if (args.length != 4) {
	                  println("please use 4 args")
	                  println("Please use: scala project2.scala number_of_nodes nodeNet_topology algorithm number_of_failnodes")
	                  sys.exit()
                    }
               if(args(0).toInt <= 1){
            	   println("number of nodes shoulde larger than 1")
            	   sys.exit()	
               }
               if(args(1). toString !="full" && args(1).toString!="line" && args(1).toString!="2D" && args(1).toString!="imp2D"){
            	   println("the nodeNet topplogic should be full, 2D, line or imp2D")
            	   sys.exit()
               }
               if(args(2).toString!="gossip" && args(2).toString!="push-sum"){
            	   println("algorithm shoulbe gossip or push-sum")
            	   sys.exit()
               }
               if(args(3).toLong >= args(0).toLong || args(3).toLong<1){
            	   println("number of failed nodes should be larger than 1 less than number of nodes")
            	   sys.exit()	
               }
               
              failNumofnode=args(3).toLong
               var sizeOfnodeNet = args(0).toDouble

               if (args(1).toString == "2D" || args(1).toString == "imp2D") //use the square
            	   sizeOfnodeNet = ceil(sqrt(sizeOfnodeNet))*ceil(sqrt(sizeOfnodeNet))
	 
                Numofnode = sizeOfnodeNet.toInt // total num of nodes in the nodeNet
                println(Numofnode)
               val nodeNet = new Array[ActorRef](Numofnode+1) // start from 1
                
               def index2D(x:Int, y:Int, dim:Int):Int =  (y-1)*dim + x //index of an 2D web	

               def random_full(n:Int):Int = {  
            		   val r = new Random()
            		   r.nextInt(n)+1   
               }
              
               val manager1=system.actorOf(Props[manager])
               for (i <- 1 to Numofnode) {
                   val node=system.actorOf(Props(new Node(i, Numofnode, args(1).toString, nodeNet, manager1)))
             	   nodeNet(i) = node 
               }

               val startingNode = random_full(Numofnode) // pick a starting node randomly
   //         		   println(startingNode+" "+Numofnode)

                for (i <- 1L to args(3).toLong){ // randomly pick node to fail
            	   val failNode = random_full(Numofnode)
            			   println("Node " + failNode +" failed.")
            			   nodeNet(failNode) ! "Fail"
               }              
               if (args(2).toString == "gossip"){
                     nodeNet(Numofnode/2) ! "Rumor"
               }
               else if (args(2).toString == "push-sum") {
            	   for (i <- 1 to Numofnode) {
            		   nodeNet(i) ! "newRound" 
  //          		   println("node"+i+"start")
            	   }
               }
               else println("unknown algorithm")
               


  }
    
    
    class manager extends Actor {
	var count = 0 
	var totalRadio = 0.0 
	var round = 1 
	
	 def receive ={
			case id:Int => // gossip
				count = count + 1
				if (count >= Numofnode-failNumofnode){ // the last node may be alone
					println("Converge! " + count + " nodes return.")
					val c:Long = System.currentTimeMillis
				    println("Time used: " + (c-b) + " milliseconds.")	
                    
					Thread.sleep(400)
					sys.exit()
				}

			case nodeRatio:Double => //  push-sum
				count = count + 1
				totalRadio = totalRadio + nodeRatio
				if (count >= Numofnode-failNumofnode){ // the last node may be alone
					val c = System.currentTimeMillis
					println("Time used: " + (c-b) + " milliseconds.")
                    println("Converge! " + count + " nodes converge at "+totalRadio/count+" ."+failNumofnode+"node failed" ) 

					sys.exit()
				}

		}
	
}
  
   case class Data(sum:Double, weight:Double, nodeID:Int) 
   def index2D(x:Int, y:Int, dim:Int):Int =  (y-1)*dim + x 
   
    class Node(NodeID:Int, totalNode:Int, topology:String,nodeNet:Array[ActorRef],manager1:ActorRef) extends Actor {
    	private var nodeSum = NodeID.toDouble
    			//println(nodeSum)

    	private val imp2D_neighbor = random_full(totalNode);
    	private var online = true; // use to simulate on and off of a node
    	private var totalNeighbor = 0
        private var isFail = false // for failure model
    	private var nodeWeight = 1.0
    	private var nodeRumorCount = 0 
    	private var nodeRatio = nodeSum/nodeWeight
    	private var nodeRatioCount = 0.0
    	private val NeighborList:List[Int] = {
		topology match {
			case "full" => 
				totalNeighbor = totalNode - 1
				List(0) 			
			case "2D" => 
				val dim = sqrt(totalNode.toDouble).toInt
				val x:Int = if (NodeID % dim == 0)  dim else (NodeID % dim)
				val y:Int = (NodeID-1) / dim + 1
				var Nlst = List[Int]()
				if (x - 1 > 0 ) Nlst = index2D(x-1,y,dim) :: Nlst
				if (y + 1 <= dim) Nlst = index2D(x,y+1,dim) :: Nlst
				if (x + 1 <= dim) Nlst = index2D(x+1,y,dim) :: Nlst
				if (y - 1 >0 ) Nlst = index2D(x,y-1,dim) :: Nlst
				totalNeighbor = Nlst.length
				Nlst
			
			case "line" => 
				if (NodeID == 1) 
				{
					totalNeighbor = 1
					List(2)	
				}				
				else if (NodeID == totalNode){
					totalNeighbor = 1
					List(totalNode-1)
				}
				else {
					totalNeighbor = 2
					List(NodeID-1,NodeID+1)		
				}
			
			case "imp2D" =>				
				val dim = sqrt(totalNode.toDouble).toInt
				val x:Int = if (NodeID % dim == 0)  dim else (NodeID % dim)
				val y:Int = (NodeID-1) / dim + 1
				var Nlst = List(imp2D_neighbor)
				if (x - 1 > 0 ) Nlst = index2D(x-1,y,dim) :: Nlst
				if (y + 1 <= dim) Nlst = index2D(x,y+1,dim) :: Nlst
				if (x + 1 <= dim) Nlst = index2D(x+1,y,dim) :: Nlst
				if (y - 1 >0 ) Nlst = index2D(x,y-1,dim) :: Nlst
				totalNeighbor = Nlst.length
				Nlst
							
			case _ => 	 List[Int]() // error
		}
	}
    	//println(NeighborList+ "node="+NodeID)
    	  def random_full(n:Int):Int = {
            		   val r = new Random()
            		   r.nextInt(n)+1   
               }
    	  def getRandomNeighbor():Int = {
			
    			  if (NeighborList(0)==0) random_full(totalNode)
    			  else {
    				  NeighborList(random_full(NeighborList.length)-1) // return a neighbor randomly
    			  }
	
    	  } 
    	  val mainActor = self
  	
         def receive = {
				case "Stop" => 

					sys.exit()
				case "Fail" => // this node fail
					isFail = true
					online = false
								
				case "newRound" => 
					if(online) {	
					  Thread.sleep(10)
						nodeNet(getRandomNeighbor()) ! Data(nodeSum/2.0, nodeWeight/2.0, NodeID) // send half out
						nodeSum = nodeSum/2.0 // keep half
						nodeWeight =nodeWeight/2.0	
	
					}	
					
								
				case Data(sum, weight, id) =>
					if(online) {
						if(id == -1)
						{
							nodeNet(getRandomNeighbor()) ! Data(sum, weight, NodeID) // node neighbor is off, try to send to another neighbor
						}
						else{
						nodeSum = nodeSum + sum
						nodeWeight = nodeWeight + weight									
						nodeNet(getRandomNeighbor()) ! Data(nodeSum/2.0, nodeWeight/2.0, NodeID) // send half out
						nodeSum = nodeSum/2.0 // keep half
						nodeWeight =nodeWeight/2.0						
						if (abs(nodeRatio - nodeSum/nodeWeight) < 1e-10){ // check the difference
							nodeRatioCount = nodeRatioCount +1
							if(nodeRatioCount >= 3) {							
								nodeRatio = nodeSum/nodeWeight // update the ratio
								manager1 ! nodeRatio // tells that I am converged
								//println("Node "+NodeID + " Converge. local sum: " + nodeSum + " local weight: " + nodeWeight+ "my radio"+nodeRatio) // line topo will converge to locally
								
								online = false // stop round
							}
						
						}
						else {
							nodeRatioCount = 0
						}

						
						nodeRatio = nodeSum/nodeWeight // update the ratio
						}
					}
					else{ 
						
						if(!isFail){ 
							nodeSum = nodeSum + sum
							nodeWeight = nodeWeight + weight									
							nodeNet(getRandomNeighbor()) ! Data(nodeSum/2.0, nodeWeight/2.0, NodeID) // send half out
							nodeSum = nodeSum/2.0 // keep half
							nodeWeight =nodeWeight/2.0	
						}
						else sender ! Data(sum, weight, -1) // doing this to pNlsterve energy
					}

				case "Rumor"=> // receive rumor 
				   //println("rumor received")
					if(online){
						nodeRumorCount = 	nodeRumorCount +1										
						if(nodeRumorCount >= 10) {

							online = false	
							if(topology == "line"){ //reference paper
								if (NodeID != 1 && NodeID != totalNode){
									nodeNet(NodeID-1) ! "Rumor"
									nodeNet(NodeID+1) ! "Rumor"	
								}
							}					
						}
						if(nodeRumorCount==1) {
							println("Node "+NodeID + " receive rumor.")
							manager1 ! NodeID
							self ! "repeatRumor"
						}
						
					}

				case "repeatRumor" => 
					if(online){
						if(nodeRumorCount>0){ // got the rumor
							nodeNet(getRandomNeighbor()) ! "Rumor" // tells the rumor
							self ! 	"repeatRumor"
						}
					}           
         }   	  
    }   
}
