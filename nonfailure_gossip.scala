import akka.actor._
import scala.math._
import scala.util.Random
import akka.actor.Actor._

object project2 {
     
    val system =ActorSystem("pyproject2")
     val b = System.currentTimeMillis               
     var Numofnode =0;          
    def main(args: Array[String]) { 
               if (args.length != 3) {
	                  println("please use 3 args")	                  
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
       
              
               var sizeOfnodeNet = args(0).toDouble

              if (args(1).toString == "2D" || args(1).toString == "imp2D")// use the square
            	   sizeOfnodeNet = ceil(sqrt(sizeOfnodeNet))*ceil(sqrt(sizeOfnodeNet))
	 
                Numofnode = sizeOfnodeNet.toInt 
                //println(Numofnode)
               val nodeNet = new Array[ActorRef](Numofnode+1) // // start from 1
                
               def index2D(x:Int, y:Int, dim:Int):Int =  (y-1)*dim + x //index of an 2D web

               def random_full(n:Int):Int = {  
            		   val r = new Random()
            		   r.nextInt(n)+1   
               }
               val manager1=system.actorOf(Props[manager])
               // start the nodeNet
               for (i <- 1 to Numofnode) {
                   val node=system.actorOf(Props(new Node(i, Numofnode, args(1).toString, nodeNet, manager1)))
                  
            	   nodeNet(i) = node 
               }
               val startingNode = random_full(Numofnode) // pick a starting node randomly
            		   println(startingNode+" "+Numofnode)

               if (args(2).toString == "gossip"){
                     nodeNet(Numofnode/2) ! "Rumor"
            	 //  println("first rumor send"+nodeNet(startingNode))
               }
               else if (args(2).toString == "push-sum") {
            	   //println (Numofnode)
            	   for (i <- 1 to Numofnode) {
            		   nodeNet(i) ! "newRound" 
            		   println("node"+i+"start")
            	   }
               }
               else println("unknown algorithm")
               

  }
    class manager extends Actor {
	var count = 0 // count the converge nodes
	var totalRadio = 0.0 // use to check the push-sum
	var round = 1 // how many rounds the protocol go

	 def receive ={
			case id:Int => //  gossip
				count = count + 1
				//println("Node "+id+ " got the rumor.")	
				if (count >= Numofnode){ // the last node may be alone
					println("Converge! " + count + " nodes return.")
					
					val c:Long = System.currentTimeMillis
				    println("Time used: " + (c-b) + " milliseconds.")	


					Thread.sleep(400)
					sys.exit()
				}

			case sumnodeRatio:Double => // upush-sum
				count = count + 1
				totalRadio = totalRadio + sumnodeRatio
				if (count >= Numofnode){
					val c = System.currentTimeMillis
					println("Time used: " + (c-b) + " milliseconds.")
                    println("Converge! " + count + " nodes converge at "+totalRadio/count+ ".") 
					sys.exit()
				}
		}	
}

   case class Data(sum:Double, weight:Double, nodeID:Int) 
   def index2D(x:Int, y:Int, dim:Int):Int =  (y-1)*dim + x 
   
    class Node(NodeID:Int, totalNode:Int, topology:String,nodeNet:Array[ActorRef],manager1:ActorRef) extends Actor {
    	private var nodeSum = NodeID.toDouble
    			//println(nodeSum)
    	private var nodeWeight = 1.0
    	private var RumorCount = 0 
    	private var nodeRatio = nodeSum/nodeWeight
    	private var nodeRatioCount = 0.0
    	private val imp2D_neighbor = random_full(totalNode);
    	private var online = true; 
    	private var totalNeighbor = 0
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
							
			case _ => 	 List[Int]() // indicating error
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
												
				case "newRound" => 
					if(online) {	
					  Thread.sleep(10)
						//println("newRound :Node "+NodeID + " starts. "+nodeSum+nodeWeight)				
						nodeNet(getRandomNeighbor()) ! Data(nodeSum/2.0, nodeWeight/2.0, NodeID) // send half out
						nodeSum = nodeSum/2.0 // keep half
						nodeWeight =nodeWeight/2.0	
						

					}	
					
								
				case Data(sum, weight, id) =>
					if(online) {
						nodeSum = nodeSum + sum
						nodeWeight = nodeWeight + weight	
						nodeNet(getRandomNeighbor()) ! Data(nodeSum/2.0, nodeWeight/2.0, NodeID) // send half out
						nodeSum = nodeSum/2.0 // keep half
						nodeWeight =nodeWeight/2.0						
						if (abs(nodeRatio - nodeSum/nodeWeight) < 1e-10){ // the difference
							nodeRatioCount = nodeRatioCount +1
							if(nodeRatioCount >= 10) {							
								nodeRatio = nodeSum/nodeWeight // update
								manager1 ! nodeRatio // report converge
								//println("Node "+NodeID + " Converge. local sum: " + nodeSum + " local weight: " + nodeWeight+ "s/w=" + nodeSum/nodeWeight)
								online = false // stop 
							}
						
						}
						else {
							nodeRatioCount = 0
						}
															
						
						nodeRatio = nodeSum/nodeWeight // update the ratio
						
					}
					else{ 				
						nodeSum = nodeSum + sum
						nodeWeight = nodeWeight + weight									
						nodeNet(getRandomNeighbor()) ! Data(nodeSum/2.0, nodeWeight/2.0, NodeID) // send half out
						nodeSum = nodeSum/2.0 // keep half
						nodeWeight =nodeWeight/2.0	
					}
				
				case "Rumor"=> // receive rumor 
				   //println("rumor received")
					if(online){
						RumorCount = 	RumorCount +1										
						if(RumorCount >= 10) {

							online = false	// stop this node 	
														
							if(topology == "line"){ //reference paper
								if (NodeID != 1 && NodeID != totalNode){
									nodeNet(NodeID-1) ! "Rumor"
									nodeNet(NodeID+1) ! "Rumor"	
								}
							}					
						}
						if(RumorCount==1) {
							println("Node "+NodeID + " receive rumor.")
							manager1 ! NodeID
							self ! "repeatRumor"
						}
						
					}
					
				case "repeatRumor" => // start propagation in a new round
					if(online){
						if(RumorCount>0){ // I have the rumor
							nodeNet(getRandomNeighbor()) ! "Rumor" // tells the rumor
							//Thread.sleep(50)
							self ! 	"repeatRumor"
						}
					}           
         }
    }   
}
