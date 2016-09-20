//this is 1 and 2 hop neighborhood label index
package GraphSum.algo;

import java.util.HashSet;
import java.util.Hashtable;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class NeiLabelIndex {
	Hashtable<Long,Hashtable<String,Integer>> hop1IndexOut; //store 1-hop label index for all nodes (for outgoing edge)
	Hashtable<Long,Hashtable<String,Integer>> hop2IndexOut; //store 2-hop label index for all nodes
	Hashtable<String,Hashtable<String,Integer>> hop1IndexPatternOut;	//store 1-hop label index for this pattern node
	Hashtable<String,Hashtable<String,Integer>> hop2IndexPatternOut;	//store 2-hop label index for this pattern node
	
	Hashtable<Long,Hashtable<String,Integer>> hop1IndexIn; //store 1-hop label index for all nodes (for incoming edge)
	Hashtable<Long,Hashtable<String,Integer>> hop2IndexIn; //store 2-hop label index for all nodes
	Hashtable<String,Hashtable<String,Integer>> hop1IndexPatternIn;	//store 1-hop label index for this pattern node
	Hashtable<String,Hashtable<String,Integer>> hop2IndexPatternIn;	//store 2-hop label index for this pattern node
	
	public NeiLabelIndex(){
		hop1IndexOut = new Hashtable<Long,Hashtable<String,Integer>>();
		hop2IndexOut = new Hashtable<Long,Hashtable<String,Integer>>();
		hop1IndexPatternOut = new Hashtable<String,Hashtable<String,Integer>>();
		hop2IndexPatternOut = new Hashtable<String,Hashtable<String,Integer>>();
		
		hop1IndexIn = new Hashtable<Long,Hashtable<String,Integer>>();
		hop2IndexIn = new Hashtable<Long,Hashtable<String,Integer>>();
		hop1IndexPatternIn = new Hashtable<String,Hashtable<String,Integer>>();
		hop2IndexPatternIn = new Hashtable<String,Hashtable<String,Integer>>();
	}
	
	public void calNeighborLabelIndexOut(Neo4jGraph dg){
		System.out.println("Start building 1-hop and 2-hop neighbor lable index (outgoing)");
		HashSet<Node> hop1Node = new HashSet<Node>();
		HashSet<Node> hop2Node = new HashSet<Node>();
		for(Node n : dg.ggdb.getAllNodes()){
			hop1Node.clear();
			hop2Node.clear();
			if(!hop1IndexOut.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop1IndexOut.put(n.getId(), ll);
			}
			if(!hop2IndexOut.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop2IndexOut.put(n.getId(), ll);
			}
			for(Relationship r : n.getRelationships(Direction.OUTGOING)){
				hop1Node.add(r.getEndNode());
				String neiLabel = dg.maxLabelofNode.get(r.getEndNode().getId());
				if(!hop1IndexOut.get(n.getId()).containsKey(neiLabel)){
					hop1IndexOut.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop1IndexOut.get(n.getId()).get(neiLabel)+1;
					hop1IndexOut.get(n.getId()).remove(neiLabel);
					hop1IndexOut.get(n.getId()).put(neiLabel, newFrequency);
				}
			}
			for(Node hop1 : hop1Node){
				for(Relationship r : hop1.getRelationships(Direction.OUTGOING)){
					hop2Node.add(r.getEndNode());
				}
			}
			for(Node hop2 : hop2Node){
				String neiLabel = dg.maxLabelofNode.get(hop2.getId());
				if(!hop2IndexOut.get(n.getId()).containsKey(neiLabel)){
					hop2IndexOut.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop2IndexOut.get(n.getId()).get(neiLabel)+1;
					hop2IndexOut.get(n.getId()).remove(neiLabel);
					hop2IndexOut.get(n.getId()).put(neiLabel, newFrequency);
				}		
			}
		}
		System.out.println("1-hop and 2-hop neighbor lable index build success (outgoing)");
	}
	
	public void calNeighborLabelIndexIn(Neo4jGraph dg){
		System.out.println("Start building 1-hop and 2-hop neighbor lable index (incoming)");
		HashSet<Node> hop1Node = new HashSet<Node>();
		HashSet<Node> hop2Node = new HashSet<Node>();
		for(Node n : dg.ggdb.getAllNodes()){
			hop1Node.clear();
			hop2Node.clear();
			if(!hop1IndexIn.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop1IndexIn.put(n.getId(), ll);
			}
			if(!hop2IndexIn.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop2IndexIn.put(n.getId(), ll);
			}
			for(Relationship r : n.getRelationships(Direction.INCOMING)){
				hop1Node.add(r.getStartNode());
				String neiLabel = dg.maxLabelofNode.get(r.getStartNode().getId());
				if(!hop1IndexIn.get(n.getId()).containsKey(neiLabel)){
					hop1IndexIn.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop1IndexIn.get(n.getId()).get(neiLabel)+1;
					hop1IndexIn.get(n.getId()).remove(neiLabel);
					hop1IndexIn.get(n.getId()).put(neiLabel, newFrequency);
				}
			}
			for(Node hop1 : hop1Node){
				for(Relationship r : hop1.getRelationships(Direction.INCOMING)){
					hop2Node.add(r.getStartNode());
				}
			}
			for(Node hop2 : hop2Node){
				String neiLabel = dg.maxLabelofNode.get(hop2.getId());
				if(!hop2IndexIn.get(n.getId()).containsKey(neiLabel)){
					hop2IndexIn.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop2IndexIn.get(n.getId()).get(neiLabel)+1;
					hop2IndexIn.get(n.getId()).remove(neiLabel);
					hop2IndexIn.get(n.getId()).put(neiLabel, newFrequency);
				}
			}
		}
		System.out.println("1-hop and 2-hop neighbor lable index build success (incoming)");
	}
}
