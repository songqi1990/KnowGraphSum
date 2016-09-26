package wsu.eecs.mlkd.KGQuery.algo.VF2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

public class VF2Matcher {
	
	public ArrayList<Node> nodes = new ArrayList<Node>();
	public ArrayList<Relationship> edges = new ArrayList<Relationship>();
	// finds all subgraph isomorphisms and prints them to the console
		// modelGraph is the big graph
		// patternGraph is the small graph which is searched for in the big one
	public void match(GraphDatabaseService modelGraph, GraphDatabaseService patternGraph) {
		
		VF2State state = new VF2State(modelGraph, patternGraph);
		this.matchInternal(state, modelGraph, patternGraph); 
		this.supportCalculate(state);
		this.coverageCalculate(state);
	}
	
	
	//calculate support based on the match
	public void supportCalculate(VF2State s){
		Vector<Integer> matchEachNode = new Vector<Integer>();
		int support = 0;
		System.out.println(s.matchNumber);
		for(int j = 0; j < s.patternSize ; j++){
			matchEachNode.clear();
			for( int k = 0 ; k < s.matchNumber ; k++){
				if (!matchEachNode.contains(s.resultMatrix[k][j]))
					matchEachNode.addElement(s.resultMatrix[k][j]);
			}
			if(support == 0)
				support = matchEachNode.size();
			else
				support = matchEachNode.size()<support?matchEachNode.size():support;
		}
		System.out.println("support = " + support);
	}
	
	public void coverageCalculate(VF2State s){
		System.out.println("coverage = " + (float)(s.nodeResult.size())/s.modelSize);
	}
	
	// internal method for finding subgraphs. called recursively
	private void matchInternal(VF2State s, GraphDatabaseService modelGraph, GraphDatabaseService patternGraph) {
		// abort search if we reached the final level of the search tree 
		int patternSize = 0;
		GlobalGraphOperations globalOperation1 = GlobalGraphOperations.at(patternGraph);
		for (Node n : globalOperation1.getAllNodes()) {
			patternSize ++;
		}
		if (s.depth == patternSize) {
			s.printMapping(); // all pattern nodes matched -> print solution
			s.storeMapping();
		} 
		else
		{
			// get candidate pairs
			Map<Integer,Integer> candiatePairs = this.getCandidatePairs(s, modelGraph, patternGraph);
			//System.out.println(candiatePairs);
			// iterate through candidate pairs
			if(candiatePairs!=null)
			for (Integer n : candiatePairs.keySet()) {
				int m = candiatePairs.get(n);
				// check if candidate pair (n,m) is feasible 
				if (checkFeasibility(s,n,m)) {
					s.match(n, m); // extend mapping
					matchInternal(s, modelGraph, patternGraph); // recursive call
					s.backtrack(n, m); // remove (n,m) from the mapping
					
				}
			}
		}
	}
	
	// determines all candidate pairs to be checked for feasibility
	private Map getCandidatePairs(VF2State s, GraphDatabaseService m, GraphDatabaseService p) {
		if (s.depth == 0)
			// the first time the Tin and Tout sets are not yet initialized.
			return this.pairGenerator(s.unmapped1, s.unmapped2);
		if (s.T1in.size() > 0 && s.T2in.size() > 0) {
			Map inmap = this.pairGenerator(s.T1in, s.T2in);
			if (inmap.size() > 0)
				return inmap;
		}
		if (s.T1out.size() > 0 && s.T2out.size() > 0) {
			Map outmap = this.pairGenerator(s.T1out, s.T2out);
			return outmap;
		}
		return null;
	}
	
	// generates pairs of nodes
		// outputs a map from model nodes to pattern nodes
		private Map pairGenerator(Collection<Integer> modelNodes , Collection<Integer> patternNodes) {
			
			TreeMap<Integer,Integer> map = new TreeMap<Integer,Integer>(); // the map storing candidate pairs
			
			// find the largest among all pattern nodes (the one with the largest ID)!
			// Note: it does not matter how to choose a node here. The only important thing is to have a total order, i.e., to uniquely choose one node. If you do not do this, you might get multiple redundant states having the same pairs of nodes mapped. The only difference will be the order in which these pairs have been included (but the order does not change the result, so these states are all the same!).
			int nextPatternNode = -1;
			for (Integer i : patternNodes)
				nextPatternNode = Math.max(nextPatternNode, i);
			// generate pairs of all model graph nodes with the designated pattern graph node
			for (Integer i : modelNodes)
//				if(nextPatternNode >=0)
					map.put(i, nextPatternNode);
			return map; // return node pairs
		}
		
		// checks whether or not it makes sense to extend the mapping by the pair (n,m)
		// n is a model graph node
		// m is a pattern graph node
		private Boolean checkFeasibility(VF2State s , int n , int m) {
			
			return checkSemanticFeasibility(s, n, m)&&checkSyntacticFeasibility(s, n, m); // return result
		}
		//checks for semantic feasibility of the pair (n,m)
		private Boolean checkSemanticFeasibility(VF2State s, int n, int m){
			HashSet<String> ns = new HashSet<String>();
			HashSet<String> ms = new HashSet<String>();
			for(Label l : s.modelGraph.getNodeById(n).getLabels()){
				ns.add(l.name());
			}
			for(Label l : s.patternGraph.getNodeById(m).getLabels())
				ms.add(l.name());
			if(ns.containsAll(ms))
				return true;
			else
				return false;
		}
		
		//checks for syntactic feasibility of the pair (n,m)
		private Boolean checkSyntacticFeasibility(VF2State s, int n, int m){
			Boolean passed = true;
			passed = passed && checkRpredAndRsucc(s,n,m); // check Rpred / Rsucc conditions (subgraph isomorphism definition)
			passed = passed && CheckRin(s,n,m);
			passed = passed && CheckRout(s,n,m);
			passed = passed && CheckRnew(s,n,m);
			return passed; // return result	
			}
		
		// checks if extending the mapping by the pair (n,m) would violate the subgraph isomorphism definition
		private Boolean checkRpredAndRsucc(VF2State s , int n , int m) {
			
			Boolean passed = true;
			HashSet<Integer> IngoingVertices = new HashSet<Integer>();
			HashSet<Integer> OutgoingVertices = new HashSet<Integer>();
			// check if the structure of the (partial) model graph is also present in the (partial) pattern graph 
			// if a predecessor of n has been mapped to a node n' before, then n' must be mapped to a predecessor of m 
//			Node nTmp = s.modelGraph.getNodeById(n);
//			for (Relationship e : nTmp.getRelationships(Direction.INCOMING)) {
//				IngoingVertices.clear();
//				if (s.core_1[(int)e.getStartNode().getId()] > -1) {
//					//passed = passed && (s.patternGraph.getAdjacencyMatrix()[s.core_1[e.source.id]][m] == 1);
//					for(Relationship re : s.patternGraph.getNodeById(m).getRelationships(Direction.INCOMING)){
//						IngoingVertices.add((int)re.getStartNode().getId());
//					}
//					passed = passed && (IngoingVertices.contains(s.core_1[(int)e.getStartNode().getId()]));
//				}
//			}
//			// if a successor of n has been mapped to a node n' before, then n' must be mapped to a successor of m
//			for (Relationship e : nTmp.getRelationships(Direction.OUTGOING)) {
//				OutgoingVertices.clear();
//				if (s.core_1[(int)e.getEndNode().getId()] > -1) {
//					//passed = passed && (s.patternGraph.getAdjacencyMatrix()[m][s.core_1[e.target.id]] == 1);
//					for(Relationship re : s.patternGraph.getNodeById(m).getRelationships(Direction.OUTGOING)){
//						OutgoingVertices.add((int)re.getEndNode().getId());
//					}
//					passed = passed && (OutgoingVertices.contains(s.core_1[(int)e.getEndNode().getId()]));
//				}
//			}
			// check if the structure of the (partial) pattern graph is also present in the (partial) model graph
			// if a predecessor of m has been mapped to a node m' before, then m' must be mapped to a predecessor of n
			Node mTmp = s.patternGraph.getNodeById(m);
			for (Relationship e : mTmp.getRelationships(Direction.INCOMING)) {
				IngoingVertices.clear();
				if (s.core_2[(int)e.getStartNode().getId()] > -1) {
					//passed = passed && (s.modelGraph.getAdjacencyMatrix()[s.core_2[e.source.id]][n] == 1);
					for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.INCOMING)){
						IngoingVertices.add((int)re.getStartNode().getId());
					}
					passed = passed && (IngoingVertices.contains(s.core_2[(int)e.getStartNode().getId()]));
				}
			}
			// if a successor of m has been mapped to a node m' before, then m' must be mapped to a successor of n
			for (Relationship e : mTmp.getRelationships(Direction.OUTGOING)) {
				OutgoingVertices.clear();
				if (s.core_2[(int)e.getEndNode().getId()] > -1) {
					//passed = passed && (s.modelGraph.getAdjacencyMatrix()[n][s.core_2[e.target.id]] == 1);
					for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.OUTGOING)){
						OutgoingVertices.add((int)re.getEndNode().getId());
					}
					passed = passed && (OutgoingVertices.contains(s.core_2[(int)e.getEndNode().getId()]));
				}
			}
			return passed; // return the result
		}
		
		
		public Boolean CheckRin(VF2State s, int n, int m){
			
			HashSet<Integer> IngoingVertices = new HashSet<Integer>();
			HashSet<Integer> OutgoingVertices = new HashSet<Integer>();
			
			IngoingVertices.clear();
			for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.INCOMING)){
				IngoingVertices.add((int)re.getStartNode().getId());
			}
			HashSet<Integer> T1in=(HashSet<Integer>) s.T1in.clone();
			T1in.removeAll(IngoingVertices);
			
			IngoingVertices.clear();
			for(Relationship re :s.patternGraph.getNodeById(m).getRelationships(Direction.INCOMING)){
				IngoingVertices.add((int)re.getStartNode().getId());
			}
			HashSet<Integer> T2in=(HashSet<Integer>) s.T2in.clone();
			T2in.removeAll(IngoingVertices);
			Boolean firstExp= T1in.size()>=T2in.size();
			
			OutgoingVertices.clear();
			for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.OUTGOING)){
				OutgoingVertices.add((int)re.getEndNode().getId());
			}
			HashSet<Integer> T1in2=(HashSet<Integer>) s.T1in.clone();
			T1in2.removeAll(OutgoingVertices);
			
			OutgoingVertices.clear();
			for(Relationship re : s.patternGraph.getNodeById(m).getRelationships(Direction.OUTGOING)){
				OutgoingVertices.add((int)re.getEndNode().getId());
			}
			HashSet<Integer> T2in2=(HashSet<Integer>) s.T2in.clone();
			T2in2.removeAll(OutgoingVertices);
			Boolean secoundExp= T1in2.size()>=T2in2.size();
			return firstExp && secoundExp;
			}
		
		public Boolean CheckRout(VF2State s, int n, int m){
			HashSet<Integer> IngoingVertices = new HashSet<Integer>();
			HashSet<Integer> OutgoingVertices = new HashSet<Integer>();
			
			IngoingVertices.clear();
			
			for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.INCOMING)){
				IngoingVertices.add((int)re.getStartNode().getId());
			}
			HashSet<Integer> T1out=(HashSet<Integer>) s.T1out.clone();
			T1out.retainAll(IngoingVertices);
			
			IngoingVertices.clear();
			for(Relationship re : s.patternGraph.getNodeById(m).getRelationships(Direction.INCOMING)){
				IngoingVertices.add((int)re.getStartNode().getId());
			}
			HashSet<Integer> T2out=(HashSet<Integer>) s.T2out.clone();
			T2out.retainAll(IngoingVertices);
			Boolean firstExp= T1out.size()>=T2out.size();
			
			OutgoingVertices.clear();
			for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.OUTGOING)){
				OutgoingVertices.add((int)re.getEndNode().getId());
			}
			HashSet<Integer> T1out2=(HashSet<Integer>) s.T1in.clone();
			T1out2.retainAll(OutgoingVertices);
			
			OutgoingVertices.clear();
			for(Relationship re : s.patternGraph.getNodeById(m).getRelationships(Direction.OUTGOING)){
				OutgoingVertices.add((int)re.getEndNode().getId());
			}
			HashSet<Integer> T2out2=(HashSet<Integer>) s.T2in.clone();
			T2out2.retainAll(OutgoingVertices);
			Boolean secoundExp= T1out2.size()>=T2out2.size();
			return firstExp && secoundExp;
		}
		
		
		public Boolean CheckRnew(VF2State s, int n, int m){
			
			HashSet<Integer> IngoingVertices = new HashSet<Integer>();
			HashSet<Integer> OutgoingVertices = new HashSet<Integer>();
			
			IngoingVertices.clear();
			for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.INCOMING)){
				IngoingVertices.add((int)re.getStartNode().getId());
			}
			HashSet<Integer> TNilt1=calcNTilt1(s);
			TNilt1.retainAll(IngoingVertices);
			
			IngoingVertices.clear();
			for(Relationship re : s.patternGraph.getNodeById(m).getRelationships(Direction.INCOMING)){
				IngoingVertices.add((int)re.getStartNode().getId());
			}
			HashSet<Integer> TNilt2=calcNTilt2(s);
			TNilt2.retainAll(IngoingVertices);
			Boolean firstExp= TNilt1.size()>=TNilt2.size();
			
			OutgoingVertices.clear();
			for(Relationship re : s.modelGraph.getNodeById(n).getRelationships(Direction.OUTGOING)){
				OutgoingVertices.add((int)re.getEndNode().getId());
			}
			HashSet<Integer> NTilt12=calcNTilt1(s);
			NTilt12.retainAll(OutgoingVertices);
			
			OutgoingVertices.clear();
			for(Relationship re : s.patternGraph.getNodeById(m).getRelationships(Direction.OUTGOING)){
				OutgoingVertices.add((int)re.getEndNode().getId());
			}
			HashSet<Integer> NTilt22=calcNTilt2(s);
			NTilt22.retainAll(OutgoingVertices);
			Boolean secoundExp= NTilt12.size()>=NTilt22.size();
			return firstExp && secoundExp;		
		}
		
		private HashSet<Integer> calcNTilt1(VF2State s){
			
			GlobalGraphOperations globalOperation1 = GlobalGraphOperations.at(s.modelGraph);
			if (nodes.isEmpty()){
				for (Node n : globalOperation1.getAllNodes()){
					nodes.add(n);
				}
			}
			HashSet<Integer> N1= new HashSet(nodes);
			HashSet<Integer> M1 = new HashSet();
			for (int node:s.core_1)
				M1.add(node);
			HashSet<Integer> T1 = (HashSet<Integer>) s.T1in.clone();
			T1.retainAll(s.T1out);
			HashSet<Integer> NTilt1=N1;
			NTilt1.removeAll(M1);
			NTilt1.removeAll(T1);
			return NTilt1;
		}
		
		private HashSet<Integer> calcNTilt2(VF2State s){
			GlobalGraphOperations globalOperation1 = GlobalGraphOperations.at(s.modelGraph);
			if (nodes.isEmpty()){
				for (Node n : globalOperation1.getAllNodes()){
					nodes.add(n);
				}
			}
			HashSet<Integer> N2= new HashSet(nodes);
			HashSet<Integer> M2 = new HashSet();
			for (int node:s.core_2)
			M2.add(node);
			HashSet<Integer> T2 = (HashSet<Integer>) s.T2in.clone();
			T2.retainAll(s.T2out);
			HashSet<Integer> NTilt2=N2;
			NTilt2.removeAll(M2);
			NTilt2.removeAll(T2);
			return NTilt2;
		}
}

