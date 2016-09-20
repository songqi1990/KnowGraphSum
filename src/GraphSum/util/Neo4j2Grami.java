package GraphSum.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.TreeMap;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import GraphSum.algo.*;

public class Neo4j2Grami {
	public Label_Index labelIndex;
	public static Neo4jGraph dg;
	public Hashtable<String, Integer> nodeLabelCount;
	public HashSet<String> maxLabelSet;
	public Hashtable<String, Integer> LabelId;
	public Neo4j2Grami(Neo4jGraph G) throws IOException{
		labelIndex = new Label_Index(G);
		maxLabelSet = new HashSet<String>();
		LabelId = new Hashtable<String, Integer>();
		BuildLabelIdx();
		nodeLabelCount = labelIndex.GetNodeLabelCount();
	}
	public void CalculateMaxLabel(Node a){
		String maxLabel = "";
		int maxLabelFrequency = 0;
		if(dg.maxLabelofNode.containsKey(a.getId()))
			return;
		else{
			for(Label l: a.getLabels()){
				if(nodeLabelCount.get(l.toString()) > maxLabelFrequency){
					maxLabel = l.toString();
					maxLabelFrequency = nodeLabelCount.get(maxLabel);	
				}
			}
			dg.maxLabelofNode.put(a.getId(), maxLabel);
			maxLabelSet.add(maxLabel);
		}
	}
	
	public void BuildLabelIdx() throws IOException{
		if(labelIndex.invIndex.size()==0){
			labelIndex.buildLabelIndex(false);
		}
		System.out.println("Label index build success");
	}
	
	public static void main(String args[]) throws IOException{
		TreeMap<Long,Long> nodeId = new TreeMap<Long,Long>();	//Original node id, new node id
		TreeMap<Long,Long> revnodeId = new TreeMap<Long,Long>();	//new node id, original node id
		String filename = "/Users/qsong/Downloads/yago.db";
		dg = new Neo4jGraph(filename,2);
		FileWriter fw,fw1,fw2,fw3;
		PrintWriter pw,pw1,pw2,pw3;
		fw = new FileWriter("/Users/qsong/Downloads/snodeId_label_labelId.txt");
		pw = new PrintWriter(fw);
		fw1 = new FileWriter("/Users/qsong/Downloads/snodeMatch.txt");
		pw1 = new PrintWriter(fw1);
		fw2 = new FileWriter("/Users/qsong/Downloads/sresnodeMatch.txt");
		pw2 = new PrintWriter(fw2);
		fw3 = new FileWriter("/Users/qsong/Downloads/syago.lg");
		pw3 = new PrintWriter(fw3);
		Long consistentNodeId = (long)1;
		
		try(Transaction tx1 = dg.getGDB().beginTx() ){
			Neo4j2Grami neo4j2grami = new Neo4j2Grami(dg);
			for(Node n : dg.getAllNodes())
				neo4j2grami.CalculateMaxLabel(n);
			System.out.println("calculate success");
			int labelId = 1;
			for(String s : neo4j2grami.maxLabelSet){
				neo4j2grami.LabelId.put(s, labelId++);
			}
			for(Node n : dg.getAllNodes()){
				pw.print(n.getId() + "\t");
//				System.out.print(n.getId() + " ");
				pw.print(dg.maxLabelofNode.get(n.getId()) + "\t");
				pw.println(neo4j2grami.LabelId.get(dg.maxLabelofNode.get(n.getId())));
//				System.out.print(dg.maxLabelofNode.get(n.getId()) + " ");
//				if(neo4j2grami.nodeLabelId.get(dg.maxLabelofNode.get(n.getId())) < 10)
//					System.out.println(neo4j2grami.nodeLabelId.get(dg.maxLabelofNode.get(n.getId())));
			}
			for(Node n : dg.getAllNodes()){
				if(n.getDegree() == 0)
					continue;
				if(nodeId.containsKey(n.getId()))
					continue;
				else{
					nodeId.put(n.getId(), consistentNodeId);
					revnodeId.put(consistentNodeId++, n.getId());
				}
			}
			for(Long key : nodeId.keySet()){
				pw1.println(key + "\t" + nodeId.get(key));
//				System.out.println(key + " " + nodeId.get(key));
			}
			for(Long key : revnodeId.keySet())
				pw2.println(key + "\t" + revnodeId.get(key));
			pw3.println("t ＃ 0");
			for(Long key : revnodeId.keySet()){
//				pw3.println("v " + key + " " + neo4j2grami.LabelId.get(dg.maxLabelofNode.get(revnodeId.get(key))));
				pw3.println(neo4j2grami.LabelId.get(dg.maxLabelofNode.get(revnodeId.get(key))));
			}
			for(Relationship s : dg.getAllEdges()){
//				pw3.println("e " + nodeId.get(s.getStartNode().getId()) + " " + nodeId.get(s.getEndNode().getId()) + " 1");
				pw3.println(nodeId.get(s.getStartNode().getId()) + "," + nodeId.get(s.getEndNode().getId()) + ",0");
			}
			fw.close();
			pw.close();
			fw1.close();
			pw1.close();
			fw2.close();
			pw2.close();
			fw3.close();
			pw3.close();
			tx1.success();
		}
	}
	
}

