package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import org.neo4j.graphdb.*;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;


import java.awt.*;

import javax.swing.*;

import org.jgraph.*;

import org.jgrapht.*;
import org.jgrapht.ext.*;
import org.jgrapht.graph.*;

// resolve ambiguity
import org.jgrapht.graph.DefaultEdge;

public class gpm_JGraphAdapter extends JApplet{

	private static final long serialVersionUID = 3256444702936019250L;
	private static final Color DEFAULT_BG_COLOR = Color.decode("#FAFBFF");
	private static final Dimension DEFAULT_SIZE = new Dimension(530, 320);

	//Instance fields 
	private JGraphModelAdapter<String, Relationship> jgAdapter;
	public Neo4jGraph G;

	//Methods
	gpm_JGraphAdapter(Neo4jGraph g){
		G = g;
	}
	
	@SuppressWarnings("unchecked")
	public void init()
	{
		// create a JGraphT graph
		ListenableGraph g =
			new ListenableDirectedGraph(
					DefaultEdge.class);

		// create a visualization using JGraph, via an adapter
		jgAdapter = new JGraphModelAdapter(g);

		JGraph jgraph = new JGraph(jgAdapter);

		adjustDisplaySettings(jgraph);
		getContentPane().add(jgraph);
		resize(DEFAULT_SIZE);

		String prefix = "";
		//if(G.gtype==0)
			prefix = "p";
		//else
		//	prefix = "d";
		
		for(Node n: this.G.getGGO().getAllNodes()){
			g.addVertex(prefix+n.getId());
		}
		
		for(Relationship e: this.G.getGGO().getAllRelationships()){
			g.addEdge(prefix+e.getStartNode(), prefix+e.getEndNode(), e.toString());
		}
	}

	private void adjustDisplaySettings(JGraph jg){
		jg.setPreferredSize(DEFAULT_SIZE);

		Color c = DEFAULT_BG_COLOR;
		String colorStr = null;

		try {
			colorStr = getParameter("bgcolor");
		} catch (Exception e) {
		}

		if (colorStr != null) {
			c = Color.decode(colorStr);
		}

		jg.setBackground(c);
	}

}
