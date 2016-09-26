package wsu.eecs.mlkd.KGQuery.algo;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;


public class PageRank {
	private int iterations = 100;

	public void calculatePageRank(GraphDatabaseService graphDb) {

		
		double q = 0.15;

		
		// Initialize

		Iterable<Node> allVertices = GlobalGraphOperations.at(graphDb).getAllNodes();
		int N = 0;
		int m = 0;
		for (Node v : allVertices) {
			N++;
			if (m < (int) v.getId()) m = (int) v.getId();
		}
		
		double[] pagerank = new double[m+1];
		double[] aux = new double[m+1];
		Node[] nodes = new Node[m+1];
		System.out.println("Number of Vertices is " + N);
		double s = 1.0 / N;
		for (int i = 0; i < pagerank.length; i++) {
			pagerank[i] = s;
			aux[i] = 0;
		}
		
		for (Node v : allVertices) {
			nodes[(int) v.getId()] = v;
		}


		// Iterations
		
		System.err.println("Computing PageRank:");

		for (int iter = 0; iter < iterations; iter++) {
			
			System.err.println("  Iteration " + (iter+1) + "/" + iterations);

			double auxSum = 0;


			// Compute the next iteration of PageRank and store it as aux

			for (Node n : nodes) {

				double a = 0;
				int c = 0;
				Iterable<Relationship> ri = n.getRelationships(Direction.INCOMING);
				for (Relationship r : ri) {
					a += pagerank[(int) r.getOtherNode(n).getId()];
					c++;
				}
				if (c > 0) a /= c;

				double x = (q / N) + (1.0 - q) * a;
				auxSum += x;
				aux[(int) n.getId()] = x;
			}


			// Store and normalize the ProvRank

			for (int i = 0; i < pagerank.length; i++) {
				pagerank[i] = aux[i] / auxSum;
				aux[i] = 0;
			}
		}


		//for(int i=0; i<m; i++)
			//System.out.println("PageRank value of "+ i + " is " + pagerank[i]);
	}
}
