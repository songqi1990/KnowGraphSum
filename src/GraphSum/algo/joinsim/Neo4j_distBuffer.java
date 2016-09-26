package wsu.eecs.mlkd.KGQuery.algo.joinsim;


import java.util.HashMap;

public class Neo4j_distBuffer {

	//structure of dbuffer: 
	//string edge --> hashmap: color, array: distance, frequency, incmark
	public String bname;
	public int maxSize = 0;
	public HashMap<String, HashMap<Integer, int[]>> distMap = null;
	//	public HashSet<int[]> minrankSet = null;
	//	public HashSet<int[]> maxrankSet = null;
	public int minrank;
	public int maxrank;

	public Neo4j_distBuffer(String gn){
		bname = gn.substring(0, gn.lastIndexOf("."))+"_dM.txt";
		distMap = new HashMap<String, HashMap<Integer, int[]>>();
	}

	//	============================================================================//
	//	This function is the main insertion algorithm
	//	============================================================================//
	public void insertDist(int mode, long fntag, long tntag, short dist, int color, boolean incins){
		int dists = getDist(fntag, tntag, color);
		if(dists!=-1 && dist==dists){
			return;
		}

		int status = 0;
		//in incremental mode, if the value is already new, no need to update.
		if(incins){
			status = getstatus(fntag,tntag,color);
			if(status == 1)
				return;
		}
		
		//all other case: batch mode, and inc mode with old value.
		int[] pair = new int[3];
		pair[0] = dist;
		pair[1] = 1;
		if(incins)
			pair[2] = 1;
		else
			pair[2] = -1;
		String key = fntag+"_"+tntag;
		HashMap<Integer, int[]> cmap = new HashMap<Integer, int[]>();
		cmap.put(color, pair);
		distMap.put(key, cmap);
		disCard(mode);
	}

	//	============================================================================//
	//	This function is the main fetch algorithm
	//	============================================================================//
	public int getDist(long fntag, long tntag, int color){
		HashMap<Integer, int[]> cmap = distMap.get(fntag+"_"+tntag);
		if(cmap==null)
			return -1;
		int[] pair = cmap.get(color);
		if(pair==null)
			return -1;
		pair[1]++;
		cmap.put(color, pair);
		distMap.put(fntag+"_"+tntag, cmap);
		return pair[0];
	}

	//	============================================================================//
	//	This function is the main fetch algorithm
	//	============================================================================//
	public int getstatus(long fntag, long tntag, int color){
		HashMap<Integer, int[]> cmap = distMap.get(fntag+"_"+tntag);
		if(cmap==null)
			return 0;
		int[] pair = cmap.get(color);
		if(pair==null)
			return 0;
		return pair[2];
	}

	//	============================================================================//
	//	This function is the main replacement algorithm
	//	============================================================================//
	public void disCard(int mode){
		if(distMap.size()<maxSize)
			return;
		else{
			//LRU
			if(mode==1){

			}
			//MRU
			else if(mode==2){

			}
		}
	}
}
