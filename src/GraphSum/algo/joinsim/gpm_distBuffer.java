package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.MyFileOperation;

public class gpm_distBuffer {

	//structure of dbuffer: 
	//string edge --> hashmap: color, array: distance, frequency, incmark
	public String bname;
	public int maxSize = 0;
	public HashMap<String, HashMap<Integer, int[]>> distMap = null;
	//	public HashSet<int[]> minrankSet = null;
	//	public HashSet<int[]> maxrankSet = null;
	public int minrank;
	public int maxrank;

	public gpm_distBuffer(String gn){
		bname = gn.substring(0, gn.lastIndexOf("."))+"_dM.txt";
		distMap = new HashMap<String, HashMap<Integer, int[]>>();
	}

	public gpm_distBuffer(int msize, String bn){
		bname = bn;
		maxSize = msize;
		distMap = new HashMap<String, HashMap<Integer, int[]>>();
		//		minrank = 1;
		//		maxrank = -1;
	}

	//	============================================================================//
	//	This function is the main cost model for replacement algorithm
	//	============================================================================//
	public int compRand(){
		return 0;
	}

	//	============================================================================//
	//	This function is the main insertion algorithm
	//	============================================================================//
	public void insertDist(int mode, String fntag, String tntag, short dist, int color, boolean incins){
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
	public int getDist(String fntag, String tntag, int color){
		HashMap<Integer, int[]> cmap = distMap.get(fntag+"_"+tntag);
		if(cmap==null)
			return -1;
		int[] pair = cmap.get(color);
		if(pair==null)
			return -1;
		pair[1]++;
		cmap.put(color, pair);
		distMap.put(fntag+"_"+tntag, cmap);
		//		if(maxrank<pair[1]){
		//			maxrank = pair[1];
		//			maxrankSet.clear();
		//			maxrankSet.add(pair);
		//		}
		//		if(maxrank==pair[1]){
		//			maxrankSet.add(pair);
		//		}
		//		if(pair[1]>minrank){
		//			minrankSet.remove(pair);
		//		}
		return pair[0];
	}

	//	============================================================================//
	//	This function is the main fetch algorithm
	//	============================================================================//
	public int getstatus(String fntag, String tntag, int color){
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

	//	============================================================================//
	//	This function stores the distbuffer into file
	//	============================================================================//
	public void storeBuff() throws IOException{
		FileWriter fw = new FileWriter(bname);
		PrintWriter pw = new PrintWriter(fw);

		for(String s: distMap.keySet()){
			pw.println(s);
			HashMap<Integer, int[]> dmap =  distMap.get(s);
			pw.println(dmap.size());
			for(Integer i:dmap.keySet()){
				pw.print(i);
				int[] pair = dmap.get(i);
				for(int j=0;j<pair.length;j++){
					pw.print("	"+pair[j]);
				}
				pw.println();
			}
		}

		pw.close();
		fw.close();

	}

	//	============================================================================//
	//	This function creates a vector from a string
	//	============================================================================//
	public Vector<String> createVectorFromString(String strContent,
			String strDelimiter) {
		Vector<String> vec = new Vector<String>();
		String[] words = strContent.split(strDelimiter);

		for (int i = 0; i < words.length; i++) {
			vec.addElement(words[i]);
		}
		return vec;
	}

	//	============================================================================//
	//	This function restores the distbuffer
	//	============================================================================//
	public void restoreBuff() throws IOException{

		File fn = new File(bname);
		if(!fn.isFile()){
			System.out.println("buffer file not exists. Needs to be recomputed.");
			return;
		}

		BufferedReader in = MyFileOperation.openFile(bname);

		String strLine = ""; 
		Vector<String> vec = null;
		strLine = in.readLine().trim();

		while(strLine!=null){
			String key = strLine;
			strLine = in.readLine().trim();
			int size = Integer.parseInt(strLine);
			HashMap<Integer, int[]> dmap = new HashMap<Integer, int[]>();
			int[] pair = new int[2];
			for(int i=0;i<size;i++){
				strLine = in.readLine().trim();
				vec = createVectorFromString(strLine,"	");
				int color = Integer.parseInt(vec.elementAt(0));
				pair[0] = Integer.parseInt(vec.elementAt(1));
				pair[1] = Integer.parseInt(vec.elementAt(2));
				dmap.put(color, pair);
			}
			distMap.put(key, dmap);	
			strLine = in.readLine().trim();
		}

		in.close();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String s = "2452_4254";
		int fn = Integer.parseInt(s.substring(0, s.indexOf("_")));
		int tn = Integer.parseInt(s.substring(s.indexOf("_")+1, s.length()));
		System.out.println(fn+" "+tn);
	}

}

