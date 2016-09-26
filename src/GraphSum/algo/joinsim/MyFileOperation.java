package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.*;


/**
 * Create on 2007-3-1
 *
 * pachage=org,
 * MyFileOperation
 */
public class MyFileOperation {

	//	 create an input stream
	public static BufferedReader openFile(String strFilename) {
		try {
			// Open the file first
			FileInputStream inputFile = new FileInputStream(strFilename);
			// Convert our input stream to a DataInputStream
			// DataInputStream in = new DataInputStream(inputFile);

			BufferedReader d = new BufferedReader(new InputStreamReader(
					inputFile));
			return d;
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Something wrong
		return null;
	}


//	create an input stream with read ahead limit ability
	public static BufferedReader openFile(String strFilename, int readAheadLimit) {
		try {
			// Open the file first
			FileInputStream inputFile = new FileInputStream(strFilename);
			// Convert our input stream to a DataInputStream
			// DataInputStream in = new DataInputStream(inputFile);

			BufferedReader d = new BufferedReader(new InputStreamReader(
					inputFile));
			// No mark support
			if (!d.markSupported()) {
				System.out
				.println("Can not randomly read a line from this file!");
				return null;
			}
			// Set the read ahead limit
			d.mark(readAheadLimit);
			return d;
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Something wrong
		return null;
	}
//	Randomly read a line from a file with read ahead limit
	@SuppressWarnings("unused")
	private static  String readLine(BufferedReader bufReader, int readAheadLimit,
			int intLineNumber) {
		int i = 1;
		String str = "";

		try {
			if (intLineNumber > readAheadLimit)
				return "Error: the line number should be less than or equal to the read ahead limit!";
			while (i < intLineNumber) {
				bufReader.readLine();
				i++;
			}
			str = bufReader.readLine();
			// if(!bufReader.markSupported()) System.out.println("not support
			// make!");
			bufReader.reset();
			return str;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return str;
	}

	// Randomly read a line from a file
	@SuppressWarnings("unused")
	private  static String readLine(BufferedReader bufReader, int intLineNumber) {

		int i = 1;
		String str = "";
		try {
			while (i < intLineNumber) {
				bufReader.readLine();
				i++;
			}
			str = bufReader.readLine();
			return str;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return str;
	}

	// create an output writer
	public  static PrintWriter writeFile(String strFilename) {
		try {
			PrintWriter pw; // declare a print stream object

			// Create a new file output writer
			pw = new PrintWriter(
					new BufferedWriter(new FileWriter(strFilename)));

			return pw;
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Something wrong
		return null;
	}

	// create an output writer with append style
	public static  PrintWriter writeFileAppend(String strFilename) {

		try {
			PrintWriter pw; // declare a print stream object
			// Create a new file output writer
			pw = new PrintWriter(new BufferedWriter(new FileWriter(strFilename,
					true)));
			return pw;
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Something wrong
		return null;
	}
	
	//coby file
	public void CopyFile(File in, File out) throws IOException{
		FileInputStream fis = new FileInputStream(in);
		FileOutputStream fos = new FileOutputStream(out);
		byte[] buf = new byte[1024];
		int i=0;
		while((i=fis.read(buf))!=-1){
			fos.write(buf,0,i);
		}
		fis.close();
		fos.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
