package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/*
 * VERY IMPORTANT 
 * 
 * Each time you need to read/write a file, retrieve the directory path with conf.get 
 * The paths will change during the release tests, so be very carefully, never write the actual path "data/..." 
 * CORRECT:
 * String initialVector = conf.get("initialRankVectorPath");
 * BufferedWriter output = new BufferedWriter(new FileWriter(initialVector + "/vector.txt"));
 * 
 * WRONG
 * BufferedWriter output = new BufferedWriter(new FileWriter(data/initialVector/vector.txt"));
 */

public class PageRank {
	
	public static void createInitialRankVector(String directoryPath, long n) throws IOException 
	{

		File dir = new File(directoryPath);
		if(! dir.exists())
			FileUtils.forceMkdir(dir);
		
		BufferedWriter output = new BufferedWriter(new FileWriter(directoryPath + "/vector.txt"));
		float a= (float)1/n;
		for(int i=0;i<n;i++){
		output.write(String.format("%.5f", a));
		output.write("\n");
		}
		System.out.print(n);
		output.close();
		//TO DO			
	}
	
	public static boolean checkConvergence(String initialDirPath, String iterationDirPath, double epsilon) throws NumberFormatException, IOException
	{
		//TO DO
		// you need to use the L1 norm 
		
		//Récupérer les vecteurs
		BufferedReader iniVect = new BufferedReader(new FileReader(initialDirPath + "/vector.txt"));
		BufferedReader iteVect = new BufferedReader(new FileReader(iterationDirPath + "/vector.txt"));
		double ecart=0;
		
		while(iniVect.hashCode()==1){//?
			
			ecart=ecart+Math.abs(Double.parseDouble(iniVect.readLine())-Double.parseDouble(iteVect.readLine()));
			
		}
		
		if(ecart<epsilon){
			return true;
		}
		else{
			return false;
		}
		
	}
	
	public static void avoidSpiderTraps(String vectorDirPath, long nNodes, double beta) throws IOException 
	{
		//TO DO
		BufferedWriter output = new BufferedWriter(new FileWriter(vectorDirPath + "/vector.txt"));
		double[nNodes] vectorX;
		for(int i=0;i<n;i++){
			output.write(String.format("%.5f", a));
			output.write("\n");
		}
		
		
		
	}
	
	public static void iterativePageRank(Configuration conf) 
			throws IOException, InterruptedException, ClassNotFoundException
	{
		
		
		String initialVector = conf.get("initialVectorPath");
		String currentVector = conf.get("currentVectorPath");
		
		String finalVector = conf.get("finalVectorPath"); 
		/*here the testing system will search for the final rank vector*/
		
		Double epsilon = conf.getDouble("epsilon", 0.1);
		Double beta = conf.getDouble("beta", 0.8);

		boolean getResult=false;
		//TO DO

		// to retrieve the number of nodes use long nNodes = conf.getLong("numNodes", 0); 
		Long nNodes = conf.getLong("numNodes", 0); 
		
		
		//Creat the initial RankVector 
		createInitialRankVector(initialVector,nNodes);
		File dir = new File(currentVector);
		
		
		//Get the matrix
		GraphToMatrix.job(conf);
		
		
		while(!getResult){
		//Calculer le nouveau vecteur
		avoidSpiderTraps(initialVector,nNodes,beta);
		
		//Comparer l'ecart
		getResult=checkConvergence(initialVector,currentVector,epsilon);
		if(!getResult){
												//copy current to initial.
		}
			
		}
		
													//copy current to result
		
		
		// when you finished implementing delete this line
		throw new UnsupportedOperationException("Implementation missing");
		
	}
}
