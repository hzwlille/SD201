package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
		
		BufferedWriter output = new BufferedWriter(new FileWriter(directoryPath + "/part-r-00000"));
		float a= (float)1/n;
		for(int i=0;i<n;i++){

		output.write(String.valueOf(i+1));
		output.write("\t");
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
		
		BufferedReader iniVect = new BufferedReader(new FileReader(initialDirPath + "/part-r-00000"));
		BufferedReader iteVect = new BufferedReader(new FileReader(iterationDirPath + "/part-r-00000"));
		
		double ecart=0;
		String lineG;
		String lineD;
		
		for(int i=0;i<7;i++){//?

			lineG=iniVect.readLine();
			lineD=iteVect.readLine();
			ecart=ecart+Math.abs(Double.parseDouble(lineG.split("\t")[1])-Double.parseDouble(lineD.split("\t")[1]));

			
		}

		iniVect.close();
		iteVect.close();
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
		BufferedReader output = new BufferedReader(new FileReader(vectorDirPath + "/part-r-00000"));
		double use;
		String line;
		ArrayList<Double> newVector=new ArrayList<Double>();
		for(int i=0;i<nNodes;i++){
			line=output.readLine();
			System.out.println();
			System.out.println(line.split("\t")[1]);
			use=Double.parseDouble(line.split("\t")[1]);
			use=use*beta+((1-beta)/nNodes);
			newVector.add(use);
		}
		System.out.println("I arrive here haha3");
		

		output.close();
		BufferedWriter input = new BufferedWriter(new FileWriter(vectorDirPath + "/part-r-00000"));
		for(int i=0;i<nNodes;i++){
			input.write(String.valueOf(i+1));
			input.write("\t");
			input.write((String.format("%.5f", newVector.get(i))));
			input.write("\n");

		}
		input.close();
		

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
		
		
		//Get the matrix and copie to inputMatrixPath
		GraphToMatrix.job(conf);



		while(!getResult){

			System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++");
			//Calculer le nouveau vecteur
			
			MatrixVectorMult.job(conf);

			System.out.println("++++++++++++++++++++");
			avoidSpiderTraps(currentVector,nNodes,beta);
			//Comparer l'ecart
			getResult=checkConvergence(initialVector,currentVector,epsilon);
			System.out.println("The get result is :");
			System.out.print(getResult);
			if(!getResult){
				
				
				BufferedReader curVect = new BufferedReader(new FileReader(currentVector + "/part-r-00000"));
				BufferedWriter iteVect = new BufferedWriter(new FileWriter(initialVector + "/part-r-00000"));
				for(int i=0;i<nNodes;i++){
				String myStr;
				myStr=curVect.readLine();
				System.out.println(myStr);
					iteVect.write(myStr);
					iteVect.write("\n");
					

				}
				iteVect.close();
				curVect.close();
				FileUtils.deleteDirectory(new File(currentVector));
				
				/*FileUtils.deleteDirectory(new File(initialVector));
				FileUtils.copyDirectory(new File(currentVector), new File(initialVector));								//copy current to initial.		
				
				*/
			}
			System.out.println("One turn finished");
			
		}

													//copy current to result
		
		//FileUtils.deleteDirectory(new File(conf.get("inputMatrixPath")));
		//FileUtils.deleteDirectory(new File(conf.get(currentVector)));
		// when you finished implementing delete this line
		FileUtils.copyDirectory(new File(initialVector), new File(conf.get("finalVectorPath")));
		
		
	}
}
