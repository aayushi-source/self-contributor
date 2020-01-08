package main;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class SpitFiles implements Runnable{

	public static void main(String args[]) throws IOException
	{
	 try{
	  // Reading file and getting no. of files to be generated

	  URL url = new URL("http://www.gutenberg.org/files/2600/2600-0.txt");
	  Scanner scanner = new Scanner(url.openStream());
	
	  double nol = 2000.0; //  No. of lines to be split and saved in each output file.
	  int count = 0;
	  while (scanner.hasNextLine()) 
	  {
	   scanner.nextLine();
	   count++;
	  }
	  System.out.println("Lines in the file: " + count);     // Displays no. of lines in the input file.
	 
	  double temp = (count/nol);
	  int temp1=(int)temp;
	  int nof=0;
	  if(temp1==temp)
	  {
	   nof=temp1;
	  }
	  else
	  {
	   nof=temp1+1;
	  }
	  System.out.println("No. of files to be generated :"+nof); // Displays no. of files to be generated.
	  
	  //---------------------------------------------------------------------------------------------------------
	 
	  // Actual splitting of file into smaller files
	 
	  InputStream fstream = new URL("http://www.gutenberg.org/files/2600/2600-0.txt").openStream(); 
	  DataInputStream in = new DataInputStream(fstream);
	 
	  BufferedReader br = new BufferedReader(new InputStreamReader(in)); String strLine;
	  
	  for (int j=1;j<=nof;j++)
	  {
	   FileWriter fstream1 = new FileWriter("/home/user/Desktop/Aayushi/RackspaceAssignment/SplittedFiles"+"_"+j+".txt");     // Destination File Location
	   BufferedWriter out = new BufferedWriter(fstream1); 
	   for (int i=1;i<=nol;i++)
	   {
	    strLine = br.readLine(); 
	    if (strLine!= null)
	    {
	     out.write(strLine); 
	     if(i!=nol)
	     {
	      out.newLine();
	     }
	    }
	   }
	   out.close();
	  }
	 
	  in.close();
	 }catch (Exception e)
	 {
	  System.err.println("Error: " + e.getMessage());
	 }
	 
	 System.out.println("====================COUNTING WORDS IN EACH FILE AND NUMBER OF TIMES THEY APPEAR=======================");
	 
     // Number of parallel thread to run
     int THREAD_COUNT = 10;
     String s;
     String[] words=null;    //Intialize the word Array
     int wc=0;     //Intialize word count to zero

     File folder = new File("/home/user/Desktop/Aayushi/RackspaceAssignment");
     List<File> paths = new ArrayList<>();
     File[] listOfFiles = folder.listFiles();
     paths =  Arrays.asList(listOfFiles);
     // Shared word counter
     Map<String, AtomicLong> sharedCounter = new ConcurrentHashMap<>();

     ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

     for (File path : paths) {
         executor.execute(new SpitFiles(new Scanner(path), sharedCounter));
         
         FileReader fr = new FileReader(path.getAbsoluteFile());    //Creation of File Reader object
         BufferedReader br = new BufferedReader(fr);    //Creation of BufferedReader object
         while((s=br.readLine())!=null)    //Reading Content from the file
         {
            words=s.split(" ");   //Split the word using space
            wc=wc+words.length;   //increase the word count for each word
         }
         fr.close();
         
         System.out.println("Word Count for each word for file : "+ path.getName()+" is : "+ sharedCounter);
         System.out.println("Total words in the file: "+path.getName() + wc); 
     }
     executor.shutdown();
     // Wait until all threads are finish
     while (!executor.isTerminated()) {
     }
   
	} 
	
	// COUNTING WORDS IN EACH FILE AND NO. OF TIMES THEY APPEAR
	 private final Scanner scanner;
	    private Map<String, AtomicLong> sharedCounter;

	    public SpitFiles(Scanner scanner, Map<String, AtomicLong> sharedCounter) {
	        this.scanner = scanner;
	        this.sharedCounter = sharedCounter;
	    }

	    public void run() {
	        if (scanner == null) {
	            return;
	        }

	        while (scanner.hasNext()) {
	            String word = scanner.next().toLowerCase();
	            sharedCounter.putIfAbsent(word, new AtomicLong(0));
	            sharedCounter.get(word).incrementAndGet();
	        }
	    }
} 
