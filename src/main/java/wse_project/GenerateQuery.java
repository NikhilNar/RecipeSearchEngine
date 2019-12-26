package wse_project;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GenerateQuery {
    private String sortedFilePath;
    private GZIPOutputStream queries;
    private String queriesPath;

    GenerateQuery(String sortedFilePath, String queriesPath){
        this.sortedFilePath = sortedFilePath;
        this.queriesPath = queriesPath;
        this.queries = createGzipFile(this.queriesPath);
    }

    private GZIPOutputStream createGzipFile(String fileName) {
        try {
            return new GZIPOutputStream(new FileOutputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    public String getTermBasedOnProbability(ArrayList<Double> termProbabilities, ArrayList<String> termList, Double randProb1){
        int left = 0, right = termProbabilities.size()-1;
        while(left<=right){
            int mid = (left+right)/2;
            Double midProbability = termProbabilities.get(mid);
            if(right-left<=1){
                return termList.get(right);
            } else if(midProbability > randProb1){
                right = mid-1;
            } else if(midProbability < randProb1){
                left = mid+1;
            } else{
                return termList.get(mid);
            }
        }
        return null;
    }

    public void generateQueries(){
        try {
            GZIPInputStream sortedTermsFile = new GZIPInputStream(new FileInputStream(this.sortedFilePath));
            BufferedReader br = new BufferedReader(new InputStreamReader(sortedTermsFile));
            HashMap<String, Integer> termFrequencyMap = new HashMap();
            String currentTerm = null;
            Integer totalFrequencyOfWords = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] termValues = currentTerm.split(" ");
                if (termValues.length == 3) {
                    String term = termValues[0];
                    Integer frequency = Integer.parseInt(termValues[2]);
                    Integer previousTermValue = (termFrequencyMap.containsKey(term))? termFrequencyMap.get(term):0;
                    termFrequencyMap.put(term, previousTermValue+frequency);
                    totalFrequencyOfWords+=frequency;
                }
            }
            ArrayList<Double> termProbabilities = new ArrayList();
            ArrayList<String> termList = new ArrayList();

            Double totalTermFrequenciesCoverage = 0.0;
            for(String key: termFrequencyMap.keySet()){
                Double probabilityOfTerm = termFrequencyMap.get(key)*1.0/totalFrequencyOfWords;
                totalTermFrequenciesCoverage+=probabilityOfTerm;
                termProbabilities.add(totalTermFrequenciesCoverage);
                termList.add(key);
            }

            Random r = new Random();
            for(int i=0; i<1000; i++){
                Double randProb1 = r.nextDouble();
                Double randProb2 = r.nextDouble();
                String term1 = getTermBasedOnProbability(termProbabilities, termList, randProb1);
                String term2 = getTermBasedOnProbability(termProbabilities, termList, randProb2);
                String finalTerm = term1 + " " + term2+"\n";
                queries.write(finalTerm.getBytes());
            }
            sortedTermsFile.close();
            queries.finish();
            queries.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to read content from file");
        }
    }

    public static void main(String[] args){
        GenerateQuery gq = new GenerateQuery("./sorted.gz", "./queries.gz");
        gq.generateQueries();
    }
}
