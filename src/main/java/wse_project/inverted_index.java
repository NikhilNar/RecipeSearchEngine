package wse_project;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

class FrequencyDocIdsMapping implements Comparable<FrequencyDocIdsMapping>{
    private Integer frequency;
    private Long docId;

    FrequencyDocIdsMapping(Integer frequency, Long docId){
        this.frequency = frequency;
        this.docId = docId;
    }

    public Integer getFrequency(){
        return frequency;
    }

    public Long getDocId(){
        return docId;
    }

    @Override
    public int compareTo(FrequencyDocIdsMapping frequencyDocIdsMapping) {
        return frequencyDocIdsMapping.getFrequency() - getFrequency();
    }
}

class InvertedIndex {
    private GZIPOutputStream lexiconTier1;
    private GZIPOutputStream lexiconTier2;
    private FileOutputStream invertedIndexTier1;
    private FileOutputStream invertedIndexTier2;
    private GZIPInputStream sortedTermsFile;
    private Double tier1Percentage;

    InvertedIndex(String sortedTermsFilePath, String lexiconTier1FilePath, String lexiconTier2FilePath, String invertedIndexTier1Path, String invertedIndexTier2Path, Double tier1Percentage) {
        this.lexiconTier1 = createGzipFile(lexiconTier1FilePath);
        this.lexiconTier2 = createGzipFile(lexiconTier2FilePath);
        this.invertedIndexTier1 = createFile(invertedIndexTier1Path);
        this.invertedIndexTier2 = createFile(invertedIndexTier2Path);
        this.sortedTermsFile = openTermsFile(sortedTermsFilePath);
        this.tier1Percentage = tier1Percentage;
    }

    private FileOutputStream createFile(String fileName) {
        try {
            return new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            System.out.println("Unable to read file");
        }
        return null;
    }

    private GZIPInputStream openTermsFile(String fileName) {
        try {
            return new GZIPInputStream(new FileInputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    private GZIPOutputStream createGzipFile(String fileName) {
        try {
            return new GZIPOutputStream(new FileOutputStream(fileName));
        } catch (IOException e) {
            System.out.println("Unable to create " + fileName);
        }
        return null;
    }

    public Boolean ifLexiconAndInvertedIndexDocumentCreated() {
        return lexiconTier1 != null && lexiconTier2 != null && invertedIndexTier1 != null && invertedIndexTier2 != null && sortedTermsFile != null;
    }

    public String findVarByte(Long number) {
        String binaryValue = Long.toBinaryString(number);
        StringBuffer sb = new StringBuffer();
        int counter = 7;
        Boolean lastBit = false;
        for (int i = binaryValue.length() - 1; i >= 0; i--) {
            Character bit = binaryValue.charAt(i);
            if (counter == 0) {
                String lastBitValue = lastBit ? "0" : "1";
                lastBit = true;
                sb.append(lastBitValue + bit);
                counter = 6;
            } else {
                sb.append(bit);
                counter--;
            }
        }
        while (counter > 0) {
            sb.append("0");
            counter--;
        }
        if (number <= 127) {
            sb.append("1");
        } else {
            sb.append("0");
        }
        return sb.reverse().toString();
    }

    public void createIndex() {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(sortedTermsFile));
            String previousTerm = null, currentTerm = null;
            ArrayList<FrequencyDocIdsMapping> frequencyDocIdsMappingList = new ArrayList();
            Integer totalBytesTier1 = 0, totalBytesTier2 = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] posting = currentTerm.split(" ");
                if (posting[0].equals(previousTerm) || previousTerm == null) {
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1])));
                } else {
                    Collections.sort(frequencyDocIdsMappingList);
                    Integer tier1Length = Double.valueOf(Math.ceil(frequencyDocIdsMappingList.size()*tier1Percentage/100.0)).intValue();
                    StringBuffer totalBytesDocIdsFreqs = new StringBuffer();
                    for(int i=0; i< tier1Length;i++){
                        totalBytesDocIdsFreqs.append(findVarByte((Long.valueOf(frequencyDocIdsMappingList.get(i).getFrequency()))));
                        totalBytesDocIdsFreqs.append(findVarByte(frequencyDocIdsMappingList.get(i).getDocId()));
                    }
                    byte[] bytesForTerm = totalBytesDocIdsFreqs.toString().getBytes();
                    Integer totalBytesForTerm = bytesForTerm.length;
                    invertedIndexTier1.write(bytesForTerm);
                    lexiconTier1.write((previousTerm + " " + (totalBytesTier1 + 1) + " " + totalBytesForTerm + " "
                            + tier1Length + " \n").getBytes());
                    totalBytesTier1 += totalBytesForTerm;
                    totalBytesDocIdsFreqs = new StringBuffer();
                    for(int i=tier1Length; i< frequencyDocIdsMappingList.size();i++){
                        totalBytesDocIdsFreqs.append(findVarByte((Long.valueOf(frequencyDocIdsMappingList.get(i).getFrequency()))));
                        totalBytesDocIdsFreqs.append(findVarByte(frequencyDocIdsMappingList.get(i).getDocId()));
                    }
                    bytesForTerm = totalBytesDocIdsFreqs.toString().getBytes();
                    totalBytesForTerm = bytesForTerm.length;
                    invertedIndexTier2.write(bytesForTerm);
                    lexiconTier2.write((previousTerm + " " + (totalBytesTier2 + 1) + " " + totalBytesForTerm + " "
                            + (frequencyDocIdsMappingList.size() - tier1Length) + " \n").getBytes());
                    totalBytesTier2 += totalBytesForTerm;
                    frequencyDocIdsMappingList.clear();
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1])));
                }
                previousTerm = posting[0];
            }
            sortedTermsFile.close();
            invertedIndexTier1.close();
            invertedIndexTier2.close();
            lexiconTier1.finish();
            lexiconTier1.close();
            lexiconTier2.finish();
            lexiconTier2.close();
        } catch (IOException e) {
            System.out.println("Error while reading the input" + e);
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        InvertedIndex index = new InvertedIndex("./sorted.gz", "./lexiconTier1.gz", "./lexiconTier2.gz", "./invertedIndexTier1", "./invertedIndexTier2", 30.0);
        if (index.ifLexiconAndInvertedIndexDocumentCreated())
            index.createIndex();
        System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 60000.0);
    }

}