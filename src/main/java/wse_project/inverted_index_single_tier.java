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
//
//class FrequencyDocIdsMapping implements Comparable<FrequencyDocIdsMapping>{
//    private Integer frequency;
//    private Long docId;
//
//    FrequencyDocIdsMapping(Integer frequency, Long docId){
//        this.frequency = frequency;
//        this.docId = docId;
//    }
//
//    public Integer getFrequency(){
//        return frequency;
//    }
//
//    public Long getDocId(){
//        return docId;
//    }
//
//    @Override
//    public int compareTo(FrequencyDocIdsMapping frequencyDocIdsMapping) {
//        return frequencyDocIdsMapping.getFrequency() - getFrequency();
//    }
//}

class InvertedIndexSingleTier {
    private GZIPOutputStream lexiconSingleTier;
    private FileOutputStream invertedIndexSingleTier;
    private GZIPInputStream sortedTermsFile;

    InvertedIndexSingleTier(String sortedTermsFilePath, String lexiconSingleTierFilePath, String invertedIndexSingleTierPath) {
        this.lexiconSingleTier = createGzipFile(lexiconSingleTierFilePath);
        this.invertedIndexSingleTier = createFile(invertedIndexSingleTierPath);
        this.sortedTermsFile = openTermsFile(sortedTermsFilePath);
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
        return lexiconSingleTier != null && invertedIndexSingleTier != null && sortedTermsFile != null;
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
            Integer totalBytesSingleTier = 0;
            while ((currentTerm = br.readLine()) != null) {
                String[] posting = currentTerm.split(" ");
                if (posting[0].equals(previousTerm) || previousTerm == null) {
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1])));
                } else {
                    Collections.sort(frequencyDocIdsMappingList);
                    Integer singleTierLength = Double.valueOf(Math.ceil(frequencyDocIdsMappingList.size())).intValue();
                    StringBuffer totalBytesDocIdsFreqs = new StringBuffer();
                    for(int i=0; i< singleTierLength;i++){
                        totalBytesDocIdsFreqs.append(findVarByte((Long.valueOf(frequencyDocIdsMappingList.get(i).getFrequency()))));
                        totalBytesDocIdsFreqs.append(findVarByte(frequencyDocIdsMappingList.get(i).getDocId()));
                    }
                    byte[] bytesForTerm = totalBytesDocIdsFreqs.toString().getBytes();
                    Integer totalBytesForTerm = bytesForTerm.length;
                    invertedIndexSingleTier.write(bytesForTerm);
                    lexiconSingleTier.write((previousTerm + " " + (totalBytesSingleTier + 1) + " " + totalBytesForTerm + " "
                            + singleTierLength + " \n").getBytes());
                    totalBytesSingleTier += totalBytesForTerm;
                    totalBytesDocIdsFreqs = new StringBuffer();
                    frequencyDocIdsMappingList.clear();
                    frequencyDocIdsMappingList.add(new FrequencyDocIdsMapping(Integer.parseInt(posting[2]), Long.parseLong(posting[1])));
                }
                previousTerm = posting[0];
            }
            sortedTermsFile.close();
            invertedIndexSingleTier.close();
            lexiconSingleTier.finish();
            lexiconSingleTier.close();
        } catch (IOException e) {
            System.out.println("Error while reading the input" + e);
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        InvertedIndexSingleTier index = new InvertedIndexSingleTier("data/1_intermediate/postings/sorted.gz", "data/2_index/lexiconSingleTier.gz", "data/2_index/invertedIndexSingleTier");
        if (index.ifLexiconAndInvertedIndexDocumentCreated())
            index.createIndex();
        System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 60000.0);
    }

}