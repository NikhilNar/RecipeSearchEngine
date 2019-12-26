package wse_project;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.zip.GZIPInputStream;

// stores the postings that are generated from the terms in the queries
class Term {
    private Integer offset;
    private Integer size;
    private Integer count;

    Term(Integer offset, Integer size, Integer count) {
        this.offset = offset;
        this.size = size;
        this.count = count;
    }
    Integer getOffset() {
        return offset;
    }
    Integer getSize() {
        return size;
    }
    Integer getCount() { return count; }
}

// Stores all the documentIds and frequencies for the specific word in the query
class termList {
    ArrayList<Integer> docIDs;
    ArrayList<Integer> freqs;
    private Integer index; // current pointer to the list

    termList() {
        docIDs = new ArrayList();
        freqs = new ArrayList();
        index = -1;
    }

    public String[] decodeVarByteCompression(String varByteEncoding) {
        String[] decodings = new String[varByteEncoding.length() / 8];
        int j = 0;
        for (int i = 0; i < decodings.length; i++) {
            decodings[i] = varByteEncoding.substring(j, j + 8);
            j += 8; }

        return decodings;
    }

    public Integer nextGEQ(Integer k) {
        if (index > -1 && k != null) {
            for (int i = index; i < docIDs.size(); i++) {
                if (docIDs.get(i) >= k)
                    return docIDs.get(i);
            }
        }
        return null;
    }

    public void printDocIDs() { for (Integer i : this.docIDs) System.out.println(i); }

    public void printFreqs() { for (Integer i : this.freqs) System.out.println(i); }

    public Integer getDocIDsSize() {
        return docIDs.size();
    }

    public Integer getFreq() {
        return (index > -1) ? freqs.get(index) : null;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    // decodes the varbyte compression and populates the docIDs and frequencies list.
    public void loadList(String fileName, Integer offset, Integer size) {
        try {
            RandomAccessFile invertedIndexFile = new RandomAccessFile(fileName, "r");
            invertedIndexFile.seek(offset);
            byte[] byteArray = new byte[size];
            invertedIndexFile.read(byteArray);
            String varByteEncoding = new String(byteArray);
            String[] bytes = decodeVarByteCompression(varByteEncoding);
            StringBuffer sb = new StringBuffer();
            ArrayList<Integer> varByteDecodingList = new ArrayList();

            for (String byteString : bytes) {
                sb.append(byteString.substring(1));
                if (byteString.charAt(0) == '1') {
                    varByteDecodingList.add(Integer.parseInt(sb.toString(), 2));
                    sb = new StringBuffer();
                }
            }
            int i = 0;
            for (Integer num : varByteDecodingList) {
                if (i % 2 == 0) freqs.add(num);
                else docIDs.add(num);
                ++i;
            }
            if (docIDs.size() > 0) index++;
            invertedIndexFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
    }
}

// output format of the final result that should be displayed to the user.
class SearchResult implements Comparable<SearchResult> {
    private String url;
    private Integer documentId;
    private Double score;
    private String snippet;
    private ArrayList<Integer> wordsFrequenciesList;

    SearchResult(String url, Integer documentId, Double score, String snippet) {
        this.url = url;
        this.documentId = documentId;
        this.score = score;
        this.snippet = snippet;
        wordsFrequenciesList = new ArrayList();
    }

    public String getUrl() {
        return url;
    }

    public Double getScore() {
        return score;
    }

    public String getSnippet() {
        return snippet;
    }

    public Integer getDocumentId() {
        return documentId;
    }

    public ArrayList<Integer> getWordsFrequenciesList() {
        return wordsFrequenciesList;
    }

    public void setSnippet(String snippet) {
        this.snippet = snippet;
    }

    public void setWordsFrequenciesList(ArrayList<Integer> wordsFrequenciesList) {
        this.wordsFrequenciesList = wordsFrequenciesList;
    }

    @Override
    public int compareTo(SearchResult sr) {
        if (this.score > sr.score) return 1;
        else if (this.score < sr.score) return -1;
        else return 0;
    }
}

// used to store the url elements from url_doc_mapping.gz file
class URLMapping {
    private String url;
    private Integer totalTermsCount;

    URLMapping(String url, Integer totalTermsCount) {
        this.url = url;
        this.totalTermsCount = totalTermsCount;

    }

    public String getUrl() {
        return url;
    }

    public Integer getTotalTermsCount() {
        return totalTermsCount;
    }
}

class Query_MultiTier {
    // stores the mapping of terms and the offset of the inverted index
    private HashMap<String, Term> lexiconMapTier1;
    private HashMap<String, Term> lexiconMapTier2;
    // stores docid to url mapping
    private HashMap<Integer, URLMapping> docIDToUrlMap;
    // stores the maximum no of results that should be returned
    private Integer totalResults;
    private String invertedIndexPathTier1;
    private String invertedIndexPathTier2;
    // stores the total terms in all the documents
    private Integer totalDocumentsTerms;

    Query_MultiTier(Integer totalResults, String invertedIndexPathTier1, String invertedIndexPathTier2) {
        lexiconMapTier1 = new HashMap();
        lexiconMapTier2 = new HashMap();
        docIDToUrlMap = new HashMap();
        this.totalResults = totalResults;
        this.invertedIndexPathTier1 = invertedIndexPathTier1;
        this.invertedIndexPathTier2 = invertedIndexPathTier2;
        this.totalDocumentsTerms = 0;
    }

    public HashMap<String, Term> getLexiconMapTier1(){
        return lexiconMapTier1;
    }

    public HashMap<String, Term> getLexiconMapTier2(){
        return lexiconMapTier2;
    }

    public void buildLexicon(String fileName, HashMap<String, Term> tier) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] lexiconValues = currentTerm.split(" ");
                if (lexiconValues.length == 4) {
                    String term = lexiconValues[0];
                    Integer offset = Integer.parseInt(lexiconValues[1]) - 1;
                    Integer size = Integer.parseInt(lexiconValues[2]);
                    Integer count = Integer.parseInt(lexiconValues[3]);
                    tier.put(term, new Term(offset, size, count));
                }
            }
            System.out.println("lexicon map size = " + tier.size());
            lexiconFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to read content from file");
        }
    }

    public void buildDocIDsToUrlMapping(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] docIDsToUrlMappingValues = currentTerm.split(" ");
                if (docIDsToUrlMappingValues.length == 3) {
                    Integer docId = Integer.parseInt(docIDsToUrlMappingValues[0]);
                    String url = docIDsToUrlMappingValues[1];
                    Integer totalTermsCount = Integer.parseInt(docIDsToUrlMappingValues[2]);
                    //String documentFileName = docIdsToUrlMappingValues[3];
                    try {
                        //Integer offset = Integer.parseInt(docIdsToUrlMappingValues[4]) - 1;
                        //Integer size = Integer.parseInt(docIdsToUrlMappingValues[5]);
                        //totalDocumentsTerms += totalTermsCount;
                        //docIdToUrlMap.put(docId, new URLMapping(url, totalTermsCount, documentFileName, offset, size));
                        docIDToUrlMap.put(docId, new URLMapping(url, totalTermsCount));
                    } catch (Exception e) {
                        System.out.println("Exception caught" + e);
                    }

                }
            }
            System.out.println("buildDocIdsToUrl mapping size = " + docIDToUrlMap.size());
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }

    }

    public Double calculateBM25(ArrayList<Integer> ft, ArrayList<Integer> fdt, Integer modd) {
        Double score = 0.0;
        Integer N = docIDToUrlMap.size();
        Double moddavg = (double) totalDocumentsTerms / N;
        double k1 = 1.2, b = 0.75;
        for (int i = 0; i < ft.size(); i++) {
            Double logarithmicTerm = (N - ft.get(i) + 0.5) / (ft.get(i) + 0.5);
            Double K = k1 * ((1 - b) + b * modd / moddavg);
            Double secondTerm = (k1 + 1) * fdt.get(i) / (K + fdt.get(i));
            score += Math.log(logarithmicTerm) * secondTerm;
        }
        return score;
    }

    // used to find the index of the capital letter for snippet generation
    public int lastCapitalIndex(String content) {
        int index = 0;
        for (int i = content.length() - 1; i >= 0; i--) {
            char letter = content.charAt(i);
            if (Character.isUpperCase(letter) && (i + 1 <= content.length()
                    && (Character.isLowerCase(content.charAt(i + 1)) || content.charAt(i + 1) == ' '))) {
                return i;
            }
        }
        return index;
    }

    // refer document for details regarding the implementation
    public String createSnippet(String content, String[] words) {
        String contentLowerCase = content.toLowerCase();
        ArrayList<Integer> indices = new ArrayList();
        LinkedList<Integer> queue = new LinkedList();
        for (String word : words) {
            if (word.length() == 0) {
                continue;
            }
            int index = contentLowerCase.indexOf(word);
            if (index >= 0) {
                indices.add(index);
            }
            while (index >= 0) {
                index = contentLowerCase.indexOf(word, index + word.length());
                if (index >= 0) {
                    indices.add(index);
                }
            }
        }
        Collections.sort(indices);
        int startIndex = indices.get(0), endIndex = startIndex, maxIndices = 1, noOfCharsDiff = 300, diffInQueue = 0;
        queue.add(startIndex);
        for (int i = 1; i < indices.size() - 1; i++) {
            Integer diff = indices.get(i) - indices.get(i - 1);
            while (diffInQueue + diff > noOfCharsDiff && queue.size() > 0) {
                int diffAfterRemoval = 0;
                int removedValue = queue.poll();
                if (queue.size() > 1) {
                    diffAfterRemoval = queue.get(0) - removedValue;
                }
                diffInQueue -= diffAfterRemoval;
            }
            queue.add(indices.get(i));
            if (queue.size() > maxIndices) {
                startIndex = queue.get(0);
                endIndex = queue.get(queue.size() - 1);
                maxIndices = queue.size();
            }
        }
        String start = "";
        String preIndex = content.substring(0, startIndex);
        int preIndexValue = -1;
        if ((preIndexValue = preIndex.lastIndexOf(".")) > 0) {
            start = content.substring(preIndexValue + 1, startIndex);
        } else {
            start = content.substring(lastCapitalIndex(content.substring(0, startIndex)), startIndex);
        }

        String snippetContent = start + content.substring(startIndex);

        return snippetContent.substring(0, Math.min(497, snippetContent.length())) + "...";
    }

    public ArrayList<Integer> getSearchResults_MultiTier(String query, Integer k) {
        ArrayList<ArrayList<Integer>> docIDLists = new ArrayList();
        ArrayList<ArrayList<Integer>> freqLists = new ArrayList();

        SearchNode_MultiTier test = new SearchNode_MultiTier();

        ArrayList<Integer> results;
        results = test.thresholdAlgo(k, query, lexiconMapTier1, lexiconMapTier2, invertedIndexPathTier1, invertedIndexPathTier2);
//        for (int i : results) System.out.println("result: " + i);

        return results;
    }

    public static void main(String[] args) throws IOException { return; }
}

class Query_SingleTier {
    private HashMap<String, Term> lexiconMapSingleTier;
    private HashMap<Integer, URLMapping> docIDToUrlMap;
    private Integer totalResults;
    private String invertedIndexPathSingleTier;
    private Integer totalDocumentsTerms;

    Query_SingleTier(Integer totalResults, String invertedIndexPathSingleTier) {
        lexiconMapSingleTier = new HashMap();
        docIDToUrlMap = new HashMap();
        this.totalResults = totalResults;
        this.invertedIndexPathSingleTier = invertedIndexPathSingleTier;
        this.totalDocumentsTerms = 0;
    }
    public HashMap<String, Term> getLexiconMapSingleTier(){
        return lexiconMapSingleTier;
    }

    public void buildLexicon(String fileName, HashMap<String, Term> lexicon) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] lexiconValues = currentTerm.split(" ");
                if (lexiconValues.length == 4) {
                    String term = lexiconValues[0];
                    Integer offset = Integer.parseInt(lexiconValues[1]) - 1;
                    Integer size = Integer.parseInt(lexiconValues[2]);
                    Integer count = Integer.parseInt(lexiconValues[3]);
                    lexicon.put(term, new Term(offset, size, count));
                }
            }
            System.out.println("lexicon map size = " + lexicon.size());
            lexiconFile.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to read content from file");
        }
    }

    public void buildDocIDsToUrlMapping(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] docIDsToUrlMappingValues = currentTerm.split(" ");
                if (docIDsToUrlMappingValues.length == 3) {
                    Integer docId = Integer.parseInt(docIDsToUrlMappingValues[0]);
                    String url = docIDsToUrlMappingValues[1];
                    Integer totalTermsCount = Integer.parseInt(docIDsToUrlMappingValues[2]);
                    try {
                        docIDToUrlMap.put(docId, new URLMapping(url, totalTermsCount));
                    } catch (Exception e) {
                        System.out.println("Exception caught" + e);
                    }

                }
            }
            System.out.println("buildDocIdsToUrl mapping size = " + docIDToUrlMap.size());
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }

    }

    public Double calculateBM25(ArrayList<Integer> ft, ArrayList<Integer> fdt, Integer modd) {
        Double score = 0.0;
        Integer N = docIDToUrlMap.size();
        Double moddavg = (double) totalDocumentsTerms / N;
        double k1 = 1.2, b = 0.75;
        for (int i = 0; i < ft.size(); i++) {
            Double logarithmicTerm = (N - ft.get(i) + 0.5) / (ft.get(i) + 0.5);
            Double K = k1 * ((1 - b) + b * modd / moddavg);
            Double secondTerm = (k1 + 1) * fdt.get(i) / (K + fdt.get(i));
            score += Math.log(logarithmicTerm) * secondTerm;
        }
        return score;
    }

    // used to find the index of the capital letter for snippet generation
    public int lastCapitalIndex(String content) {
        int index = 0;
        for (int i = content.length() - 1; i >= 0; i--) {
            char letter = content.charAt(i);
            if (Character.isUpperCase(letter) && (i + 1 <= content.length()
                    && (Character.isLowerCase(content.charAt(i + 1)) || content.charAt(i + 1) == ' '))) {
                return i;
            }
        }
        return index;
    }

    // refer document for details regarding the implementation
    public String createSnippet(String content, String[] words) {
        String contentLowerCase = content.toLowerCase();
        ArrayList<Integer> indices = new ArrayList();
        LinkedList<Integer> queue = new LinkedList();
        for (String word : words) {
            if (word.length() == 0) {
                continue;
            }
            int index = contentLowerCase.indexOf(word);
            if (index >= 0) {
                indices.add(index);
            }
            while (index >= 0) {
                index = contentLowerCase.indexOf(word, index + word.length());
                if (index >= 0) {
                    indices.add(index);
                }
            }
        }
        Collections.sort(indices);
        int startIndex = indices.get(0), endIndex = startIndex, maxIndices = 1, noOfCharsDiff = 300, diffInQueue = 0;
        queue.add(startIndex);
        for (int i = 1; i < indices.size() - 1; i++) {
            Integer diff = indices.get(i) - indices.get(i - 1);
            while (diffInQueue + diff > noOfCharsDiff && queue.size() > 0) {
                int diffAfterRemoval = 0;
                int removedValue = queue.poll();
                if (queue.size() > 1) {
                    diffAfterRemoval = queue.get(0) - removedValue;
                }
                diffInQueue -= diffAfterRemoval;
            }
            queue.add(indices.get(i));
            if (queue.size() > maxIndices) {
                startIndex = queue.get(0);
                endIndex = queue.get(queue.size() - 1);
                maxIndices = queue.size();
            }
        }
        String start = "";
        String preIndex = content.substring(0, startIndex);
        int preIndexValue = -1;
        if ((preIndexValue = preIndex.lastIndexOf(".")) > 0) {
            start = content.substring(preIndexValue + 1, startIndex);
        } else {
            start = content.substring(lastCapitalIndex(content.substring(0, startIndex)), startIndex);
        }

        String snippetContent = start + content.substring(startIndex);

        return snippetContent.substring(0, Math.min(497, snippetContent.length())) + "...";
    }

    public ArrayList<Integer> getSearchResults_SingleTier(String query, Integer k) {
        ArrayList<ArrayList<Integer>> docIDLists = new ArrayList();
        ArrayList<ArrayList<Integer>> freqLists = new ArrayList();

        SearchNode_SingleTier searcher = new SearchNode_SingleTier();

        ArrayList<Integer> results;
        results = searcher.thresholdAlgo(k, query, lexiconMapSingleTier, invertedIndexPathSingleTier);
//        for (int i : results) System.out.println("result: " + i);

        return results;
    }

    public static void main(String[] args) throws IOException { return; }
}


class MainQueryProgram {
    MainQueryProgram() { }
    public float average_precision(ArrayList<Integer> singleTierResults, ArrayList<Integer> multiTierResults) {
        float ap = 0.0f;
        int divisor = 0, num_true_pos = 0;
        HashSet<Integer> set = new HashSet<Integer>();
        for (Integer i : singleTierResults) { set.add(i); }

        for (int i = 0; i < multiTierResults.size(); ++i) {
            if (set.contains(multiTierResults.get(i))) {
                ++num_true_pos;
                ++divisor;
                ap += (float)num_true_pos/(i + 1);}}
        ap /= divisor;
        return ap;
    }

    public static void main(String[] args) throws IOException {
        MainQueryProgram query = new MainQueryProgram();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Enter the max no of results that should be returned : ");
        String records = br.readLine();

        Query_MultiTier query_multi = new Query_MultiTier(Integer.parseInt(records), "data/2_index/invertedIndexTier1", "data/2_index/invertedIndexTier2");
        query_multi.buildLexicon("data/2_index/lexiconTier1.gz", query_multi.getLexiconMapTier1());
        query_multi.buildLexicon("data/2_index/lexiconTier2.gz", query_multi.getLexiconMapTier2());
        query_multi.buildDocIDsToUrlMapping("data/2_index/url_doc_mapping.gz");

        Query_SingleTier query_single = new Query_SingleTier(Integer.parseInt(records), "data/2_index/invertedIndexSingleTier");
        query_single.buildLexicon("data/2_index/lexiconSingleTier.gz", query_single.getLexiconMapSingleTier());
        query_single.buildDocIDsToUrlMapping("data/2_index/url_doc_mapping.gz");

        while (true) {
            System.out.print("Enter query or enter exit to quit : ");
            String input = br.readLine();
            if (input.equals("exit")) { break; }

            long startTime = System.currentTimeMillis();
            ArrayList<Integer> results_multi = query_multi.getSearchResults_MultiTier(input, Integer.parseInt(records));
            System.out.println("Mutli-Tier Query Time =" + (System.currentTimeMillis() - startTime) / 1000.0 + " s");

            startTime = System.currentTimeMillis();
            ArrayList<Integer> results_single = query_single.getSearchResults_SingleTier(input, Integer.parseInt(records));
            System.out.println("Single-Tier Query Time =" + (System.currentTimeMillis() - startTime) / 1000.0 + " s");

            System.out.println("Average Precision (AP): " + query.average_precision(results_single, results_multi) + "\n\n");

//            System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 1000.0 + " s");
//            System.out.println("\n\n\n\n\n");
        }
        return;
    }
}