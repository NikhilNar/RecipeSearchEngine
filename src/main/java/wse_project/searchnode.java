package wse_project;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Map;

class HeapKV implements Comparable<HeapKV> {
    private int key;
    private int value;
    HeapKV(int score, int docID) {
        this.key = score;
        this.value = docID; }
    public int getKey() { return this.key; }
    public int getValue() { return this.value; }
    public int compareTo(HeapKV other) { return this.getKey() - other.getKey(); }
}

class SearchNode {
    ArrayList<Integer> docs_per_term;
    ArrayList<ArrayList<Integer>> freqLists;
    ArrayList<ArrayList<Integer>> docIDLists;
//
//    ArrayList<ArrayList<Integer>> termLists1;
//    ArrayList<ArrayList<Integer>> termLists2;
//
//    ArrayList<Integer> list1_freq;
//    ArrayList<Integer> list1_docID;
//    ArrayList<Integer> list2_freq;
//    ArrayList<Integer> list2_docID;
//    ArrayList<Integer> list3_freq;
//    ArrayList<Integer> list3_docID;
//    ArrayList<Integer> list4_freq;
//    ArrayList<Integer> list4_docID;
//    ArrayList<Integer> list5_freq;
//    ArrayList<Integer> list5_docID;
//    ArrayList<Integer> list6_freq;
//    ArrayList<Integer> list6_docID;

    SearchNode() {
        freqLists = new ArrayList<ArrayList<Integer>>();
        docIDLists = new ArrayList<ArrayList<Integer>>();
//        list1_freq = new ArrayList<Integer>(Arrays.asList(6, 5, 3, 2));
//        list1_docID = new ArrayList<Integer>(Arrays.asList(3, 7, 2, 1));
//        list2_freq = new ArrayList<Integer>(Arrays.asList(5, 3, 1));
//        list2_docID = new ArrayList<Integer>(Arrays.asList(4, 3, 5));
//        list3_freq = new ArrayList<Integer>(Arrays.asList(7, 4, 2));
//        list3_docID = new ArrayList<Integer>(Arrays.asList(1, 5, 3));
//        list4_freq = new ArrayList<Integer>(Arrays.asList(2, 2, 1));
//        list4_docID = new ArrayList<Integer>(Arrays.asList(4, 6, 5));
//        list5_freq = new ArrayList<Integer>(Arrays.asList(1, 1));
//        list5_docID = new ArrayList<Integer>(Arrays.asList(7, 6));
//        list6_freq = new ArrayList<Integer>(Arrays.asList(2, 2, 1));
//        list6_docID = new ArrayList<Integer>(Arrays.asList(4, 2, 7));
//        list2 = new ArrayList<Integer>(Arrays.asList(4, 5, 3, 3, 5, 1));
//        list3 = new ArrayList<Integer>(Arrays.asList(1, 7, 5, 4, 3, 2));
//        list4 = new ArrayList<Integer>(Arrays.asList(4, 2, 6, 2, 5, 1));
//        list5 = new ArrayList<Integer>(Arrays.asList(7, 1, 6, 1));
//        list6 = new ArrayList<Integer>(Arrays.asList(4, 2, 2, 2, 7, 1));
        docs_per_term = new ArrayList<Integer>();
    }

//    public void loadTier1(ArrayList<String> query) {
//        this.freqLists.add(this.list1_freq);
//        this.freqLists.add(this.list2_freq);
//        this.freqLists.add(this.list3_freq);
//        this.docIDLists.add(this.list1_docID);
//        this.docIDLists.add(this.list2_docID);
//        this.docIDLists.add(this.list3_docID);
//        this.docs_per_term.add(this.list1_docID.size());
//        this.docs_per_term.add(this.list2_docID.size());
//        this.docs_per_term.add(this.list3_docID.size());
//    }

    public int intersection(Integer num_lists) {
        int num_intersect = 0;
        HashMap<Integer, Integer> counter = new  HashMap<Integer, Integer>();

        for (ArrayList<Integer> docIDList : docIDLists) {
            for (int i = 0; i < docIDList.size(); ++i) {
                if (counter.get(docIDList.get(i)) == null) counter.put(docIDList.get(i), 1);
                else counter.replace(docIDList.get(i), counter.get(docIDList.get(i)) + 1); } }

        for (Map.Entry each : counter.entrySet()) {
            if (each.getValue() == num_lists) ++num_intersect; }

        return num_intersect;
    }

    public void loadTier1(String[] query_terms, HashMap<String, Term> lexiconMapTier1, String invertedIndexPathTier1) {

        for (String t : query_terms) {
            if (t.length() == 0) continue;
            termList list_cur = new termList();
            Term term_cur = lexiconMapTier1.get(t);
            if (term_cur != null) {
                Integer offset = term_cur.getOffset();
                Integer size = term_cur.getSize();
                list_cur.loadList(invertedIndexPathTier1, offset, size);
            }
            docIDLists.add(list_cur.docIDs);
            freqLists.add(list_cur.freqs);
            this.docs_per_term.add(list_cur.docIDs.size());
        }
    }

    public void fallThrough(String[] query_terms, HashMap<String, Term> lexiconMapTier2, String invertedIndexPathTier2) {
        System.out.println("(FALLING THROUGH TO TIER 2)");

        for (int i = 0; i < query_terms.length; ++i) {
            if (query_terms[i].length() == 0) continue;
            termList list_cur = new termList();
            Term term_cur = lexiconMapTier2.get(query_terms[i]);
            if (term_cur != null) {
                Integer offset = term_cur.getOffset();
                Integer size = term_cur.getSize();
                list_cur.loadList(invertedIndexPathTier2, offset, size);
            }
            docIDLists.get(i).addAll(list_cur.docIDs);
            freqLists.get(i).addAll(list_cur.freqs);
            this.docs_per_term.set(i, docs_per_term.get(i) + list_cur.docIDs.size());
        }

//        this.freqLists.get(0).addAll(this.list4_freq);
//        this.freqLists.get(1).addAll(this.list5_freq);
//        this.freqLists.get(2).addAll(this.list6_freq);
//        this.docIDLists.get(0).addAll(this.list4_docID);
//        this.docIDLists.get(1).addAll(this.list5_docID);
//        this.docIDLists.get(2).addAll(this.list6_docID);
//        this.docs_per_term.set(0, this.docs_per_term.get(0) + this.list4_docID.size());
//        this.docs_per_term.set(1, this.docs_per_term.get(1) + this.list5_docID.size());
//        this.docs_per_term.set(2, this.docs_per_term.get(2) + this.list6_docID.size());
    }

    public ArrayList<Integer> thresholdAlgo(int k, String query, HashMap<String, Term> lexiconMapTier1, HashMap<String, Term> lexiconMapTier2, String invertedIndexPathTier1, String invertedIndexPathTier2) {
        /*
        :param k:  target num results to be returned
        :param docs_per_term:
        :param termLists:  axis 0 := term-index, axis 1 := i-th element in term's list
        :return results:  list of top-k scoring results
        */

        String[] query_terms = query.split(" ");

        this.loadTier1(query_terms, lexiconMapTier1, invertedIndexPathTier1);

        if (this.intersection(query_terms.length) < k) this.fallThrough(query_terms, lexiconMapTier2, invertedIndexPathTier2);

        int threshold = 0;
        int n_terms = query_terms.length;
        int min_list_size = Collections.min(docs_per_term);

        ArrayList<Integer> results = new ArrayList<Integer>();
        ArrayList<HashMap<Integer, Integer>> termMaps = new ArrayList<HashMap<Integer, Integer>>();
        HashMap<Integer, Integer> seen_value = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> seen_num = new HashMap<Integer, Integer>();

        PriorityQueue<HeapKV> minHeap = new PriorityQueue<HeapKV>();
        HeapKV kth = new HeapKV(0, -1);

        //Initialize Term HashMaps
        for (int i = 0; i < n_terms; ++i) {
            HashMap<Integer, Integer> termMap = new HashMap<Integer, Integer>();
            for (int j = 0; j < docIDLists.get(i).size(); ++j)
                termMap.put(docIDLists.get(i).get(j), freqLists.get(i).get(j));
            termMaps.add(termMap); }

        int i = 0;
        while (threshold >= kth.getKey() && i < min_list_size) {
            threshold = 0;
            for (int j = 0; j < n_terms; ++j) {
                int docID = docIDLists.get(j).get(i);
                int freq = freqLists.get(j).get(i);

                if (!seen_value.containsKey(docID)) {
                    for (int l = 0; l < n_terms; ++l) {
                        Integer freq_in_list = termMaps.get(l).get(docID);
                        if (freq_in_list != null) {
                            if (seen_value.get(docID) == null) {
                                seen_value.put(docID, freq_in_list);
                                seen_num.put(docID, 1);
                            } else {
                                seen_value.replace(docID, seen_value.get(docID) + freq_in_list);
                                seen_num.replace(docID, seen_num.get(docID) + 1);
                            }
                        }
                    }
//                    if (seen_num.get(docID) == n_terms) --k;

                    int cur = seen_value.get(docID);

                    if (minHeap.size() < k)
                        minHeap.add(new HeapKV(cur, docID));
                    else if (minHeap.size() == k && cur > minHeap.peek().getKey()) {
                        minHeap.add(new HeapKV(cur, docID));
                        minHeap.poll(); }
                }

                threshold += freq;
            }
            kth = minHeap.peek();
            ++i;
        }

//
//        if (threshold >= kth.getKey()) {
//            this.fallThrough();
//        }

        int m = 0;
        while (m < k && minHeap.peek() != null) {
            results.add(minHeap.poll().getValue());
            ++m;
        }
        Collections.reverse(results);
        return results;
    }

    public static void main(String[] args) throws IOException {
//        SearchNode test = new SearchNode();
//
//        List<Integer> res;
//        ArrayList<String> query = new ArrayList<String>(Arrays.asList("test1", "test2", "test3"));
//        res = test.thresholdAlgo(8, query);
//        for (int i : res) System.out.println("result: " + i);
        return;
    }
}