package web_indexing;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

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

class Searcher {

    Searcher() {}

    public List<Integer> thresholdAlgo(int k, ArrayList<Integer> docs_per_term, ArrayList<ArrayList<Integer>> termLists) {
        /*
        :param k:  target num results to be returned
        :param docs_per_term:
        :param termLists:  axis 0 := term-index, axis 1 := i-th element in term's list
        :return results:  list of top-k scoring results
        */

        int threshold = 0;
        int n_terms = docs_per_term.size();
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
            for (int j = 0; j < termLists.get(i).size(); j += 2)
                termMap.put(termLists.get(i).get(j), termLists.get(i).get(j + 1));
            termMaps.add(termMap); }

        int i = 0;
        while (threshold >= kth.getKey() && i < min_list_size) {
            threshold = 0;
            for (int j = 0; j < n_terms; ++j) {
                int docID = termLists.get(j).get(i);
                int freq = termLists.get(j).get(i + 1);

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
            i += 2;
        }

        int m = 0;
        while (m < k && minHeap.peek() != null) {
            results.add(minHeap.poll().getValue());
            ++i;
        }
        Collections.reverse(results);
        return results;
    }

    public static void main(String[] args) throws IOException {
        // 8 documents

        Searcher test = new Searcher();

        ArrayList<Integer> docs_per_term = new ArrayList<Integer>(Arrays.asList(8, 6, 8));

        ArrayList<ArrayList<Integer>> termLists = new ArrayList<ArrayList<Integer>>();;
        ArrayList<Integer> list1 = new ArrayList<Integer>(Arrays.asList(3, 6, 7, 5, 2, 3, 1, 2));
        ArrayList<Integer> list2 = new ArrayList<Integer>(Arrays.asList(4, 5, 3, 3, 5, 1));
        ArrayList<Integer> list3 = new ArrayList<Integer>(Arrays.asList(1, 7, 5, 4, 3, 2, 6, 2));
        termLists.add(list1);
        termLists.add(list2);
        termLists.add(list3);

        List<Integer> res;
        res = test.thresholdAlgo(5, docs_per_term, termLists);
        for (int i : res) System.out.println("result: " + i);
    }
}