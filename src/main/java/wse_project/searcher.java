package web_indexing;

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

class Searcher {
    ArrayList<Integer> docs_per_term;
    ArrayList<ArrayList<Integer>> termLists1;
    ArrayList<ArrayList<Integer>> termLists2;
    ArrayList<Integer> list1;
    ArrayList<Integer> list2;
    ArrayList<Integer> list3;
    ArrayList<Integer> list4;
    ArrayList<Integer> list5;
    ArrayList<Integer> list6;

    Searcher() {
        termLists1 = new ArrayList<ArrayList<Integer>>();
        termLists2 = new ArrayList<ArrayList<Integer>>();
        list1 = new ArrayList<Integer>(Arrays.asList(3, 6, 7, 5, 2, 3, 1, 2));
        list2 = new ArrayList<Integer>(Arrays.asList(4, 5, 3, 3, 5, 1));
        list3 = new ArrayList<Integer>(Arrays.asList(1, 7, 5, 4, 3, 2));
        list4 = new ArrayList<Integer>(Arrays.asList(4, 2, 6, 2, 5, 1));
        list5 = new ArrayList<Integer>(Arrays.asList(7, 1, 6, 1));
        list6 = new ArrayList<Integer>(Arrays.asList(4, 2, 2, 2, 7, 1));
        docs_per_term = new ArrayList<Integer>();
    }

    public void loadList(int tier_num, ArrayList<Integer> termList) {
        for (int i = 0; i < list1.size(); ++i) {
            System.out.println(list1.get(i));
        }
//        this.termLists1.add(this.list1);
//        this.termLists1.add(this.list2);
//        this.termLists1.add(this.list3);
//        this.docs_per_term.add(termList.size());
    }

    public void loadTier1(ArrayList<String> query) {
        this.termLists1.add(this.list1);
        this.termLists1.add(this.list2);
        this.termLists1.add(this.list3);
        this.docs_per_term.add(this.list1.size());
        this.docs_per_term.add(this.list2.size());
        this.docs_per_term.add(this.list3.size());
    }

    public void fallThrough(ArrayList<String> query) {
        System.out.println("(FALLING THROUGH TO TIER 2)");
        this.termLists1.get(0).addAll(this.list4);
        this.termLists1.get(1).addAll(this.list5);
        this.termLists1.get(2).addAll(this.list6);
        this.docs_per_term.set(0, this.docs_per_term.get(0) + this.list4.size());
        this.docs_per_term.set(1, this.docs_per_term.get(1) + this.list5.size());
        this.docs_per_term.set(2, this.docs_per_term.get(2) + this.list6.size());
    }

    public int intersection(Integer num_lists) {
        int num_intersect = 0;
        HashMap<Integer, Integer> counter = new  HashMap<Integer, Integer>();

        for (ArrayList<Integer> termList : termLists1) {
            for (int i = 0; i < termList.size()/2; ++i) {
                if (counter.get(termList.get(2 * i)) == null) counter.put(termList.get(2 * i), 1);
                else counter.replace(termList.get(2 * i), counter.get(termList.get(2 * i)) + 1); } }

        for (Map.Entry each : counter.entrySet()) {
            if (each.getValue() == num_lists) ++num_intersect; }

        return num_intersect;
    }

    public List<Integer> thresholdAlgo(int k, ArrayList<String> query) {
        /*
        :param k:  target num results to be returned
        :param docs_per_term:
        :param termLists:  axis 0 := term-index, axis 1 := i-th element in term's list
        :return results:  list of top-k scoring results
        */
        this.loadTier1(query);
        if (this.intersection(query.size()) < k) this.fallThrough(query);

        int threshold = 0;
        int n_terms = query.size();
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
            for (int j = 0; j < termLists1.get(i).size(); j += 2)
                termMap.put(termLists1.get(i).get(j), termLists1.get(i).get(j + 1));
            termMaps.add(termMap); }

        int i = 0;
        while (threshold >= kth.getKey() && i < min_list_size) {
            threshold = 0;
            for (int j = 0; j < n_terms; ++j) {
                int docID = termLists1.get(j).get(i);
                int freq = termLists1.get(j).get(i + 1);

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
        Searcher test = new Searcher();

        List<Integer> res;
        ArrayList<String> query = new ArrayList<String>(Arrays.asList("test1", "test2", "test3"));
        res = test.thresholdAlgo(2, query);
        for (int i : res) System.out.println("result: " + i);
    }
}