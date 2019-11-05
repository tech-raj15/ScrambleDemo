package com.raj.scramble;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScrambleDemoMain {

    public static String getIntermediateText(String str) {
	return str.substring(1, str.length() - 1);
    }

    public static Set<String> getPermutation(String str, Character start, Character end) {
	Set<String> perm = new HashSet<>();// Using set to have distinct combinations
	// Handling error scenarios
	if (str == null) {
	    return Collections.emptySet();
	} else if (str.length() == 0) {
	    perm.add("");
	    return perm;
	}
	char initial = str.charAt(0);
	String rem = str.substring(1);
	Set<String> words = getPermutation(rem, null, null);
	for (String strNew : words) {
	    for (int i = 0; i <= strNew.length(); i++) {
		perm.add(charInsert(strNew, initial, i, start, end));
	    }
	}
	return perm;
    }

    public static String charInsert(String str, char c, int j, Character first, Character last) {
	String begin = str.substring(0, j);
	String end = str.substring(j);
	if (first != null || last != null)
	    return first + begin + c + end + last;
	return begin + c + end;
    }

    public static List<String> findInInputString(List<String> inputString, List<String> permutedList) {
	int count = 0;
	List<String> list = new ArrayList<>();
	for (int i = 1; i <= inputString.size(); i++) {
	    for (int j = 0; j < permutedList.size(); j++) {
		if (inputString.get(i - 1).contains(permutedList.get(j)))
		    count++;
	    }
	    list.add("Case #" + i + " : " + count);
	    count = 0;

	}
	return list;
    }

    public static void main(String[] args) {
	// dictionary value can be read from any source

	List<String> dictionaryString = readFile("Resources//dictionaryFile.txt"); // readFile(args[0]);
	List<String> inputString = readFile("Resources//inputFile.txt"); // readFile(args[1]);

	// Generate Permutations
	List<String> permutedList = new ArrayList<>();
	for (String s : dictionaryString) {
	    permutedList.addAll(getPermutation(getIntermediateText(s), s.charAt(0), s.charAt(s.length() - 1)));
	}
	// Check for Permutations in file
	findInInputString(inputString, permutedList).stream().forEach(System.out::println);
    }

    protected static List<String> readFile(String fileName) {
	File file = new File(fileName);
	FileInputStream fis;
	InputStreamReader isr;
	BufferedReader br;
	List<String> list = new ArrayList<>();
	try {
	    fis = new FileInputStream(file);
	    isr = new InputStreamReader(fis);
	    br = new BufferedReader(isr);

	    String line;
	    while ((line = br.readLine()) != null) {
		list.add(line);
		System.out.println(line);
	    }
	    br.close();
	} catch (IOException e) {
	    System.out.println(fileName + e.getMessage());
	}
	return list;
    }
}
