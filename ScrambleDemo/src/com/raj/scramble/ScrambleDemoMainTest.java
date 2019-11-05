package com.raj.scramble;

import static com.raj.scramble.ScrambleDemoMain.findInInputString;
import static com.raj.scramble.ScrambleDemoMain.getIntermediateText;
import static com.raj.scramble.ScrambleDemoMain.getPermutation;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ScrambleDemoMainTest {

    @org.junit.Test
    public void getPermutation_should_succeed() {

	// given
	String inputString = "axpaj";
	Set<String> expectedPermutedSet = new HashSet<>(
		Arrays.asList("aaxpj", "aapxj", "apxaj", "axapj", "axpaj", "apaxj"));

	// when
	Set<String> actualPermutedSet = getPermutation(getIntermediateText(inputString), inputString.charAt(0),
		inputString.charAt(inputString.length() - 1));

	// then
	assertEquals(actualPermutedSet.size(), expectedPermutedSet.size());
	assertEquals(!Collections.disjoint(expectedPermutedSet, actualPermutedSet), true);
    }

    @org.junit.Test
    public void findInInputString_should_succeed() {
	// given
	List<String> inputList = new ArrayList<>(Arrays.asList("aapxjdnrbtvldptfzbbdbbzxtndrvjblnzjfpvhdhhpxjdnrbt"));
	List<String> permutedList = new ArrayList<>(
		Arrays.asList("aaxpj", "aapxj", "apxaj", "axapj", "axpaj", "apaxj"));
	List<String> expectedResult = new ArrayList<>(Arrays.asList("Case #1 : 1"));

	// when
	List<String> actualResult = findInInputString(inputList, permutedList);
	// then
	assertEquals(expectedResult.size(), actualResult.size());
	assertEquals(!Collections.disjoint(expectedResult, actualResult), true);
    }
}