package org.venice.piazza.servicecontroller.util;

import java.util.Random;
/**
 * Class providing utilities to support unit testing.
 * @author mlynum
 *
 */
public class TestUtilities {

	public static int randInt(int min, int max) {
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}
}
