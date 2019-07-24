package statistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WordFreStatisTest {

	public static void main(String[] args) throws Exception {
		
		
		
		WordFreStatis word = new WordFreStatis("C:\\Users\\weizhongkai\\Desktop\\输入");
//		word.threadPool();
//		word.singleThread();
//		word.mutilThread();
		word.threadPoolInFile();
	}

}
