package statistics;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 词频统计
 * 
 * @author weizhongkai
 *
 */
public class WordFreStatis {

	//存放单词的map
	private Map<String, Integer> map;

	//目录下的所有文件
	private File[] listFiles;

	private long time;

	//线程池
	static ExecutorService executor;
	
	CountDownLatch count; 
	
	//记录读取同一个文件的线程数
	private int threadNumInOneFile = 5;
	
	
	private char endCh = ' ';
	
	

	public WordFreStatis() {

	}

	public WordFreStatis(String directory) {
		try {
			verifyDirectory(directory);
			
			map = new ConcurrentHashMap<String, Integer>(listFiles.length);
		} catch (FileNotFoundException e) {
		}
	}

	/**
	 * 验证目录路径
	 * 
	 * @param directory
	 * @throws FileNotFoundException
	 */
	public void verifyDirectory(String directory) throws FileNotFoundException {
		File file = new File(directory);

		// 获取目录下所有的文件
		listFiles = file.listFiles();

		if (listFiles == null || listFiles.length == 0) {
			System.out.println("目标目录不存在或者目录为空");
			throw new FileNotFoundException();
		}
	}

	/**
	 * 排序并打印
	 * 
	 * @param map
	 */
	public void sortAndPrint(Long time) {
		Set<Entry<String, Integer>> entrySet = map.entrySet();

		List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(entrySet);

		Collections.sort(list, new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		Iterator<Entry<String, Integer>> iterator = list.iterator();

		int i = 0;
		while (iterator.hasNext() && i++ < 10) {
			Entry<String, Integer> next = iterator.next();
			System.out.println(next.getKey() + " : " + next.getValue());
		}

		System.out.println(System.currentTimeMillis() - time);

	}

	/**
	 * 单线程实现读文件
	 * 
	 * @param fileList
	 */
	public void singleThread() {
		if (listFiles != null && listFiles.length > 0) {
			time = System.currentTimeMillis();
			for (File file : listFiles) {
				formatFile(file.getPath());
			}
			sortAndPrint(time);
		}

	}

	/**
	 * 多线程实现读文件
	 * 
	 * @param fileList
	 */
	public void mutilThread() {

		if (listFiles != null && listFiles.length > 0) {
			time = System.currentTimeMillis();
			for (File file : listFiles) {
				Thread thread = new Thread(new Runnable() {

					@Override
					public void run() {

						formatFile(file.getPath());
					}
				});

				thread.start();
				try {
					thread.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			sortAndPrint(time);
		}
	}

	/**
	 * 线程池实现读文件
	 * 
	 * @param fileList
	 */
	public void threadPool() {

		if (listFiles != null && listFiles.length > 0) {
			executor = new ThreadPoolExecutor(5, 10, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue());
			time = System.currentTimeMillis();
			List<Future> futureList = new ArrayList<Future>();
			for (File file : listFiles) {
				Future future = executor.submit(new Runnable() {

					@Override
					public void run() {

						formatFile(file.getPath());

					}
				});

				futureList.add(future);
			}

			for (Future f : futureList) {
				try {
					f.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}

			sortAndPrint(time);
			executor.shutdown();
		}
	}
	
	/**
	 * 
	 * 线程池实现读多个文件（多个线程读同个文件版本）
	 */
	public void threadPoolInFile() {
		if (listFiles != null && listFiles.length > 0) {
			executor = new ThreadPoolExecutor(5, 10, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue());
			count= new CountDownLatch(threadNumInOneFile*listFiles.length);
			time = System.currentTimeMillis();
			
			for (File f : listFiles) {
				read(threadNumInOneFile, f.getPath());
			}
			try {
				count.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			sortAndPrint(time);
		}
		
	}
	
	/**
	 * 计算每个线程在文件中的开始和结束位置，并开启线程
	 * @param threadCount
	 * @param filePath
	 */
	public void read(int threadCount, String filePath){
		
		try {
		RandomAccessFile accessFile = new RandomAccessFile(filePath, "r");

		// 文件长度
		long fileLength = accessFile.length();

		// 间隔
		long gap = fileLength / threadCount;

		// 开始结束位置记录
		long start[] = new long[threadCount];
		long end[] = new long[threadCount];
		long checkIndex = 0;

		// 计算开始结束位置
		for (int i = 0; i < threadCount; i++) {

			start[i] = checkIndex;

			if (i + 1 == threadCount) {
				end[i] = fileLength;
				break;
			}

			checkIndex += gap;

			long stepGap = getGapVal(checkIndex, accessFile, endCh);
			checkIndex += stepGap;
			end[i] = checkIndex;
		}

		runTask(threadCount, start, end, filePath, accessFile);
		}catch (Exception e) {
		}
	}
	
	/**
	 * 防止将文件分给不同的线程后，把一个单词拆分开
	 * @param checkIndex
	 * @param accessFile
	 * @param c   当前为" "
	 * @return
	 * @throws Exception
	 */
	private long getGapVal(long checkIndex, RandomAccessFile accessFile, char c) throws Exception {
		accessFile.seek(checkIndex);
		long count = 0;
		char ch;
		while ((ch = (char) accessFile.read()) != c) {
			count++;
		}
		count++;
		return count;
	}
	
	/**
	 * 开启线程
	 * @param threadCount
	 * @param start
	 * @param endIndex
	 * @param filePath
	 * @param accessFile
	 * @throws Exception
	 */
	public void runTask(int threadCount, long[] start, long[] endIndex, String filePath,
			RandomAccessFile accessFile) throws Exception {

		
		// 开启的一个 Runnable 线程.
		executor.execute(() -> {
			try {
				readData(start[0], endIndex[0], filePath, accessFile);
				count.countDown();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		for (int i = 1; i < threadCount; i++) {
			long begin = start[i];
			long end = endIndex[i];
			executor.execute(() -> {
				try {
					readData(begin, end, filePath, null);
					count.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}
//		count.await();
//		executorService.shutdown();
//		sortAndPrint(time);
	}
	
	/**
	 * 线程读取其所对应要读的文件范围
	 * @param start
	 * @param end
	 * @param filePath
	 * @param accessFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void readData(long start, long end, String filePath, RandomAccessFile accessFile)
			throws FileNotFoundException, IOException {
		if (null == accessFile) {
			accessFile = new RandomAccessFile(filePath, "r");
		}
		// seek 用于设置文件指针位置，设置后ras会从当前指针的下一位读取到或写入到
		accessFile.seek(start);
		// 可以使用:StringBuilder(非线程安全)
		StringBuffer buffer = new StringBuffer("");
		while (true) {
			int read = accessFile.read();
			start++;

			buffer.append((char) read);

			if (start >= end) {
				break;
			}
		}
		accessFile.close();

		String line = buffer.toString();
		String[] strings = line.split("[\\t \\n]+");
		formatAndPutToMap(strings);
	}
	


	/**
	 * 将文件里的内容格式化为一个个单词，并放进map中
	 * 
	 * @param filePath
	 */
	public void formatFile(String filePath) {
		FileReader fr = null;
		try {
			fr = new FileReader(filePath);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		BufferedReader bis = new BufferedReader(fr);

		String line;

		try {
			while ((line = bis.readLine()) != null) {

				String[] strings = line.split(" ");
				formatAndPutToMap(strings);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 格式化字符串并放进map中
	 * @param strings
	 */
	public void formatAndPutToMap(String[] strings) {
		for (String str : strings) {

			String newStr = formatString(str);
			if (newStr.equals(""))
				continue;

			//重量级锁
//			synchronized (map) {
//				if (map.containsKey(newStr)) {
//					map.put(newStr, map.get(newStr) + 1);
//				} else {
//					map.put(newStr, 1);
//				}
//			}
			
			//轻量级锁
			while (true) {
				Integer putIfAbsent = map.putIfAbsent(newStr, 1);
				if (putIfAbsent == null) {
					break;
				} else {
					if (map.replace(newStr, putIfAbsent, putIfAbsent + 1))
						break;
				}
			}
		}
	}

	/**
	 * 格式化字符串
	 * 
	 * @param str
	 * @return
	 */
	public String formatString(String str) {

		StringBuilder builder = new StringBuilder();

		char[] charList = str.trim().toLowerCase().toCharArray();

		for (char ch : charList) {
			//判读是否是字母以及'符号(如：your's看成一个单词)
			if (ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == 39) {
				builder.append(ch);
			}
		}

		return builder.toString();
	}
	
//	public void formatFile(File src,long start,long end) {
//	FileChannel fc;
//	FileLock fl = null;
//	MappedByteBuffer mp;
//	try {
//		fc = new RandomAccessFile(src, "rw").getChannel();
//		fl = fc.lock(start, end, true);
//		mp = fc.map(FileChannel.MapMode.READ_ONLY, start, end);
//		String line = Charset.forName("UTF-8").decode(mp).toString();
//		String[] strings = line.split(" ");
//		for (String str : strings) {
//
//			String newStr = formatString(str);
//			if (newStr.equals(""))
//				continue;
//
////			synchronized (map) {
////				if (map.containsKey(newStr)) {
////					map.put(newStr, map.get(newStr) + 1);
////				} else {
////					map.put(newStr, 1);
////				}
////			}
//			while (true) {
//				Integer putIfAbsent = map.putIfAbsent(newStr, 1);
//				if (putIfAbsent == null) {
//					break;
//				} else {
//					if (map.replace(newStr, putIfAbsent, putIfAbsent + 1))
//						break;
//				}
//			}
//			
//		}
//		
//		
//	} catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}finally{
//		try {
//			if(fl!=null)
//				fl.release();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//}

}
