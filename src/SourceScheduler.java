import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileSystemNotFoundException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * 类说明
 *
 * @author:Amy
 * @version:2016年7月30日下午1:54:45
 */
public class SourceScheduler {

	// =====================公用变量：全局变量定义============================
	// --------------------------节点 node 数量-------------
	int[] nodeNum = { 10, 15, 20, 25, 30 };

	// --------------------------slot 数量----------------
	// 得到slot比例M/R：R1=2：2，R2=4：2，R3=6：2，R4=8：2，
	// map:每个节点map slot数目
	int[] mapSlotNumNode = { 2, 4, 6, 8 };
	// reduce：每个节点reduce slot数目
	int reduceSlotNumNode = 2;

	// Mapslot 列表
	static List<MapSlot> listMapSlot;
	// Reduceslot 列表
	static List<ReduceSlot> listReduceSlot;

	static // --------------------------传输速度 Mb/s----------------
	int fd = 100;

	static int fr = 50;

	static int fn = 30;
	
	// --------------------------存储EA、EF的下界值----------------
	public static double[] LB_VALUE_EA;
	public static double[] LB_VALUE_EF;
	// --------------------------作业 数量-------------
	int[] jobNum = { 50, 100, 150, 200, 250 };

//	public static void queue() {
//		// 10个作业在2台机子上执行的时间
//		int[] aMachineTime = { 2, 7, 8, 3, 9, 5, 11, 16, 15, 10 };
//		int[] bMachineTime = { 5, 9, 6, 4, 3, 7, 12, 8, 10, 14 };
//		int[] pMachine = new int[10];
//		int[] qMachine = new int[10];
//		int p = -1;
//		int q = -1;
//		for (int i = 0; i < 10; i++) {
//			if (aMachineTime[i] >= bMachineTime[i]) {// a 比 b机器费时
//				p++;
//				q++;
//				pMachine[p] = bMachineTime[i];
//				qMachine[q] = aMachineTime[i];
//			} else {
//				p++;
//				q++;
//				pMachine[p] = aMachineTime[i];
//				qMachine[q] = bMachineTime[i];
//			}
//		}
//		for (int i = 0; i < pMachine.length; i++) {
//			System.out.print(pMachine[i] + " ");
//		}
//		System.out.println();
//
//		// p 队列递增
//		int tmp = 0;
//		for (int i = 0; i < pMachine.length; i++) {
//			int middle = pMachine[i];
//			// int middle =0;
//			// 选最小的数
//			int k = i;
//			for (int j = i + 1; j < pMachine.length; j++) {
//				if (pMachine[k] > pMachine[j]) {
//					k = j;
//				}
//			}
//			tmp = pMachine[i];// 每次选出最小的数
//			pMachine[i] = pMachine[k];
//			pMachine[k] = tmp;
//		}

//		for (int i = 0; i < pMachine.length; i++) {
//			System.out.print(pMachine[i] + " ");
//		}
//		System.out.println();
//		for (int i = 0; i < qMachine.length; i++) {
//			System.out.print(qMachine[i] + " ");
//		}
//		System.out.println();
//		// q 队列递减
//		for (int i = 0; i < qMachine.length; i++) {
//			for (int j = i + 1; j < qMachine.length; j++) {
//				if (qMachine[i] <= qMachine[j]) {
//					tmp = qMachine[i];
//					qMachine[i] = qMachine[j];
//					qMachine[j] = tmp;
//				}
//			}
//		}
//		for (int i = 0; i < qMachine.length; i++) {
//			System.out.print(qMachine[i] + " ");
//		}
//	}

	// 参数初始化:生成数据 作业数{50，100，150，200，250}，每种30套
	public static void produceDataset() {

		// --------------------------Dout=k*Din----------------
		// Generate Data
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\";

		// 作业数5种，生成30个实例，共150个实例
		for (int i = 0; i < 30; i++) {
			// 作业 -50
			String dataFilePath50 = pathCommon + "Job-50\\" + "Job-50-" + i + ".txt";// 文件名
			writeFile(dataFilePath50, generateJobs(50));
			// 作业 -100
			String dataFilePath100 = pathCommon + "Job-100\\" + "Job-100-" + i + ".txt";// 文件名
			writeFile(dataFilePath100, generateJobs(100));
			// 作业 -150
			String dataFilePath150 = pathCommon + "Job-150\\" + "Job-150-" + i + ".txt";// 文件名
			writeFile(dataFilePath150, generateJobs(150));
			// 作业 -200
			String dataFilePath200 = pathCommon + "Job-200\\" + "Job-200-" + i + ".txt";// 文件名
			writeFile(dataFilePath200, generateJobs(200));
			// 作业 -250
			String dataFilePath250 = pathCommon + "Job-250\\" + "Job-250-" + i + ".txt";// 文件名
			writeFile(dataFilePath250, generateJobs(250));
		}

	}

	// 数据集来源： 生成数据后写入文件
	// 参数：文件路径文件名path，写入作业任务内容writerTxt
	public static void writeFile(String path, String writerTxt) {// String
																	// writerTxt
		try {
			// FileWriter fw = new
			// FileWriter("D:/JavaWeb/MapReduceScheduler/scheduler-data.txt");
			// BufferedWriter bw =new BufferedWriter(fw);
			File file = new File(path);
			if (!file.exists()) {
				file.createNewFile();
			}
			OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(file));
			BufferedWriter bw = new BufferedWriter(osw);
			// bw.write("1 hellow world!\r\n");
			bw.write(writerTxt);
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 生成作业的的Map、reduce任务
	// 参数：jobNum 作业数，取值{50,100,150,200,250} 文件要写入的内容
	public static String generateJobs(int jobNum) {
		String dataStr = "";
		// ------------------Task 数量 生成-----------------------
		// Map:产生N(a,b)的数 -正态分布/高斯分布
		int aTask = 154, bTask = 558;
		Random random = new Random();
		// Reduce:产生N(c,d)的数 -正态分布/高斯分布
		int cTask = 19, dTask = 145;

		// ------------------Process time 生成-----------------------
		// Map:
		int aTime = 50, bTime = 200;
		// 即均值为a，方差为b的随机数
		// Reduce:
		int cTime = 100, dTime = 300;
		// 即均值为a，方差为b的随机数

		// 均匀分布
		// 生成 [1，2] 80%
		// double factorA = convert(random.nextDouble() * 1 + 1);
		// 生成 [8，10] 20%
		// double factorB = convert(random.nextDouble() * 2 + 8);
		// System.out.println("factorA:"+factorA+",factorB:"+factorB);

		// 每个实例：作业Map任务数，Reduce个数，Map任务输出输入比k，Map输入文件大小以及每个任务的处理时间
		double[] rateArr = { 0.2, 0.4, 0.6, 0.8, 1.0 };

		// Map 每个任务的文件大小{128,192,256,320} MB
		int[] fileSizeArr = { 128, 192, 256, 320 };
		/**
		 * Job实例包括的属性：
		 * 作业Map，reduce任务数、Map任务输出输入比rate{0.2,0.4,0.6,0.8}、Map输入文件大小和每个任务处理时间*
		 * factor 作业Map，reduce任务数，执行时间
		 */
		int countJobId = 0;

		// 80%的作业处理
		for (int i = 0; i < jobNum * 0.8; i++) {
			// 计算reduce的size=Sum（mapsize）*rate/reduceNum；
			int mapFileSizeSum1 = 0;
			// 即均值为a，方差为b的随机数
			int mapTaskNum = Math.abs((int) Math.floor(Math.sqrt(bTask) * random.nextGaussian() + aTask)) + 1;// Math.sqrt(bTask)
			int reduceTaskNum = Math.abs((int) Math.floor(Math.sqrt(dTask) * random.nextGaussian() + cTask)) + 1;// Math.sqrt(dTask)
			countJobId += 1;
			dataStr += "<job id='" + countJobId + "' maptasknum='" + mapTaskNum + "' reducetasknum='" + reduceTaskNum
					+ "'>\r\n";
			// map task
			for (int j = 0; j < mapTaskNum; j++) {
				// 每个任务有不同的sigama(rate)
				double rate = rateArr[getRandomFromArray(rateArr.length)];
				int mapTime = Math
						.abs((int) Math.floor(
								(Math.sqrt(bTime) * random.nextGaussian() + aTime) * (random.nextDouble() * 1 + 1)))
						+ 1; // Math.sqrt(bTime)
				int inputFileSize1 = fileSizeArr[getRandomFromArray(fileSizeArr.length)];
				mapFileSizeSum1 += inputFileSize1 * rate;
				dataStr += "  <maptask runtime='" + mapTime + "' size='" + inputFileSize1 + "'/>\r\n";// "'
																										// sigama='"+rate+
			}

			// reduce task
			int reduceFileSize1 = (int) Math.floor(mapFileSizeSum1 / reduceTaskNum);
			for (int j = 0; j < reduceTaskNum; j++) {
				int reduceTime = Math
						.abs((int) Math.floor(
								(Math.sqrt(dTime) * random.nextGaussian() + cTime) * (random.nextDouble() * 1 + 1)))
						+ 1;
				dataStr += "  <reducetask runtime='" + reduceTime + "' size='" + reduceFileSize1 + "'/>\r\n";
			}

			dataStr += "</job>\r\n";
		}
		// 20%的作业处理
		for (int i = 0; i < jobNum * 0.2; i++) {
			// 计算reduce的size=Sum（mapsize）*rate/reduceNum；
			int mapFileSizeSum2 = 0;
			// 即均值为a，方差为b的随机数
			int mapTaskNum = Math.abs((int) Math.floor(Math.sqrt(bTask) * random.nextGaussian() + aTask)) + 1;
			int reduceTaskNum = Math.abs((int) Math.floor(Math.sqrt(dTask) * random.nextGaussian() + cTask)) + 1;
			countJobId += 1;
			dataStr += "<job id='" + countJobId + "' maptasknum='" + mapTaskNum + "' reducetasknum='" + reduceTaskNum
					+ "'>\r\n";
			// map task
			for (int j = 0; j < mapTaskNum; j++) {
				double rate = rateArr[getRandomFromArray(rateArr.length)];
				int mapTime = Math
						.abs((int) Math.floor(
								(Math.sqrt(bTime) * random.nextGaussian() + aTime) * (random.nextDouble() * 1 + 1)))
						+ 1; // Math.sqrt(bTime)
				int inputFileSize2 = fileSizeArr[getRandomFromArray(fileSizeArr.length)];
				mapFileSizeSum2 += inputFileSize2 * rate;
				dataStr += "  <maptask runtime='" + mapTime + "' size='" + inputFileSize2 + "'/>\r\n";// "'
																										// sigama='"+rate+
			}

			// reduce task
			int reduceFileSize2 = (int) Math.floor(mapFileSizeSum2 / reduceTaskNum);
			for (int j = 0; j < reduceTaskNum; j++) {
				int reduceTime = Math
						.abs((int) Math.floor(
								(Math.sqrt(dTime) * random.nextGaussian() + cTime) * (random.nextDouble() * 1 + 1)))
						+ 1;

				dataStr += "  <reducetask runtime='" + reduceTime + "' size='" + reduceFileSize2 + "'/>\r\n";
			}

			dataStr += "</job>\r\n";
		}
		return dataStr;
	}

	// 数据集调用：random调用随机产生数据集
	public static int getRandomFromArray(int arrLength) {
		// 由于random取值的数组可能是int型或double型，所以返回取值数组的任意位置random
		int index = (int) (Math.random() * arrLength);// 0-1
		return index;
	}

	// 保留小数位数 数据集中用，已注销
	// static double convert(double value) {
	// long l1 = Math.round(value * 100); // 四舍五入 value *100
	// double ret = l1 / 100.0; // 注意：使用 /100.0 而不是 100
	// return ret;
	// }

	// ===========================数据内容处理=============================
	// 读取数据：一个实例 50个作业，100个作业....
	public static List<Job> readFromFile(String filepath) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		List<Job> listJobs = new ArrayList<Job>();
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			// 读取文件路径："D:\\JaveWeb\\MapReduceScheduler\\DataSet\\Job-50\\Job-50.txt"
			Document doc = db.parse(filepath);
			NodeList jobList = doc.getElementsByTagName("job");
			// System.out.println("job num:" + jobList.getLength() + " 个job节点");

			// 遍历<job>
			for (int i = 0; i < jobList.getLength(); i++) {
				Node job = jobList.item(i);
				Element elem = (Element) job;

				// 一条job属性 输出验证
				// System.out.println("id:" + elem.getAttribute("id") +
				// ",maptasknum:" + elem.getAttribute("maptasknum")
				// + ",reducetasknum:" + elem.getAttribute("reducetasknum"));
				Job jobInstance = new Job();
				// 为了TBS算法，加了归属具体作业所在的id
				jobInstance.jobId = Integer.parseInt(elem.getAttribute("id"));

				jobInstance.mapTaskNum = Integer.parseInt(elem.getAttribute("maptasknum"));
				jobInstance.reduceTaskNum = Integer.parseInt(elem.getAttribute("reducetasknum"));

				// map 阶段总的使用时间
				// jobInstance.mapStageTime = 0;

				NodeList mapList = null;
				NodeList reduceList = null;
				List<MapTask> mapTasks = new ArrayList<MapTask>();
				List<ReduceTask> reduceTasks = new ArrayList<ReduceTask>();
				// 遍历<task>
				for (Node node = job.getFirstChild(); node != null; node = node.getNextSibling()) {
					mapList = elem.getElementsByTagName("maptask");
					reduceList = elem.getElementsByTagName("reducetask");
				}
				// 遍历mapList
				for (int j = 0; j < mapList.getLength(); j++) {
					Node mapTask = mapList.item(j);
					Element mapElem = (Element) mapTask;
					MapTask maptaskInstance = new MapTask();
					maptaskInstance.mapRunTime = Integer.parseInt(mapElem.getAttribute("runtime"));
					maptaskInstance.mapSize = Integer.parseInt(mapElem.getAttribute("size"));

					// 为了TBS算法，加了归属具体作业所在的id,作业跟任务是1对多的关系。
					maptaskInstance.belongJobId = jobInstance.jobId;
					// 初始化准备时间为0
					maptaskInstance.mapSetupTime = 0;
					mapTasks.add(maptaskInstance);
					// System.out.println("mapTask runtime:" +
					// mapElem.getAttribute("runtime") + ",size:"
					// + mapElem.getAttribute("size"));

				}
				// 遍历reduceList
				for (int k = 0; k < reduceList.getLength(); k++) {
					Node reduceTask = reduceList.item(k);
					Element mapElem = (Element) reduceTask;
					ReduceTask reducetaskInstance = new ReduceTask();
					reducetaskInstance.reduceRunTime = Integer.parseInt(mapElem.getAttribute("runtime"));
					reducetaskInstance.reduceSize = Integer.parseInt(mapElem.getAttribute("size"));
					// 初始化准备时间为0
					reducetaskInstance.reduceSetupTime = 0;
					reduceTasks.add(reducetaskInstance);
					// System.out.println("reduceTask runtime:" +
					// mapElem.getAttribute("runtime") + ",size:"
					// + mapElem.getAttribute("size"));
				}
				jobInstance.mapTask = mapTasks;
				jobInstance.reduceTask = reduceTasks;
				// System.out.println("task num:" + mapList.getLength() + "
				// 个maptask节点");
				// System.out.println("reduce num:" + reduceList.getLength()
				// +"个maptask节点");
				listJobs.add(jobInstance);
			}

		} catch (ParserConfigurationException | SAXException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// String readText = "";
		// File file = new
		// File("D:\\JaveWeb\\MapReduceScheduler\\DataSet\\Job-50\\Job-50-0.txt");
		// try {
		// InputStreamReader read = new InputStreamReader(new
		// FileInputStream(file), "UTF-8");
		// BufferedReader reader = new BufferedReader(read);
		// String line;
		// while ((line = reader.readLine()) != null) {
		// readText += line + "\r\n";
		// }
		// System.out.println(readText);
		//
		// } catch (UnsupportedEncodingException | FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// System.out.println("listJobs size:" + listJobs.size());
		return listJobs;
	}

	// 递归文件夹下所有文件
	//public static void getFiles(String filePath) {
		// File root=new File(filePath);
		// File[] files=root.listFiles();
		// for(File file:files){
		// if(file.isDirectory()){
		// //递归调用
		// getFiles(file.getAbsolutePath());
		// System.out.println("显示"+filePath+"下所有子目录及其文件："+file.getAbsolutePath());
		// }else{
		// System.out.println("显示"+filePath+"下所有子目录："+file.getAbsolutePath());
		// //追加<jobs>......</jobs>
		// //String appendTail="</jobs>\r\n";
		//
		// try {
		// // FileWriter writer=new FileWriter(file.getAbsolutePath(),true);
		// // writer.write(appendTail);
		// // writer.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// }
		// String
		// path="D:\\JaveWeb\\MapReduceScheduler\\DataSet\\Job-50\\Job-50.txt";
		// String appendTail="</jobs>\r\n";
		// String appendHead="<jobs>\r\n";
		// 尾部增加内容
		// try {
		// FileWriter writer=new FileWriter(path,true);
		// writer.write(appendTail);
		// writer.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// System.out.println("append finish!");
		// 头部增加内容
		// RandomAccessFile randomFile = null;
		// try {
		// randomFile = new RandomAccessFile(path, "rw");
		// randomFile.seek(0);
		// randomFile.writeBytes(appendHead);
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }finally{
		// if(randomFile!=null){
		// try {
		// randomFile.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }
		// }
		//
		// System.out.println("append finish!");
	//}

	// ========================调度过程-调度算法===========================
	// Johnson规则需要计算 ：持续时间，为了排序 持续时间参数设置-----------------------w
	// 把计算出来的每个作业map|reduce阶段的持续时间duration存储在每个作业的stageTime中
	// 参数：作业集合，节点数，每个节点的map slot数目，持续时间评估的参数w
	public static void durationTime(List<Job> listJob, int nodeNum, int mapSlotNumNode, double w) {
		// 可用的map|reduce slot数目
		int slotMapUse = mapSlotNumNode * nodeNum;// 所有的map slot数量
		int slotReduceUse = 2 * nodeNum;// 所有的reduce slot数量

		// 计算map stage阶段时间，可能需要加上准备时间！！！！????
		// w 假设固定 ：Ti=w*T_low+(1-w)*T_up
		for (Job job : listJob) {
			// map上下界定义,最大的map运行时间，总的map运行时间
			double T_map_low = 0, T_map_up = 0, max_map_time = 0, sum_map_time = 0;
			// reduce上下界定义
			double T_reduce_low = 0, T_reduce_up = 0, max_reduce_time = 0, sum_reduce_time = 0;// reduce
																								// 上下界定义
			// map T上下界计算 由于调度问题的准备时间无法确定，所以准备时间采用本地数据读取时间 fd
			for (MapTask maptaskInstance : job.mapTask) {
				// 临时定义任务时间：运行时间+准备时间
				maptaskInstance.mapSetupTime = maptaskInstance.mapSize / fd;
				double tmpMapTaskTime = (maptaskInstance.mapRunTime + maptaskInstance.mapSize / fd);
				sum_map_time += tmpMapTaskTime;// 所有map任务执行总时间
				if (tmpMapTaskTime > max_map_time) {// 计算map上界要用到最大的执行时间
					max_map_time = tmpMapTaskTime;
				}
			}
			T_map_low = Math.round(sum_map_time / slotMapUse);
			T_map_up = (job.mapTaskNum - 1) * sum_map_time / (slotMapUse * job.mapTaskNum) + max_map_time;
			job.mapStageTime = (int) (w * T_map_low + (1 - w) * T_map_up);

			// redue T上下界计算
			for (ReduceTask reducetaskInstance : job.reduceTask) {
				reducetaskInstance.reduceSetupTime = reducetaskInstance.reduceSize / fd;
				double tmpReduceTaskTime = reducetaskInstance.reduceRunTime + reducetaskInstance.reduceSize / fd;
				sum_reduce_time += tmpReduceTaskTime;// 所有reduce任务执行总时间
				if (tmpReduceTaskTime > max_reduce_time) {// 计算reduce上界要用到最大的执行时间
					max_reduce_time = reducetaskInstance.reduceRunTime;
				}

			}
			T_reduce_low = Math.round(sum_reduce_time / slotReduceUse);
			T_reduce_up = (job.reduceTaskNum - 1) * sum_reduce_time / (slotReduceUse * job.reduceTaskNum)
					+ max_reduce_time;
			job.reduceStageTime = (int) (w * T_reduce_low + (1 - w) * T_reduce_up);
			// System.out.println("111mapstagetime:"+job.mapStageTime+",222reducestagetime:"+job.reduceStageTime);

		}
	}

	// 升序排列-------对Job的排序 Johnson法则-------------------------------
	public static List<Job> johnsonSort(List<Job> listjob) {
		// 比较所有job的map阶段完成时间
		// 根据JR1规则对所有作业进行排序
		// 对job集群计数，控制循环次数,因为赋值循环后listjob长度变了
		int jobNum = listjob.size();

		// 排序前输出验证listjob----已验证正确
		// for (Job jo : listjob) {
		// System.out.println("mapstageTime:" + jo.getMapStageTime() +
		// ",reducestageTime:" + jo.getReduceStageTime());
		// }

		// 1.设存储前后两种JR1规则的map、reduce界阶段的作业排序结果，listMapJob、listReduceJob
		List<Job> listMapJob = new ArrayList<Job>();
		// reduce阶段的job排序 ，从小到大，如果与map阶段的合并，需要倒序 Collections.reverse(list);
		List<Job> listReduceJob = new ArrayList<Job>();

		// 2.每次找出所有Job中，map和reduce 的stagetime最小的，找到后移除该作业，在剩余作业中继续寻找
		List<Job> tmplistjob = new ArrayList<Job>();
		for (Job jb1 : listjob) {
			tmplistjob.add(jb1);
		}
		// tmplistjob = listjob;// job集合不断在变化的长度，最长时为所有job长度，每循环一次减1
		// System.out.println("job sum size:" + listjob.size());
		// 每次找出最小的job元素
		for (int i = 0; i < jobNum - 1; i++) {
			// 假设第一个job的mapstagetime最小
			int minJobStageTime = tmplistjob.get(0).getMapStageTime();
			Job minJob = new Job();
			minJob = tmplistjob.get(0);
			int flag = -1;// 标注是map阶段还是reduce阶段产生的stagetime值最小的job
			for (Job jb : tmplistjob) {// 一次遍历完该作业，只能找到一个最小当前job，然后移除该job，继续寻找
				if (jb.getMapStageTime() < minJobStageTime) {// 第一个跟自己比，加等号为了最后一个元素
					minJobStageTime = jb.getMapStageTime(); // 动态变化，保持一直最小的stagetime值
					minJob = jb;
					flag = 0;// 表示map阶段产生
				}
				if (jb.getReduceStageTime() < minJobStageTime) {
					minJobStageTime = jb.getReduceStageTime();
					minJob = jb;
					flag = 1;// 表示reduce阶段产生
				}
			}
			if (flag == 0) {// -1表示当前最小或只有最后一个
				listMapJob.add(minJob);
			} else {
				listReduceJob.add(minJob);
			}
			tmplistjob.remove(minJob);
			// 验证---已验证正确 ，不断变小到1
			// System.out.println("===变化的的tmplistjob size:" +
			// tmplistjob.size());
		}
		// 加最后一个元素,不管map或reduce，都是放中间，所以跟在map后
		listMapJob.add(tmplistjob.get(0));
		//// System.out.println("===JR1 map size:" + listMapJob.size());

		//// System.out.println("===JR1 reduce size:" + listReduceJob.size());

		// 把reduce stage time排序得到的作业倒序
		Collections.reverse(listReduceJob);

		// 把map、reduce stage time排序得到的作业排序合并,再重新赋值给listjob
		listMapJob.addAll(listReduceJob);
		listjob = listMapJob;// 重新赋值，恢复原来大小
		// 验证JR1的作业排序
		// System.out.println("===JR1 last tmplistjob
		// size:"+tmplistjob.size());//应该为1
		// System.out.println("===JR1 sum size:"+listjob.size());
		// System.out.println("job size:"+listjob.size());
		// job的JR1排序后输出验证
		// System.out.println("-------------------------");
		// for (Job jo : listjob) {
		// System.out.println("--mapstageTime:" + jo.getMapStageTime() +
		// ",reducestageTime:" + jo.getReduceStageTime());
		// }
		return listjob;
	}

	// 每个作业单独可以调用对里面的maptask进行排序 降序 LPT原则，是否需要加准备时间(这边准备时间是修正时间速度本地速度fd算)
	public static void sortMapTaskLPT(List<MapTask> maptaskList) {
		Collections.sort(maptaskList, new Comparator<MapTask>() {// 升序
			public int compare(MapTask maptask1, MapTask maptask2) {
				// maptask1.map_L_time
				Integer mt1 = maptask1.mapRunTime + maptask1.mapSetupTime;
				Integer mt2 = maptask2.mapRunTime + maptask2.mapSetupTime;
				// System.out.println("maptask2.mapSetupTime:"+maptask2.mapSetupTime);
				return mt1.compareTo(mt2);
				// return
				// maptask1.getMapRunTime().compareTo(maptask2.getMapRunTime());
			}
		});
		Collections.reverse(maptaskList);// reverse 降序
		// 输出验证
		// for (MapTask mt : maptaskList) {
		// System.out.println("all maptask
		// LPT:"+(mt.getMapRunTime()+mt.getMapSetupTime()));
		// }
		// return maptaskList;
	}

	// 每个作业单独可以调用对里面的maptask进行排序 降序 LPT原则
	public static void sortReduceTaskLPT(List<ReduceTask> reduceTaskList) {
		Collections.sort(reduceTaskList, new Comparator<ReduceTask>() {// 这是升序，reverse
																		// 降序
			public int compare(ReduceTask reducetask1, ReduceTask reducetask2) {
				Integer rt1 = reducetask1.reduceRunTime + reducetask1.reduceSetupTime;
				Integer rt2 = reducetask2.reduceRunTime + reducetask2.reduceSetupTime;
				return rt1.compareTo(rt2);
				// return
				// reducetask1.getReduceRunTime().compareTo(reducetask2.getReduceRunTime());
			}
		});
		Collections.reverse(reduceTaskList);// 降序
		// 输出验证
		// for (ReduceTask mt : reduceTaskList) {
		// System.out.println("reduce LPT:"+mt.reduceRunTime);
		// }
		// return reduceTaskList;
	}

	// 降序排列

	// 总的调度参数传入，参数控制入口
	/*
	 * w={0.1,0.3,0.5,0.7,0.9} 会影响johnson排序 nodeNum={10,15,20,25,30} 并行化程度
	 * mapSlotNum={2,4,6,8},reduceSlotNum=2 作业数也是参数={50,100,150,200,250}
	 * 取决于处理的文件
	 */
	// 初始化所有slot，map、reduce 当前使用时间、速度
	// 参数：节点数量，每个节点map slot数量，每个节点的reduce数量固定为2
	public static Map initSlots(int nodeNum, int mapSlotNum) {
		// 存储listMapSlot、listReduceSlot
		Map map = new HashMap();
		// nodeNum*mapSlotNum 总的map slot数 先假设每个节点mapSlot个数为2
		// 定义三种速度 fr(30)，fn(50)各占0.3，0.3，fd(100)占0.4 速度是全局变量
		// int nodeNum = 10, mapSlotNum = 2;
		listMapSlot = new ArrayList<MapSlot>();
		int sumMapSlot = nodeNum * mapSlotNum;// 每个节点的slot个数为mapSlotNum

		// 同节点 100
		for (int i = 0; i < sumMapSlot * 0.4; i++) {
			MapSlot ms100 = new MapSlot();
			ms100.currentMapSlotTime = 0;
			ms100.mapSlotSpeed = fd;
			listMapSlot.add(ms100);
		}
		// 同机架，不同节点 50
		for (int i = 0; i < sumMapSlot * 0.3; i++) {
			MapSlot ms50 = new MapSlot();
			ms50.currentMapSlotTime = 0;
			ms50.mapSlotSpeed = fr;
			listMapSlot.add(ms50);
		}
		// 不同机架：30
		for (int i = 0; i < sumMapSlot * 0.3; i++) {
			MapSlot ms30 = new MapSlot();
			ms30.currentMapSlotTime = 0;
			ms30.mapSlotSpeed = fn;
			listMapSlot.add(ms30);
		}
		map.put("listMapSlot", listMapSlot);
		// reduce slot
		listReduceSlot = new ArrayList<ReduceSlot>();
		int sumReduceSlot = nodeNum * 2;
		for (int i = 0; i < sumReduceSlot * 0.4; i++) {
			ReduceSlot rs100 = new ReduceSlot();
			rs100.currentReduceSlotTime = 0;
			rs100.reduceSlotSpeed = fd;
			listReduceSlot.add(rs100);
		}
		for (int i = 0; i < sumReduceSlot * 0.3; i++) {
			ReduceSlot rs50 = new ReduceSlot();
			rs50.currentReduceSlotTime = 0;
			rs50.reduceSlotSpeed = fr;
			listReduceSlot.add(rs50);
		}
		for (int i = 0; i < sumReduceSlot * 0.3; i++) {
			ReduceSlot rs30 = new ReduceSlot();
			rs30.currentReduceSlotTime = 0;
			rs30.reduceSlotSpeed = fn;
			listReduceSlot.add(rs30);
		}
		// 验证slot 赋值初始化情况------已验证
		// for(MapSlot ms:listMapSlot){
		// System.out.println("mapslot current
		// time:"+ms.currentMapSlotTime+",slotSpeed:"+ms.mapSlotSpeed);
		// }
		// for(ReduceSlot rs:listReduceSlot){
		// System.out.println("reduceslot current
		// time:"+rs.currentReduceSlotTime+",slotSpeed:"+rs.reduceSlotSpeed);
		// }

		map.put("listReduceSlot", listReduceSlot);
		//// System.out.println("map slot size:" + listMapSlot.size() + ",reduce
		//// slot size:" + listReduceSlot.size());
		return map;
	}

	// Reduce前需要对任务按照sigma m的非降序（增序）排序 sigma-》stagetime
	public static List<Job> jobSortAsMapStageTime(List<Job> listjob) {
		// 比较所有job的mapStageTime
		Collections.sort(listjob, new Comparator<Job>() {// 增序
			public int compare(Job job1, Job job2) {
				return job1.getMapStageTime().compareTo(job2.getMapStageTime());
			}
		});
		// // 验证输出-------正确
		// for (Job job : listjob) {
		// System.out.println("MapStageTime:" + job.getMapStageTime());
		// }
		return listjob;
	}

	// =========================调度算法EASS、EFSS启发式算法==========================
	// 任务的slot分配，关于EASS、EFSS算法
	// 参数：mapslot集合，reduceslot集合，作业集合，EA_EF方法选择
	// 返回Cmax
	public static double taskToSlotScheduler_EA_EF(List<MapSlot> listMapSlot, List<ReduceSlot> listReduceSlot,
			List<Job> listJob, char m_r) {
		// 约束一个作业的所有map任务完成后再开始reduce任务
		for (Job job : listJob) {
			// 使用LPT规则对每个作业的map、reduce进行任务排序 ,执行时间加上准备时间进行,最长时间优先，降序
			sortMapTaskLPT(job.mapTask);
			sortReduceTaskLPT(job.reduceTask);

			// 验证LPT结果是否正确 从大到小排序 -----正确
			// 赋值sigma-》stagetime=0;
			job.mapStageTime = 0;
			// 对每个map任务处理
			for (MapTask maptask : job.mapTask) {
				// 当前任务下标
				int indexMapTask = job.mapTask.indexOf(maptask);// 当前map任务下标
				int indexMapSlot;
				if (m_r == 'A') {// EA
					// EA：最早可用map slot
					// 参数：mapslot集合，reduceslot集合，map|reduce阶段的选择
					// indexMapSlot = minSlotTime_EA_slot_index(listMapSlot,
					// null, 'm');// 最早可用slot
					indexMapSlot = minMapSlotTime_EA_slot_index(listMapSlot);
					// System.out.println(" min indexMapSlot:" + indexMapSlot +
					// ",current time:"
					// + listMapSlot.get(indexMapSlot).hashCode());

				} else {// EF
						// EF:最早完成任务 map slot
					// 参数：mapslot集合，reduceslot集合，任务修正时间，map|reduce阶段的选择
					indexMapSlot = minMapSlotTime_EF_slot_index(listMapSlot, maptask);
				}
				// 参数：具体作业，任务下标，分配的slot编号，map/reduce任务选择
				TAP(job, indexMapTask, indexMapSlot, 'm');// job,task_j,slot_k,m_r
				// 准备时间是：任务文件大小/该slot速度
				// int setupTime1 = (int) Math.floor((maptask.mapSize) /
				// (listMapSlot.get(indexMapSlot).mapSlotSpeed));

				// map 阶段的sigma m ，为了Reduce前的排序
				// job.mapStageTime += (setupTime1 + maptask.mapRunTime);

				// 运行完该任务，该slot的当前时间要加刚才运行的任务的时间
				// listMapSlot.get(indexMapSlot).currentMapSlotTime +=
				// maptask.mapRunTime + setupTime1;

			}

			// System.out.println("-------------------------");// 换作业
		}
		// Reduce阶段：将所有的作业按照map sigma m的非降序(增序)牌系列-----stagetime
		listJob = jobSortAsMapStageTime(listJob);

		for (Job job : listJob) {
			// 根据reduce slot数量，把map slot相同数量的slot上的stageTime赋值给reduce slot
			job.reduceStageTime = job.mapStageTime;
			// 每个作业的map阶段完成时间传给reduce，在此基础上继续执行reduce任务
			// System.out.println("reduceStageTime init:"+job.reduceStageTime);

			// 对reduce任务进行处理
			for (ReduceTask reduceTask : job.reduceTask) {
				// 当前reduce任务下标
				int indexReduceTask = job.reduceTask.indexOf(reduceTask);
				int indexReduceSlot;
				if (m_r == 'A') {// EA
					// EA：最早可用reduce slot
					// indexReduceSlot = minSlotTime_EA_slot_index(null,
					// listReduceSlot, 'r');
					indexReduceSlot = minReduceSlotTime_EA_slot_index(listReduceSlot);
				} else {// EF
					// EF:最早完成任务 reduce slot
					// 即将执行的任务的修正时间：运行时间+准备时间
					// int tmp_L_task = reduceTask.reduceRunTime +
					// reduceTask.reduceSetupTime;
					// 参数：mapslot集合，reduceslot集合，任务修正时间，map|reduce阶段的选择
					indexReduceSlot = minReduceSlotTime_EF_slot_index(listReduceSlot, reduceTask);
				}
				// 参数：具体作业，任务下标，分配的slot编号，map/reduce任务选择
				TAP(job, indexReduceTask, indexReduceSlot, 'r');

			}
			// for (ReduceSlot rs : listReduceSlot) {
			// // rs.currentReduceSlotTime=0;
			// System.out.println("reduce slot currentime:" +
			// rs.currentReduceSlotTime);
			// }
			// System.out.println("-------------------------");// 换作业

		}
		// 返回最大的reduce阶段的处理时间
		double Cmax = maxReduceStageTime(listJob);// listReduceSlot.get(maxReduceSlotValue(listReduceSlot)).currentReduceSlotTime;
		// System.out.println("Cmax:" + Cmax);
		// 返回最大的reduce slot 时间 与上一种算法得到的值一样?reduce slot开始执行时间不为0
		// double Cmax1 = maxReduceSlotTime(listReduceSlot);
		// System.out.println("Cmax1:" + Cmax1);
		return Cmax;
	}

	// 任务分配过程TAP
	// 参数：具体作业，任务下标，分配的slot编号，map/reduce任务选择
	// 用下标不用任务对象的原因是，因为任务对象可能为maptask，可能为reducetask，用下标可以在该作业中根据m_r选择，获取对应具体任务对象
	public static void TAP(Job job, int j, int k, char m_r) {
		// 运行时间
		// double currentSlotKTime;// 人k 针对slot
		double completeJob_i_j_Time;// 当前作业任务J(i，j)的完成时间C、
		// double m_r_stageTime;// 当前任务所处作业阶段完成时间，针对作业的map/reduce阶段
		if (m_r == 'm') {
			// 1.执行时间+准备时间
			// 准备时间是：任务文件大小/该slot速度 重新改变setup time 时间
			// map任务中的setuptime可以不用定义，直接用临时变量取代
			job.mapTask.get(j).mapSetupTime = (int) Math
					.round((job.mapTask.get(j).mapSize) / (listMapSlot.get(k).mapSlotSpeed));

			// 2.当前slot时间
			// currentSlotKTime =
			// listMapSlot.get(k).currentMapSlotTime+(maptask.mapSetupTime +
			// runTime);// 针对slot

			// 3.当前作于作业，任务阶段（m/r）时间
			// map 阶段的sigma m ，为了Reduce前的排序 是否需更新,加更新结果正常一些
			job.mapStageTime += (job.mapTask.get(j).mapSetupTime + job.mapTask.get(j).mapRunTime);//
			// map某任务调度完需要加上该任务的时间---更新
			// m_r_stageTime = job.mapStageTime; //
			// 当前任务所处作业阶段完成时间，针对作业的map/reduce阶段

			// 4.作业完成时间 针对slot
			completeJob_i_j_Time = listMapSlot.get(k).currentMapSlotTime
					+ (job.mapTask.get(j).mapSetupTime + job.mapTask.get(j).mapRunTime);// 针对slot

			if (completeJob_i_j_Time > job.mapStageTime) {
				job.mapStageTime = (int) completeJob_i_j_Time;
			}
			// 5.更新新的当前slot时间
			// 运行完该任务，该slot的当前时间要加刚才运行的任务的时间---更新
			listMapSlot.get(k).currentMapSlotTime = (int) completeJob_i_j_Time;

			// TBS算法，需要记录添加的mapTask的顺序,每个Slot需要记录maptask任务 ，只对map阶段，没有reduce阶段
			listMapSlot.get(k).maptaskSlotList.add(job.mapTask.get(j));

		} else {// reduce
			// 1.执行时间+准备时间
			// ReduceTask reducetask = job.reduceTask.get(j);////
			// 通过下标取到具体reduce任务
			job.reduceTask.get(j).reduceSetupTime = (int) Math // 准备时间
					.round((job.reduceTask.get(j).reduceSize) / (listReduceSlot.get(k).reduceSlotSpeed));
					// 2.当前slot时间
					// currentSlotKTime =
					// listReduceSlot.get(k).currentReduceSlotTime
					// +(reducetask.reduceSetupTime + runTime);// 人k
					// 针对slot
					// reduce阶段该任务完成的时间更新

			// 4.作业完成时间 针对slot
			completeJob_i_j_Time = Math.max(listReduceSlot.get(k).currentReduceSlotTime, job.mapStageTime)// job.mapStageTime
																											// job.reduceStageTime
					+ job.reduceTask.get(j).reduceSetupTime + job.reduceTask.get(j).reduceRunTime;

			// 3.当前作于作业，任务阶段（m/r）时间
			// m_r_stageTime = job.mapStageTime; //
			// 当前任务所处作业阶段完成时间，针对作业的map/reduce阶段

			job.reduceStageTime += (job.reduceTask.get(j).reduceSetupTime + job.reduceTask.get(j).reduceRunTime);//
			if (completeJob_i_j_Time > job.reduceStageTime) {
				job.reduceStageTime = (int) completeJob_i_j_Time;
			}
			// 5.更新新的当前slot时间
			// 运行完该任务，该slot的当前时间要加刚才运行的任务的时间---更新
			listReduceSlot.get(k).currentReduceSlotTime = (int) completeJob_i_j_Time;// +=
																						// (job.reduceTask.get(j).reduceSetupTime
																						// +
																						// job.reduceTask.get(j).reduceRunTime);
		}
		return;
	}

	// Cmax计算
	// 得到所有作业，reduce完成最大的时间值 reduceStageTime
	public static double maxReduceStageTime(List<Job> listJob) {
		double Cmax = 0.0;
		for (Job job : listJob) {
			// 验证输出所有reduce stage time
			// System.out.println("result reduce stage time:" +
			// job.reduceStageTime);
			if (job.reduceStageTime > Cmax) {
				Cmax = job.reduceStageTime;// 动态变化取到的最大值
			}
		}
		// System.out.println("listjob last reduce time:" +
		// listjob.get(49).getReduceStageTime());
		return Cmax;
	}

	// Cmax计算
	// 返回最大的Reduce slot的值
	public static double maxReduceSlotTime(List<ReduceSlot> listReduceSlot) {
		double Cmax = 0.0;
		for (ReduceSlot rs : listReduceSlot) {
			if (rs.currentReduceSlotTime > Cmax) {
				Cmax = rs.currentReduceSlotTime;
			}
		}
		return Cmax;
	}

	// EA算法：从所有map|reduce slot中选当前时间最小值
	// 参数：所有map slot,所有reduce slot,map|reduce阶段选择,没有参数为null
	// 返回对应的map|reduce slot的List中下标
	public static Integer minMapSlotTime_EA_slot_index(List<MapSlot> listMapSlot) {
		// 要返回 的下标值
		Integer minIndex = 0;
		// 计数，为了返回下标值
		int count = -1;
		// 假设第一个map slot的当前时间最小
		Integer minMapSlotTime = listMapSlot.get(0).currentMapSlotTime;
		for (MapSlot ms : listMapSlot) {
			count++;
			// 根据EA
			if (ms.currentMapSlotTime < minMapSlotTime) {// 如果比第一个小，则取代
				minMapSlotTime = ms.currentMapSlotTime;// 要动态更新最小值，为了下一轮的比较
				minIndex = count;
			}
		}
		return minIndex;
	}

	public static Integer minReduceSlotTime_EA_slot_index(List<ReduceSlot> listReduceSlot) {
		// 要返回 的下标值
		Integer minIndex = 0;
		// 计数，为了返回下标值
		int count = -1;
		// 假设第一个map slot的当前时间最小
		// 假设第一个reduce slot的当前时间最小
		Integer minReduceSlotTime = listReduceSlot.get(0).currentReduceSlotTime;
		for (ReduceSlot rs : listReduceSlot) {
			count++;
			// 验证slot值，reduce slot个数 slot只对reduce阶段前reduce slot的个数排序有效
			// System.out.println("reduce slot current
			// time:"+rs.currentReduceSlotTime);
			if (rs.currentReduceSlotTime < minReduceSlotTime) {
				minReduceSlotTime = rs.currentReduceSlotTime;// 要动态更新最小值，为了下一轮的比较
				minIndex = count;
			}
		}
		return minIndex;
	}

	 
	// EF算法：从所有map slot中选最早完成时间
	// 参数：mapslot集合，当前处理的任务
	// 返回对应的map slot的List中下标
	public static Integer minMapSlotTime_EF_slot_index(List<MapSlot> listMapSlot, MapTask maptask) {
		// 返回最小值的下标
		Integer minIndex = 0;
		int count = -1;
		// 任务的修正时间：运行速度+准备时间(任务大小/速度)
		double L_time = 0;
		// 假设第一个slot加上当前任务的修正时间的值最小
		// 即将执行的任务的修正时间：运行时间+文件大小/传输速率
		// 不同slot的处理速度不同，导致任务的准备时间不同
		MapSlot ms0 = listMapSlot.get(0);
		double minMapSlotTime = ms0.currentMapSlotTime + (maptask.mapRunTime + (maptask.mapSize / ms0.mapSlotSpeed));

		for (MapSlot ms : listMapSlot) {
			count++;
			L_time = maptask.mapRunTime + (maptask.mapSize / ms.mapSlotSpeed);
			// 根据EF:当前遍历得到的
			if ((ms.currentMapSlotTime + L_time) < minMapSlotTime) {// 如果比第一个小，则重新赋最小值
				minMapSlotTime = ms.currentMapSlotTime + L_time;// 最小值比较动态变化
				minIndex = count;
			}
		}
		return minIndex;
	}

	// EF算法：从所有reduce slot中选最早完成时间
	// 参数：reduceslot集合，当前处理的任务
	// 返回对应的reduce slot的List中下标
	public static Integer minReduceSlotTime_EF_slot_index(List<ReduceSlot> listReduceSlot, ReduceTask reducetask) {
		// 返回最小值的下标
		Integer minIndex = 0;
		int count = -1;
		// 任务的修正时间：运行速度+准备时间(任务大小/速度)
		double L_time = 0;
		// 假设第一个reduce slot加上当前任务的修正时间的值最小
		ReduceSlot rs0 = listReduceSlot.get(0);
		double minReduceSlotTime = rs0.currentReduceSlotTime
				+ (reducetask.reduceRunTime + (reducetask.reduceSize / rs0.reduceSlotSpeed));

		for (ReduceSlot rs : listReduceSlot) {
			count++;
			L_time = reducetask.reduceRunTime + (reducetask.reduceSize / rs.reduceSlotSpeed);
			if ((rs.currentReduceSlotTime + L_time) < minReduceSlotTime) {// 如果比第一个小，则重新赋最小值
				minReduceSlotTime = rs.currentReduceSlotTime + L_time;// 最小值比较动态变化
				minIndex = count;
			}
		}
		return minIndex;

	}

	// ------得到reduce 最大的slot上的时间值-------调用需要
	public static Integer maxReduceSlotValue(List<ReduceSlot> listReduceSlot) {
		Integer maxReduceSlot = listReduceSlot.get(0).currentReduceSlotTime;
		int count = -1;
		Integer maxIndex = 0;
		for (ReduceSlot rs : listReduceSlot) {
			count++;
			if (rs.currentReduceSlotTime > maxReduceSlot) {
				maxIndex = count;
			}
		}
		return maxIndex;
	}

	// ========================调度算法TBS启发式算法=========================
	// 任务的slot分配，关于EASS、EFSS算法
	// 参数：mapslot集合，reduceslot集合，作业集合
	// 返回Cmax
	public static double taskToSlotScheduler_TBS(List<MapSlot> listMapSlot, List<ReduceSlot> listReduceSlot,
			List<Job> listJob) {
		// 1.初始化所有slot initSlots 该函数外
		// 2.初始化所有作业，读取数据成listjob readFromFile(path) 该函数外
		// 计算算法调度时间用的startime、endtime
		long starTime = System.currentTimeMillis();
		// 返回值的定义
		double Cmax = 0;
		// 3.用LPT(最长处理时间优先)对所有作业的map任务进行排序 ，如何记住每个任务所在的job //本排在1前，但顺序不影响
		// 为了让任务属于具体哪个作业明确，加上一个字段 Integer belongId
		// 新增一个List<MapTask>存放所有作业的的所有map 任务
		List<MapTask> maptaskAll = new ArrayList<MapTask>();

		for (Job jb : listJob) {
			// 遍历所以作业总map任务添加到新的list中，为便于排序
			for (MapTask mt : jb.mapTask) {
				maptaskAll.add(mt);
			}
		}
		//// System.out.println("maptaskAll size:" + maptaskAll.size());
		// 使用LPT进行排序 maptask runtime降序 是否需要加准备时间！！！！！！！！！！！！
		sortMapTaskLPT(maptaskAll);

		// for (MapTask mt : maptaskAll) {
		// 验证---已验证可以输出map 任务对应的job的id
		// System.out.println("maptaskAll elem jobid:"+mt.getBelongJobId());
		// 验证--已验证LPT排序正确
		// System.out.println("maptaskAll elem runtime:"+mt.mapRunTime);//
		// }

		// 对所有的任务根据 arg min{人k+s+p} 用EF取最小的slot
		int indexMapSlot;
		int indexMapTask;// 在作业中位置
		Integer jobId;
		// 通过任务的jobid得到对应的job对象
		Job jobInstance = new Job();
		for (MapTask mta : maptaskAll) {
			// 4.对任务进行最早完成时间的slot分配 ：人k+s+p 当前slot时间+准备时间+执行时间
			// 得到最早可用的Map slot下标
			indexMapSlot = minMapSlotTime_EF_slot_index(listMapSlot, mta);
			// 通过任务得到作业id，从而得到对应的作业
			jobId = mta.belongJobId;
			jobInstance = findJobByJobId(listJob, jobId);
			// 在作业中位置
			indexMapTask = jobInstance.mapTask.indexOf(mta);
			// 在map阶段调用TAP TAP(job,int indexTask,int slotk,m);
			TAP(jobInstance, indexMapTask, indexMapSlot, 'm');
		}

		// johnson 对作业排序
		listJob = johnsonSort(listJob);// 对作业排序
		// // ---------------根据johnson的作业排序-------------
		// // 记录JR1排序下的jobId的排序,johnson 算法 该 函数外调用,根据JR1排序排对应的job id

		// ---------------根据johnson的作业排序-------------
		// 记录JR1排序下的jobId的排序,johnson 算法 该 函数外调用,根据JR1排序排对应的job id
		List<Integer> jobIdJR1Sort = new ArrayList<Integer>();
		for (Job jb : listJob) {
			jobIdJR1Sort.add(jb.jobId);// JR1排序下的jobId的排序
			// System.out.println("jobId:"+jb.jobId);
		}
		// 5.根据johnson的作业排序，移动任务 jobIdJR1Sort 作业的JR1排序List
		// TAP每个slot上有map task调度的序列
		for (MapSlot ms : listMapSlot) {
			// 对每个map slot上的任务单独排序
			// 一个map slot上的处理 key为jobid,value为List<MapTask>
			Map<Integer, List<MapTask>> map_jobid_maptask = new HashMap<Integer, List<MapTask>>();
			for (MapTask mt : ms.maptaskSlotList) {// 遍历slot上的maptask任务
				Integer jobid = mt.getBelongJobId();
				// System.out.println("jobid:" + jobid);
				// 通过key得到listmaptask
				List<MapTask> lmt = new ArrayList<MapTask>();
				if (map_jobid_maptask.get(jobid) == null) {
					lmt.add(mt);// 为null则直接添加
				} else {
					lmt = map_jobid_maptask.get(jobid);// 否则在原先基础上添加
					lmt.add(mt);
				}
				map_jobid_maptask.put(jobid, lmt);
			}
			// 对每个map slot上的key进行排序，即对对应的key-value进行排序，与johnson一样的作业排序
			// 新建一个List<MapTask> ，用于存储该map slot排序后的值
			List<MapTask> newSlotMapTasks = new ArrayList<MapTask>();
			for (Integer jobElem : jobIdJR1Sort) {
				// 遍历里面的key值，对key值进行排序
				for (Integer taskKeyId : map_jobid_maptask.keySet()) {
					if (jobElem == taskKeyId) {// 如果id作业相等,则添加对应的value,有多个maptask的值
						newSlotMapTasks.addAll(map_jobid_maptask.get(taskKeyId));
						// System.out.print(taskKeyId+",");
					}
				}

			}
			// 把新排序好的map slot上的maptask，赋值给原先的listMapTask
			ms.maptaskSlotList = newSlotMapTasks;
			// System.out.println("-------slot
			// change---------size:"+ms.maptaskSlotList.size());
		}

		// 7、更新每个任务的完成时刻(currentSlotTime)和所属作业的Map阶段完成时刻(stagetime) listJob
		// 通过map slot上的任务分配情况，确定每个作业的stagetime和每个slot的当前时间等
		// 初始化listjob所有内容，通过map slot上分配好的任务确定任务执行时间分配，stagetime

		for (Job jbElem : listJob) {
			jbElem.mapStageTime = 0;// 重新调度做准备
		}

		for (MapSlot ms_jb : listMapSlot) {
			// 初始化所有MapSlot的当前时间为0，根据具体任务排序累加
			ms_jb.currentMapSlotTime = 0;
			// 依次遍历每个map slot上任务排序
			for (MapTask mt_jb : ms_jb.maptaskSlotList) {// slot上的map任务排序
				// 验证map的setup时间是否存在(已赋值)？setuptime-----已验证存在
				// System.out.println("task setup time:"+mt_jb.mapSetupTime);
				// --每执行一个map任务，job上任务执行情况的stagetime更新--
				// 根据任务找到对应job
				Integer job_id = mt_jb.belongJobId;
				Job jobCase = findJobByJobId(listJob, job_id);
				// jobCase.mapStageTime += (mt_jb.mapRunTime +
				// mt_jb.mapSetupTime);
				// 确定对应作业的stageTime改变
				Integer jobIndex = listJob.indexOf(jobCase);
				listJob.get(jobIndex).mapStageTime += (mt_jb.mapRunTime + mt_jb.mapSetupTime);
				// System.out.println("listJob.size:"+listJob.size());

				// --每执行一个map任务，slot 当前时间更新--
				ms_jb.currentMapSlotTime += (mt_jb.mapRunTime + mt_jb.mapSetupTime);

				// 作业的当前完成时刻stagetime要考虑当下slot上的时间，取大的时间为准
				if (listJob.get(jobIndex).mapStageTime < ms_jb.currentMapSlotTime) {
					listJob.get(jobIndex).mapStageTime = ms_jb.currentMapSlotTime;
				}

			}
		}
		// System.out.println("listMapSlot.size:"+listMapSlot.size() );//= 0;//
		// 重新调度做准备
		// System.out.println("------alltasksize:"+alltasksize);
		// 验证map的stagetime ---已验证正确
		// for(Job jbElem:listJob){
		// System.out.println("new job stage time:"+jbElem.mapStageTime);
		// }
		// -------------Reduce阶段-----------
		// 8. 将作业按住sigma m(stagetime)非降序(增序)排序 Reduce阶段
		listJob = jobSortAsMapStageTime(listJob);
		// 后续跟EFSS过程类似
		for (Job job : listJob) {
			// 9.对每个作业的reduce任务按照LPT排序
			sortReduceTaskLPT(job.reduceTask);
			// 根据reduce slot数量，把map slot相同数量的slot上的stageTime赋值给reduce slot
			job.reduceStageTime = job.mapStageTime;
			// 对reduce任务进行处理
			for (ReduceTask reduceTask : job.reduceTask) {
				// 当前reduce任务下标
				int indexReduceTask = job.reduceTask.indexOf(reduceTask);
				int indexReduceSlot;
				// EF:最早完成任务 reduce slot
				// 即将执行的任务的修正时间：运行时间+准备时间
				// int tmp_L_task = reduceTask.reduceRunTime +
				// reduceTask.reduceSetupTime;
				// 参数：mapslot集合，reduceslot集合，任务修正时间，map|reduce阶段的选择
				indexReduceSlot = minReduceSlotTime_EF_slot_index(listReduceSlot, reduceTask);
				// 参数：具体作业，任务下标，分配的slot编号，map/reduce任务选择
				TAP(job, indexReduceTask, indexReduceSlot, 'r');
			}
		}
		// 返回最大的reduce slot 时间 与上一种算法得到的值一样
		// Cmax = maxReduceSlotTime(listReduceSlot);
		// 得到最长的调度时间
		// System.out.println("listjob size===:"+listJob.size());
		Cmax = maxReduceStageTime(listJob);
		// System.out.println("job size ===:"+listJob.size());
		return Cmax;
	}

	// 通过jobId返回job对象
	public static Job findJobByJobId(List<Job> listJob, Integer jobid) {
		Job job = new Job();
		for (Job jb : listJob) {
			if (jb.jobId == jobid) {
				job = jb;
			}
		}
		return job;
	}

	// =========================评价调度指标=============================
	// 评价调度的下界： LB={max{LB1,LB2}};
	public static double lowerBound(List<Job> listJob, int nodeNum, int mapSlotNum) {
		// for(Job jb:listJob){
		// for(MapTask mt:jb.mapTask){
		// System.out.println("map setup time:"+mt.mapSetupTime);
		// }
		// for(ReduceTask rt:jb.reduceTask){
		// System.out.println("reduce setup time:"+rt.reduceSetupTime);
		// }
		// }
		// Mr表示Reduce slot的数量
		// Mm表示Map Slot数量
		int Mm = nodeNum * mapSlotNum;
		int Mr = nodeNum * 2;// 因为每个节点固定包含reduce slot数目为2
		// System.out.println("listJob size:"+listJob.size());
		// 验证准备时间是否是调度时存入时间
		// for(Job jb:listJob){
		// for(MapTask mt:jb.mapTask){
		// System.out.println("map setup time:"+mt.getMapSetupTime());
		// }
		// for(ReduceTask rt:jb.reduceTask){
		// System.out.println("reduce setup time:"+rt.getReduceSetupTime());
		// }
		// }

		// 所有作业map|reduce的任务的修正时间总和 LB1,LB2都要用到
		int allTaskL_map_time = 0;
		int allTaskL_reduce_time = 0;
		// 定义要求变量
		double LB = 0.0, LB1 = 0, LB2 = 0;

		// (所有作业中map|reduce最大的任务的修正时间 )的最小任务所在作业 LB2
		MapTask maptask = listJob.get(0).mapTask.get(0);// 假设第一作业的第一个map任务的修正时间最小

		// 最小的map 任务的修正时间 准备时间最小是用最大传输速度取代其他传输速度麽？因为没发生调度(maptask.mapSize / fd)
		int min_L_map_time = maptask.mapRunTime + maptask.mapSetupTime;

		ReduceTask reducetask = listJob.get(0).reduceTask.get(0);// 假设第一作业的第一个reduce任务的修正时间最小
		// 最小的reduce 任务的修正时间
		// 准备时间最小是用最大传输速度取代其他传输速度麽？因为没发生调度(reducetask.reduceSize / fd);
		int min_L_reduce_time = reducetask.reduceRunTime + reducetask.reduceSetupTime;

		// 已验证正确
		// System.out.println("计算前
		// min_L_map_time:"+min_L_map_time+",min_L_reduce_time:"+min_L_reduce_time);
		// 要使修正时间下界更小，数据要放本地，传输速度选择最大 100 fd
		for (Job job : listJob) {
			// 一个作业的 map 阶段修正时间总和
			job.L_map_sumTime = 0;

			// 一个作业map阶段最大修正时间的任务的修正时间
			int max_L_map_time = 0;
			// 一个作业的map任务的修正时间总和
			for (MapTask mt : job.mapTask) {
				// 一个map任务的修正时间
				int tmpMapTaskLtime = mt.mapRunTime + mt.mapSetupTime;
				// 修正时间：执行时间P+最小准备时间 min S
				job.L_map_sumTime += tmpMapTaskLtime;

				// 为得到该作业所有map任务中最大的修正时间的任务的修正时间。LB2
				if (tmpMapTaskLtime > max_L_map_time) {
					max_L_map_time = tmpMapTaskLtime;
				}
			}
			// 所有作业的map任务的修正时间总和
			allTaskL_map_time += job.L_map_sumTime;

			// reduce 阶段修正时间
			job.L_reduce_sumTime = 0;
			// 一个作业map阶段最大修正时间任务的修正时间
			int max_L_reduce_time = 0;
			// 一个作业的reduce任务的修正时间总和
			for (ReduceTask rt : job.reduceTask) {
				// 一个reduce任务的修正时间
				int tmpReduceTaskLtime = rt.reduceRunTime + rt.reduceSetupTime;
				job.L_reduce_sumTime += tmpReduceTaskLtime;

				// 为得到该作业所有reduce任务中最大的修正时间的任务的修正时间 LB2
				if (tmpReduceTaskLtime > max_L_reduce_time) {
					max_L_reduce_time = tmpReduceTaskLtime;
				}
			}
			// 所有作业的reduce任务的修正时间总和
			allTaskL_reduce_time += job.L_reduce_sumTime;

			// (所有作业中map|reduce最大的任务的修正时间 )的最小任务所在作业 LB2
			if (min_L_map_time > max_L_map_time) {// job中的任务修正时间比这个大
				min_L_map_time = max_L_map_time;// 取小map任务修正时间
			}
			if (min_L_reduce_time > max_L_reduce_time) {
				min_L_reduce_time = max_L_reduce_time;
			}

		}
		//// System.out.println("allTaskL_map_time:" + allTaskL_map_time +
		//// ",allTaskL_reduce_time:" + allTaskL_reduce_time);
		// 验证修正时间，最小修正时间
		//// System.out.println("min_L_map_time:" + min_L_map_time +
		//// ",min_L_reduce_time:" + min_L_reduce_time);
		// 保留四位有效数字
		DecimalFormat df = new DecimalFormat("#0.0000");
		// -------------------计算LB1------------------------
		// 根据每个作业Map阶段的修正时间排序 从小到大 升序---验证正确
		jobSortAsMapLBTime(listJob);
		double LB1_x = 0.0;// LB1=max{x,y},其中的x数
		// System.out.println("LB listJob size1:"+listJob.size());
		// 取Mr个map作业的作业元素 Hm(Mr)，Mr：reduce slot的数量
		for (int i = 0; i < Math.min(Mr,listJob.size()); i++) {

			LB1_x += listJob.get(i).L_map_sumTime;
		}
		LB1_x += allTaskL_reduce_time;// 所有分子累加值 所有作业的reduce的修正时间
		LB1_x = LB1_x / Mr;// 除以分母
		df.format(LB1_x);

		// 根据每个作业Reduce阶段的修正时间排序 从小到大 升序
		jobSortAsReduceLBTime(listJob);
		//// 取Mm个reduce作业的作业元素 Hr(Mm)，Mm：Map slot的数量
		double LB1_y = 0;// LB1=max{x,y},其中的y数
		// 取Mm个reduce作业的作业元素 Hm(Mr)
		for (int i = 0; i < Math.min(Mm,listJob.size()); i++) {
			// 验证是否从小到大----已验证正确
			// System.out.println("L_reduce_sumTime:"+i+","+listJob.get(i).L_reduce_sumTime);
			LB1_y += listJob.get(i).L_reduce_sumTime;
		}
		LB1_y += allTaskL_map_time;// 所有分子累加值
		LB1_y = LB1_y / Mm;// 除以分母
		df.format(LB1_y);
		// 验证，输出LB1_x，LB1_y
		//// System.out.println("LB1_x:" + LB1_x + ",LB1_y:" + LB1_y);

		// 计算LB1;
		LB1 = Math.max(LB1_x, LB1_y);

		// -------------------计算LB2------------------------
		double LB2_x = min_L_map_time + (allTaskL_reduce_time / Mr);// LB2=max{x,y},其中的x数
		double LB2_y = min_L_reduce_time + (allTaskL_map_time / Mm);// LB2=max{x,y},其中的y数
		// 计算LB2;
		LB2 = Math.max(LB2_x, LB2_y);

		// 验证，输出LB2_x，LB2_y
		//// System.out.println("LB2_x:" + LB2_x + ",LB2_y:" + LB2_y);

		// -------------------计算LB------------------------
		LB = Math.max(LB1, LB2);
		df.format(LB);
		// 验证LB
		//// System.out.println("LB:" + LB);
		return LB;
	}

	// LB计算需要，对修正时间进行排序 从小到大的顺序 Map阶段的总修正时间
	// 参数：listjob
	public static void jobSortAsMapLBTime(List<Job> listjob) {
		// 比较所有job的mapStageTime
		Collections.sort(listjob, new Comparator<Job>() {// 增序
			public int compare(Job job1, Job job2) {
				// return job1.L_map_sumTime.compareTo(job2.getL_map_sumTime());
				return job1.getL_map_sumTime().compareTo(job2.getL_map_sumTime());

			}
		});
		// 验证输出 ---已验证正确
		// for (Job job : listjob) {
		// Map阶段
		// System.out.println("getL_map_sumTime:" + job.getL_map_sumTime());
		// }
	}

	// LB计算需要，对修正时间进行排序 从小到大的顺序 Reduce阶段的总修正时间
	// 参数：listjob
	public static void jobSortAsReduceLBTime(List<Job> listjob) {
		// 比较所有job的mapStageTime
		Collections.sort(listjob, new Comparator<Job>() {// 增序
			public int compare(Job job1, Job job2) {
				// return
				// job1.L_reduce_sumTime.compareTo(job2.getL_reduce_sumTime());
				return job1.getL_reduce_sumTime().compareTo(job2.getL_reduce_sumTime());
			}
		});
	}

	// 方法评估标准：相对误差 RE=(Cmax-LB)/LB*100% 误差越小越好，越接近下界
	// 每种作业数{50,100,150,200,250}有30套，取误差的平均，为提高结果准确性
	public static double relativeError(double Cmax, double LB) {
		double RE = (Cmax - LB) / LB;// 100%
		return RE;
	}

 
	public static void avg30DAG(int jobNum, int nodeNum, int mapSlotNum,char EA_EF) {
		// 平均调度时间
		long sumTime = 0;
		// 每次的相对误差
		double tmpDAGRE = 0;
		// 30次的平均相对误差
		double avgDAGRE = 0;
		//double avgCmax=0;
		// 读取文件路径
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-" + jobNum + "\\" + "Job-" + jobNum
				+ "-";
		for (int i = 0; i < 30; i++) {
			// 计算算法调度时间用的startime、endtime
			long starTime = System.currentTimeMillis();
			// 读取文件 ： 参数 jobNum(文件夹名)、i 文件尾号数字
			String datafilePaht = pathCommon + i + ".txt";
			List<Job> listJobs = SourceScheduler.readFromFile(datafilePaht);
			// List<Job> listJobs = readFromFile(datafilePaht);
			// ------------------------初始化数据---------------------------------
			GraphDAG.initDAGData(listJobs);
			double maxDAG;
			if(EA_EF=='A'){
				maxDAG= GraphDAG.taskToScheduler_DAG(nodeNum, mapSlotNum,'A');
				tmpDAGRE = relativeError(maxDAG, LB_VALUE_EA[i]);//
			}else{
				maxDAG= GraphDAG.taskToScheduler_DAG(nodeNum, mapSlotNum,'F');
				tmpDAGRE = relativeError(maxDAG, LB_VALUE_EF[i]);//
			//	System.out.println("CMAX:"+maxDAG);
			}
			
			//由于DAG的下界没有按照Johnson排序取下界值，所以用前几个算法中的下界作为公用下界
		//	double LB = lowerBound(listJobs, nodeNum, mapSlotNum);
	//		System.out.println("DAG LB:"+LB);
		
			avgDAGRE += tmpDAGRE;
			// System.out.println("maxDAG:" + maxDAG);
			// System.out.println("LB_VALUE:" + LB_VALUE[i]);
			// System.out.println("RE:" + tmpDAGRE);
			long endTime = System.currentTimeMillis() - starTime;
			sumTime += endTime;
	//		avgCmax+=maxDAG;
		}
	//	System.out.println("TBS avgCax:"+avgCmax/30);
		avgDAGRE = avgDAGRE / 30;
		DecimalFormat df = new DecimalFormat("#0.0000");
		System.out.println("avgDAGRE:" + df.format(avgDAGRE));

		System.out.println("作业平均调度运行的时间：" + sumTime / 30);
		// return avgDAGRE;
	}

	public static void average30JobsRE(int jobNum, int nodeNum, int mapSlotNum, double w, char EA_EF_TBS) {
		// 存储LB_VALUE,便于后面DAG算法计算
		LB_VALUE_EA = new double[jobNum];
		LB_VALUE_EF = new double[jobNum];
		// 参数：节点数量，每个节点map slot数量，每个节点的reduce数量固定为2
		// 读取文件路径
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-" + jobNum + "\\" + "Job-" + jobNum
				+ "-";
		double averageRE = 0;
		double tmpRE = 0;
		// 平均计算时间
		long avgTime = 0;
 
		//double avgCmax=0;
		// 30次读取文件 、30次计算Cmax、 30次计算LB [针对每种作业数目的文件]----总测试
		for (int i = 0; i < 30; i++) {
			// 计算算法调度时间用的startime、endtime 调度平均时间
			long starTime = System.currentTimeMillis();
			// 读取文件 ： 参数 jobNum(文件夹名)、i 文件尾号数字
			String datafilePaht = pathCommon + i + ".txt";
			List<Job> listJobs = readFromFile(datafilePaht);

			// 持续时间计算 duration time 计算，考虑参数w={0.1，0.3，0.7，0.9}
			// 参数：作业集合，节点数，每个节点的map slot数目，持续时间评估的参数w
			durationTime(listJobs, nodeNum, mapSlotNum, w);// w的值
			// johnson 对作业排序
			listJobs = johnsonSort(listJobs);// 对作业排序！！！！！

			// slot初始化生成
			Map map = initSlots(nodeNum, mapSlotNum);
			List<MapSlot> listMapSlot = (List) map.get("listMapSlot");
			List<ReduceSlot> listReduceSlot = (List) map.get("listReduceSlot");

			double Cmax, LB;
			// ---EA、EF算法验证完成----
			// slot分配 、调度,返回Cmax
			// 参数：mapslot集合，reduceslot集合，作业集合，EA、EF、TBS方法选择
			if (EA_EF_TBS == 'A') { // 判断30遍
				// EA
				Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs, 'A');
				// -------------计算LB-----------------
				// LB调用要在Cmax结果得出之后，这样的每个任务的setupTime确定，为下界计算修正时间方便
				LB = lowerBound(listJobs, nodeNum, mapSlotNum);
				// DAG中要用
				LB_VALUE_EA[i] = LB;
			} else {// if (EA_EF_TBS == 'F')
				// EF
				Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs, 'F');
				// -------------计算LB-----------------
				// LB调用要在Cmax结果得出之后，这样的每个任务的setupTime确定，为下界计算修正时间方便
				LB = lowerBound(listJobs, nodeNum, mapSlotNum);
				// DAG中要用
				LB_VALUE_EF[i] = LB; 
		//		System.out.println("LB_VALUE_EF:"+LB_VALUE_EF[i]);
			} 
			
		//	System.out.println("LB:"+i+",value:"+LB);
			// -------------验证相对错误率-------------
			tmpRE = relativeError(Cmax, LB);
			// tmpRE = Math.floor(tmpRE * 10000) * 0.0001d;// 保留四位有效数字
			averageRE += tmpRE;
			// System.out.println("tmpRE:" + tmpRE);
			// System.out.println("------------------------");
			long endTime = System.currentTimeMillis() - starTime;
			avgTime += endTime;
		//	avgCmax+=Cmax;
		}
		//System.out.println("EAEF Cmax:"+avgCmax/30);
		averageRE = averageRE / 30;
		// 保留四位有效数字
		DecimalFormat df = new DecimalFormat("#0.0000");
		System.out.println("averageRE:" + df.format(averageRE));
		System.out.println("作业平均调度运行的时间：" + avgTime / 30);

		// 返回计算平均
		// return averageRE;
	}

	public static void average30JobsTBS_RE(int jobNum, int nodeNum, int mapSlotNum, double w) {
		// 存储LB_VALUE,便于后面DAG算法计算
		// LB_VALUE = new double[jobNum];
		// 参数：节点数量，每个节点map slot数量，每个节点的reduce数量固定为2
		// 读取文件路径
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-" + jobNum + "\\" + "Job-" + jobNum
				+ "-";
		double averageRE = 0;
		double tmpRE = 0;
		// 平均计算时间
		long avgTime = 0;
	//	double avgCmax=0;
		// 30次读取文件 、30次计算Cmax、 30次计算LB [针对每种作业数目的文件]----总测试
		for (int i = 0; i < 30; i++) {
			// 计算算法调度时间用的startime、endtime 调度平均时间
			long starTime = System.currentTimeMillis();
			// 读取文件 ： 参数 jobNum(文件夹名)、i 文件尾号数字
			String datafilePaht = pathCommon + i + ".txt";
			List<Job> listJobs = readFromFile(datafilePaht);

			// slot初始化生成
			Map map = initSlots(nodeNum, mapSlotNum);
			List<MapSlot> listMapSlot = (List) map.get("listMapSlot");
			List<ReduceSlot> listReduceSlot = (List) map.get("listReduceSlot");
			// 持续时间计算 duration time 计算，考虑参数w={0.1，0.3，0.7，0.9}
			// 参数：作业集合，节点数，每个节点的map slot数目，持续时间评估的参数w
			durationTime(listJobs, nodeNum, mapSlotNum, w);// w的值

			double Cmax = 0.0;
			// ---EA、EF算法验证完成----
			// slot分配 、调度,返回Cmax
			// 参数：mapslot集合，reduceslot集合，作业集合，EA、EF、TBS方法选择
			// TBS
			Cmax = taskToSlotScheduler_TBS(listMapSlot, listReduceSlot, listJobs);
			// // johnson 对作业排序
			// -------------计算LB-----------------
			// LB调用要在Cmax结果得出之后，这样的每个任务的setupTime确定，为下界计算修正时间方便
			// for(Job jb:listJobs){
			// System.out.println("jb:"+jb.mapStageTime);
			// }
			double LB = lowerBound(listJobs, nodeNum, mapSlotNum);
			// System.out.println("LB:"+i+",value:"+LB);
			// DAG中要用
			// LB_VALUE[i] = LB;

			// -------------验证相对错误率-------------
			tmpRE = relativeError(Cmax, LB);
			// tmpRE = Math.floor(tmpRE * 10000) * 0.0001d;// 保留四位有效数字
			averageRE += tmpRE;
			// System.out.println("tmpRE:" + tmpRE);
			// System.out.println("------------------------");
			long endTime = System.currentTimeMillis() - starTime;
			avgTime += endTime;
	//		avgCmax+=Cmax;
		}
	//	System.out.println("TBS avgCax:"+avgCmax/30);
		averageRE = averageRE / 30;
		// 保留四位有效数字
		DecimalFormat df = new DecimalFormat("#0.0000");
		System.out.println("averageRE:" + df.format(averageRE));
		System.out.println("作业平均调度运行的时间：" + avgTime / 30);

		// 返回计算平均
		// return averageRE;
	}

	public static void main(String[] args) {

		// ---------------任务数据集产生----------------------
		// 生成数据 150个实例，作业{50,100,150,200,250}
		// produceDataset();

		// ---------------调参数---------------------
		// 30个作业的最后相对误差平均值结果
		// 参数：节点数量，每个节点map slot数量，每个节点的reduce数量固定为2
		int jobNum = 250;// {50,100,150,200,250}
		int nodeNum = 10;// {10,15,20,25,30}
		int mapSlotNum =8 ;// {2,4,6,8}
		double w = 0.5;// {0.1,0.3,0.5,0.7,0.9}
		System.out.println("-----------------资源调度算法比较-----------------");
		System.out.println("作业数：" + jobNum);
		System.out.println("节点数：" + nodeNum);
		System.out.println("每个节点map slot数：" + mapSlotNum);
		System.out.println("持续时间调度参数：" + w);
		// ---------EA、EF、TBS算法结果----------------
		// 参数：作业数、结点数目、每个节点map slot数目、持续时间w 
		
		System.out.println("-----------------算法EA-----------------------");
		average30JobsRE(jobNum, nodeNum, mapSlotNum, w, 'A');
		System.out.println("-----------------算法DAG EA-----------------");
		avg30DAG(jobNum, nodeNum, mapSlotNum,'A');
		
		System.out.println("-----------------算法EF-----------------------");
		average30JobsRE(jobNum, nodeNum, mapSlotNum, w, 'F');
		System.out.println("-----------------算法DAG  EF-----------------");
		avg30DAG(jobNum, nodeNum, mapSlotNum,'F');
		System.out.println("-----------------算法TBS----------------------");
		average30JobsTBS_RE(jobNum, nodeNum, mapSlotNum, w);

		// 任务分配
		// queue();
		// 递归文件增加内容
		// String path="D:\\JaveWeb\\MapReduceScheduler\\DataSet";
		// getFiles(path);
	}
}
// 测试
// List<String> l1=new ArrayList<String>();
// l1.add("aa");
// l1.add("bb");
// l1.add("cc");
// l1.add("dd");
// List<String> l2=new ArrayList<String>();
// l2=l1;
// l2.remove("aa");
// l2.remove("cc");
// for(String str:l1){
// System.out.println(str);
// }

// 预先估计所有map、reduce任务的修正时间和
// int mapLtime=0;
// int reduceLtime=0;
// for(Job jb:listJobs){
//
// for(MapTask mt:jb.mapTask){
// mapLtime+=(mt.mapRunTime+mt.mapSize/fd);
// }
// for(ReduceTask rt:jb.reduceTask){
// reduceLtime+=(rt.reduceRunTime+rt.reduceSize/fd);
// }
// }
// System.out.println("所有作业的map任务修正时间总和： "+mapLtime/20);
// System.out.println("所有作业的reduce任务修正时间总和： "+reduceLtime/20);

// 每个作业的任务数：
// -------slot change---------size:401
// -------slot change---------size:402
// -------slot change---------size:402
// -------slot change---------size:402
// -------slot change---------size:402
// -------slot change---------size:401
// -------slot change---------size:402
// -------slot change---------size:402
// -------slot change---------size:390
// -------slot change---------size:390
// -------slot change---------size:389
// -------slot change---------size:390
// -------slot change---------size:389
// -------slot change---------size:390
// -------slot change---------size:377
// -------slot change---------size:377
// -------slot change---------size:376
// -------slot change---------size:375
// -------slot change---------size:376
// -------slot change---------size:376
//
// 作业排序：
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32
//
// 任务新排序：
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32,------------------
//

// 一次的测试------单个作业集测试
/*
 * String dataFilePath50 =
 * "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\Job-50\\Job-50-0.txt";// 先固定
 * List<Job> listJobs = readFromFile(dataFilePath50); // 计算Cmax
 * durationTime(listJobs, nodeNum, mapSlotNum, w);// w的值 listJobs =
 * johnsonSort(listJobs); // 已验证
 * 
 * // slot初始化生成 Map map = initSlots(nodeNum, mapSlotNum); List<MapSlot>
 * listMapSlot = (List) map.get("listMapSlot"); List<ReduceSlot> listReduceSlot
 * = (List) map.get("listReduceSlot"); // ---EA、EF算法验证完成---- // EA double Cmax =
 * taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs, 'A'); // EF
 * // double Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, //
 * listJobs, 'F'); // TBS // double Cmax = taskToSlotScheduler_TBS(listMapSlot,
 * listReduceSlot, // listJobs);
 * 
 * // -----验证LB--------LB调用要在Cmax结果得出之后，这样的每个任务的setupTime确定，为下界计算修正时间方便
 * System.out.println("LB"); double LB = lowerBound(listJobs, nodeNum,
 * mapSlotNum); System.out.println("Cmax:" + Cmax); // 验证相对错误率 tmpRE =
 * relativeError(Cmax, LB); tmpRE = Math.round(tmpRE * 10000) * 0.0001d;//
 * 保留四位有效数字 System.out.println("tmpRE:" + tmpRE);
 */
