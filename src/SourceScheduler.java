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
 * ��˵��
 *
 * @author:Amy
 * @version:2016��7��30������1:54:45
 */
public class SourceScheduler {

	// =====================���ñ�����ȫ�ֱ�������============================
	// --------------------------�ڵ� node ����-------------
	int[] nodeNum = { 10, 15, 20, 25, 30 };

	// --------------------------slot ����----------------
	// �õ�slot����M/R��R1=2��2��R2=4��2��R3=6��2��R4=8��2��
	// map:ÿ���ڵ�map slot��Ŀ
	int[] mapSlotNumNode = { 2, 4, 6, 8 };
	// reduce��ÿ���ڵ�reduce slot��Ŀ
	int reduceSlotNumNode = 2;

	// Mapslot �б�
	static List<MapSlot> listMapSlot;
	// Reduceslot �б�
	static List<ReduceSlot> listReduceSlot;

	static // --------------------------�����ٶ� Mb/s----------------
	int fd = 100;

	static int fr = 50;

	static int fn = 30;
	
	// --------------------------�洢EA��EF���½�ֵ----------------
	public static double[] LB_VALUE_EA;
	public static double[] LB_VALUE_EF;
	// --------------------------��ҵ ����-------------
	int[] jobNum = { 50, 100, 150, 200, 250 };

	public static void queue() {
		// 10����ҵ��2̨������ִ�е�ʱ��
		int[] aMachineTime = { 2, 7, 8, 3, 9, 5, 11, 16, 15, 10 };
		int[] bMachineTime = { 5, 9, 6, 4, 3, 7, 12, 8, 10, 14 };
		int[] pMachine = new int[10];
		int[] qMachine = new int[10];
		int p = -1;
		int q = -1;
		for (int i = 0; i < 10; i++) {
			if (aMachineTime[i] >= bMachineTime[i]) {// a �� b������ʱ
				p++;
				q++;
				pMachine[p] = bMachineTime[i];
				qMachine[q] = aMachineTime[i];
			} else {
				p++;
				q++;
				pMachine[p] = aMachineTime[i];
				qMachine[q] = bMachineTime[i];
			}
		}
		for (int i = 0; i < pMachine.length; i++) {
			System.out.print(pMachine[i] + " ");
		}
		System.out.println();

		// p ���е���
		int tmp = 0;
		for (int i = 0; i < pMachine.length; i++) {
			int middle = pMachine[i];
			// int middle =0;
			// ѡ��С����
			int k = i;
			for (int j = i + 1; j < pMachine.length; j++) {
				if (pMachine[k] > pMachine[j]) {
					k = j;
				}
			}
			tmp = pMachine[i];// ÿ��ѡ����С����
			pMachine[i] = pMachine[k];
			pMachine[k] = tmp;
		}

		for (int i = 0; i < pMachine.length; i++) {
			System.out.print(pMachine[i] + " ");
		}
		System.out.println();
		for (int i = 0; i < qMachine.length; i++) {
			System.out.print(qMachine[i] + " ");
		}
		System.out.println();
		// q ���еݼ�
		for (int i = 0; i < qMachine.length; i++) {
			for (int j = i + 1; j < qMachine.length; j++) {
				if (qMachine[i] <= qMachine[j]) {
					tmp = qMachine[i];
					qMachine[i] = qMachine[j];
					qMachine[j] = tmp;
				}
			}
		}
		for (int i = 0; i < qMachine.length; i++) {
			System.out.print(qMachine[i] + " ");
		}
	}

	// ������ʼ��:�������� ��ҵ��{50��100��150��200��250}��ÿ��30��
	public static void produceDataset() {

		// --------------------------Dout=k*Din----------------
		// Generate Data
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\";

		// ��ҵ��5�֣�����30��ʵ������150��ʵ��
		for (int i = 0; i < 30; i++) {
			// ��ҵ -50
			String dataFilePath50 = pathCommon + "Job-50\\" + "Job-50-" + i + ".txt";// �ļ���
			writeFile(dataFilePath50, generateJobs(50));
			// ��ҵ -100
			String dataFilePath100 = pathCommon + "Job-100\\" + "Job-100-" + i + ".txt";// �ļ���
			writeFile(dataFilePath100, generateJobs(100));
			// ��ҵ -150
			String dataFilePath150 = pathCommon + "Job-150\\" + "Job-150-" + i + ".txt";// �ļ���
			writeFile(dataFilePath150, generateJobs(150));
			// ��ҵ -200
			String dataFilePath200 = pathCommon + "Job-200\\" + "Job-200-" + i + ".txt";// �ļ���
			writeFile(dataFilePath200, generateJobs(200));
			// ��ҵ -250
			String dataFilePath250 = pathCommon + "Job-250\\" + "Job-250-" + i + ".txt";// �ļ���
			writeFile(dataFilePath250, generateJobs(250));
		}

	}

	// ���ݼ���Դ�� �������ݺ�д���ļ�
	// �������ļ�·���ļ���path��д����ҵ��������writerTxt
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

	// ������ҵ�ĵ�Map��reduce����
	// ������jobNum ��ҵ����ȡֵ{50,100,150,200,250} �ļ�Ҫд�������
	public static String generateJobs(int jobNum) {
		String dataStr = "";
		// ------------------Task ���� ����-----------------------
		// Map:����N(a,b)���� -��̬�ֲ�/��˹�ֲ�
		int aTask = 154, bTask = 558;
		Random random = new Random();
		// Reduce:����N(c,d)���� -��̬�ֲ�/��˹�ֲ�
		int cTask = 19, dTask = 145;

		// ------------------Process time ����-----------------------
		// Map:
		int aTime = 50, bTime = 200;
		// ����ֵΪa������Ϊb�������
		// Reduce:
		int cTime = 100, dTime = 300;
		// ����ֵΪa������Ϊb�������

		// ���ȷֲ�
		// ���� [1��2] 80%
		// double factorA = convert(random.nextDouble() * 1 + 1);
		// ���� [8��10] 20%
		// double factorB = convert(random.nextDouble() * 2 + 8);
		// System.out.println("factorA:"+factorA+",factorB:"+factorB);

		// ÿ��ʵ������ҵMap��������Reduce������Map������������k��Map�����ļ���С�Լ�ÿ������Ĵ���ʱ��
		double[] rateArr = { 0.2, 0.4, 0.6, 0.8, 1.0 };

		// Map ÿ��������ļ���С{128,192,256,320} MB
		int[] fileSizeArr = { 128, 192, 256, 320 };
		/**
		 * Jobʵ�����������ԣ�
		 * ��ҵMap��reduce��������Map������������rate{0.2,0.4,0.6,0.8}��Map�����ļ���С��ÿ��������ʱ��*
		 * factor ��ҵMap��reduce��������ִ��ʱ��
		 */
		int countJobId = 0;

		// 80%����ҵ����
		for (int i = 0; i < jobNum * 0.8; i++) {
			// ����reduce��size=Sum��mapsize��*rate/reduceNum��
			int mapFileSizeSum1 = 0;
			// ����ֵΪa������Ϊb�������
			int mapTaskNum = Math.abs((int) Math.floor(Math.sqrt(bTask) * random.nextGaussian() + aTask)) + 1;// Math.sqrt(bTask)
			int reduceTaskNum = Math.abs((int) Math.floor(Math.sqrt(dTask) * random.nextGaussian() + cTask)) + 1;// Math.sqrt(dTask)
			countJobId += 1;
			dataStr += "<job id='" + countJobId + "' maptasknum='" + mapTaskNum + "' reducetasknum='" + reduceTaskNum
					+ "'>\r\n";
			// map task
			for (int j = 0; j < mapTaskNum; j++) {
				// ÿ�������в�ͬ��sigama(rate)
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
		// 20%����ҵ����
		for (int i = 0; i < jobNum * 0.2; i++) {
			// ����reduce��size=Sum��mapsize��*rate/reduceNum��
			int mapFileSizeSum2 = 0;
			// ����ֵΪa������Ϊb�������
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

	// ���ݼ����ã�random��������������ݼ�
	public static int getRandomFromArray(int arrLength) {
		// ����randomȡֵ�����������int�ͻ�double�ͣ����Է���ȡֵ���������λ��random
		int index = (int) (Math.random() * arrLength);// 0-1
		return index;
	}

	// ����С��λ�� ���ݼ����ã���ע��
	// static double convert(double value) {
	// long l1 = Math.round(value * 100); // �������� value *100
	// double ret = l1 / 100.0; // ע�⣺ʹ�� /100.0 ������ 100
	// return ret;
	// }

	// ===========================�������ݴ���=============================
	// ��ȡ���ݣ�һ��ʵ�� 50����ҵ��100����ҵ....
	public static List<Job> readFromFile(String filepath) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		List<Job> listJobs = new ArrayList<Job>();
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			// ��ȡ�ļ�·����"D:\\JaveWeb\\MapReduceScheduler\\DataSet\\Job-50\\Job-50.txt"
			Document doc = db.parse(filepath);
			NodeList jobList = doc.getElementsByTagName("job");
			// System.out.println("job num:" + jobList.getLength() + " ��job�ڵ�");

			// ����<job>
			for (int i = 0; i < jobList.getLength(); i++) {
				Node job = jobList.item(i);
				Element elem = (Element) job;

				// һ��job���� �����֤
				// System.out.println("id:" + elem.getAttribute("id") +
				// ",maptasknum:" + elem.getAttribute("maptasknum")
				// + ",reducetasknum:" + elem.getAttribute("reducetasknum"));
				Job jobInstance = new Job();
				// Ϊ��TBS�㷨�����˹���������ҵ���ڵ�id
				jobInstance.jobId = Integer.parseInt(elem.getAttribute("id"));

				jobInstance.mapTaskNum = Integer.parseInt(elem.getAttribute("maptasknum"));
				jobInstance.reduceTaskNum = Integer.parseInt(elem.getAttribute("reducetasknum"));

				// map �׶��ܵ�ʹ��ʱ��
				// jobInstance.mapStageTime = 0;

				NodeList mapList = null;
				NodeList reduceList = null;
				List<MapTask> mapTasks = new ArrayList<MapTask>();
				List<ReduceTask> reduceTasks = new ArrayList<ReduceTask>();
				// ����<task>
				for (Node node = job.getFirstChild(); node != null; node = node.getNextSibling()) {
					mapList = elem.getElementsByTagName("maptask");
					reduceList = elem.getElementsByTagName("reducetask");
				}
				// ����mapList
				for (int j = 0; j < mapList.getLength(); j++) {
					Node mapTask = mapList.item(j);
					Element mapElem = (Element) mapTask;
					MapTask maptaskInstance = new MapTask();
					maptaskInstance.mapRunTime = Integer.parseInt(mapElem.getAttribute("runtime"));
					maptaskInstance.mapSize = Integer.parseInt(mapElem.getAttribute("size"));

					// Ϊ��TBS�㷨�����˹���������ҵ���ڵ�id,��ҵ��������1�Զ�Ĺ�ϵ��
					maptaskInstance.belongJobId = jobInstance.jobId;
					// ��ʼ��׼��ʱ��Ϊ0
					maptaskInstance.mapSetupTime = 0;
					mapTasks.add(maptaskInstance);
					// System.out.println("mapTask runtime:" +
					// mapElem.getAttribute("runtime") + ",size:"
					// + mapElem.getAttribute("size"));

				}
				// ����reduceList
				for (int k = 0; k < reduceList.getLength(); k++) {
					Node reduceTask = reduceList.item(k);
					Element mapElem = (Element) reduceTask;
					ReduceTask reducetaskInstance = new ReduceTask();
					reducetaskInstance.reduceRunTime = Integer.parseInt(mapElem.getAttribute("runtime"));
					reducetaskInstance.reduceSize = Integer.parseInt(mapElem.getAttribute("size"));
					// ��ʼ��׼��ʱ��Ϊ0
					reducetaskInstance.reduceSetupTime = 0;
					reduceTasks.add(reducetaskInstance);
					// System.out.println("reduceTask runtime:" +
					// mapElem.getAttribute("runtime") + ",size:"
					// + mapElem.getAttribute("size"));
				}
				jobInstance.mapTask = mapTasks;
				jobInstance.reduceTask = reduceTasks;
				// System.out.println("task num:" + mapList.getLength() + "
				// ��maptask�ڵ�");
				// System.out.println("reduce num:" + reduceList.getLength()
				// +"��maptask�ڵ�");
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

	// �ݹ��ļ����������ļ�
	public static void getFiles(String filePath) {
		// File root=new File(filePath);
		// File[] files=root.listFiles();
		// for(File file:files){
		// if(file.isDirectory()){
		// //�ݹ����
		// getFiles(file.getAbsolutePath());
		// System.out.println("��ʾ"+filePath+"��������Ŀ¼�����ļ���"+file.getAbsolutePath());
		// }else{
		// System.out.println("��ʾ"+filePath+"��������Ŀ¼��"+file.getAbsolutePath());
		// //׷��<jobs>......</jobs>
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
		// β����������
		// try {
		// FileWriter writer=new FileWriter(path,true);
		// writer.write(appendTail);
		// writer.close();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// System.out.println("append finish!");
		// ͷ����������
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
	}

	// ========================���ȹ���-�����㷨===========================
	// Johnson������Ҫ���� ������ʱ�䣬Ϊ������ ����ʱ���������-----------------------w
	// �Ѽ��������ÿ����ҵmap|reduce�׶εĳ���ʱ��duration�洢��ÿ����ҵ��stageTime��
	// ��������ҵ���ϣ��ڵ�����ÿ���ڵ��map slot��Ŀ������ʱ�������Ĳ���w
	public static void durationTime(List<Job> listJob, int nodeNum, int mapSlotNumNode, double w) {
		// ���õ�map|reduce slot��Ŀ
		int slotMapUse = mapSlotNumNode * nodeNum;// ���е�map slot����
		int slotReduceUse = 2 * nodeNum;// ���е�reduce slot����

		// ����map stage�׶�ʱ�䣬������Ҫ����׼��ʱ�䣡������????
		// w ����̶� ��Ti=w*T_low+(1-w)*T_up
		for (Job job : listJob) {
			// map���½綨��,����map����ʱ�䣬�ܵ�map����ʱ��
			double T_map_low = 0, T_map_up = 0, max_map_time = 0, sum_map_time = 0;
			// reduce���½綨��
			double T_reduce_low = 0, T_reduce_up = 0, max_reduce_time = 0, sum_reduce_time = 0;// reduce
																								// ���½綨��
			// map T���½���� ���ڵ��������׼��ʱ���޷�ȷ��������׼��ʱ����ñ������ݶ�ȡʱ�� fd
			for (MapTask maptaskInstance : job.mapTask) {
				// ��ʱ��������ʱ�䣺����ʱ��+׼��ʱ��
				maptaskInstance.mapSetupTime = maptaskInstance.mapSize / fd;
				double tmpMapTaskTime = (maptaskInstance.mapRunTime + maptaskInstance.mapSize / fd);
				sum_map_time += tmpMapTaskTime;// ����map����ִ����ʱ��
				if (tmpMapTaskTime > max_map_time) {// ����map�Ͻ�Ҫ�õ�����ִ��ʱ��
					max_map_time = tmpMapTaskTime;
				}
			}
			T_map_low = Math.round(sum_map_time / slotMapUse);
			T_map_up = (job.mapTaskNum - 1) * sum_map_time / (slotMapUse * job.mapTaskNum) + max_map_time;
			job.mapStageTime = (int) (w * T_map_low + (1 - w) * T_map_up);

			// redue T���½����
			for (ReduceTask reducetaskInstance : job.reduceTask) {
				reducetaskInstance.reduceSetupTime = reducetaskInstance.reduceSize / fd;
				double tmpReduceTaskTime = reducetaskInstance.reduceRunTime + reducetaskInstance.reduceSize / fd;
				sum_reduce_time += tmpReduceTaskTime;// ����reduce����ִ����ʱ��
				if (tmpReduceTaskTime > max_reduce_time) {// ����reduce�Ͻ�Ҫ�õ�����ִ��ʱ��
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

	// ��������-------��Job������ Johnson����-------------------------------
	public static List<Job> johnsonSort(List<Job> listjob) {
		// �Ƚ�����job��map�׶����ʱ��
		// ����JR1�����������ҵ��������
		// ��job��Ⱥ����������ѭ������,��Ϊ��ֵѭ����listjob���ȱ���
		int jobNum = listjob.size();

		// ����ǰ�����֤listjob----����֤��ȷ
		// for (Job jo : listjob) {
		// System.out.println("mapstageTime:" + jo.getMapStageTime() +
		// ",reducestageTime:" + jo.getReduceStageTime());
		// }

		// 1.��洢ǰ������JR1�����map��reduce��׶ε���ҵ��������listMapJob��listReduceJob
		List<Job> listMapJob = new ArrayList<Job>();
		// reduce�׶ε�job���� ����С���������map�׶εĺϲ�����Ҫ���� Collections.reverse(list);
		List<Job> listReduceJob = new ArrayList<Job>();

		// 2.ÿ���ҳ�����Job�У�map��reduce ��stagetime��С�ģ��ҵ����Ƴ�����ҵ����ʣ����ҵ�м���Ѱ��
		List<Job> tmplistjob = new ArrayList<Job>();
		for (Job jb1 : listjob) {
			tmplistjob.add(jb1);
		}
		// tmplistjob = listjob;// job���ϲ����ڱ仯�ĳ��ȣ��ʱΪ����job���ȣ�ÿѭ��һ�μ�1
		// System.out.println("job sum size:" + listjob.size());
		// ÿ���ҳ���С��jobԪ��
		for (int i = 0; i < jobNum - 1; i++) {
			// �����һ��job��mapstagetime��С
			int minJobStageTime = tmplistjob.get(0).getMapStageTime();
			Job minJob = new Job();
			minJob = tmplistjob.get(0);
			int flag = -1;// ��ע��map�׶λ���reduce�׶β�����stagetimeֵ��С��job
			for (Job jb : tmplistjob) {// һ�α��������ҵ��ֻ���ҵ�һ����С��ǰjob��Ȼ���Ƴ���job������Ѱ��
				if (jb.getMapStageTime() < minJobStageTime) {// ��һ�����Լ��ȣ��ӵȺ�Ϊ�����һ��Ԫ��
					minJobStageTime = jb.getMapStageTime(); // ��̬�仯������һֱ��С��stagetimeֵ
					minJob = jb;
					flag = 0;// ��ʾmap�׶β���
				}
				if (jb.getReduceStageTime() < minJobStageTime) {
					minJobStageTime = jb.getReduceStageTime();
					minJob = jb;
					flag = 1;// ��ʾreduce�׶β���
				}
			}
			if (flag == 0) {// -1��ʾ��ǰ��С��ֻ�����һ��
				listMapJob.add(minJob);
			} else {
				listReduceJob.add(minJob);
			}
			tmplistjob.remove(minJob);
			// ��֤---����֤��ȷ �����ϱ�С��1
			// System.out.println("===�仯�ĵ�tmplistjob size:" +
			// tmplistjob.size());
		}
		// �����һ��Ԫ��,����map��reduce�����Ƿ��м䣬���Ը���map��
		listMapJob.add(tmplistjob.get(0));
		//// System.out.println("===JR1 map size:" + listMapJob.size());

		//// System.out.println("===JR1 reduce size:" + listReduceJob.size());

		// ��reduce stage time����õ�����ҵ����
		Collections.reverse(listReduceJob);

		// ��map��reduce stage time����õ�����ҵ����ϲ�,�����¸�ֵ��listjob
		listMapJob.addAll(listReduceJob);
		listjob = listMapJob;// ���¸�ֵ���ָ�ԭ����С
		// ��֤JR1����ҵ����
		// System.out.println("===JR1 last tmplistjob
		// size:"+tmplistjob.size());//Ӧ��Ϊ1
		// System.out.println("===JR1 sum size:"+listjob.size());
		// System.out.println("job size:"+listjob.size());
		// job��JR1����������֤
		// System.out.println("-------------------------");
		// for (Job jo : listjob) {
		// System.out.println("--mapstageTime:" + jo.getMapStageTime() +
		// ",reducestageTime:" + jo.getReduceStageTime());
		// }
		return listjob;
	}

	// ÿ����ҵ�������Ե��ö������maptask�������� ���� LPTԭ���Ƿ���Ҫ��׼��ʱ��(���׼��ʱ��������ʱ���ٶȱ����ٶ�fd��)
	public static void sortMapTaskLPT(List<MapTask> maptaskList) {
		Collections.sort(maptaskList, new Comparator<MapTask>() {// ����
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
		Collections.reverse(maptaskList);// reverse ����
		// �����֤
		// for (MapTask mt : maptaskList) {
		// System.out.println("all maptask
		// LPT:"+(mt.getMapRunTime()+mt.getMapSetupTime()));
		// }
		// return maptaskList;
	}

	// ÿ����ҵ�������Ե��ö������maptask�������� ���� LPTԭ��
	public static void sortReduceTaskLPT(List<ReduceTask> reduceTaskList) {
		Collections.sort(reduceTaskList, new Comparator<ReduceTask>() {// ��������reverse
																		// ����
			public int compare(ReduceTask reducetask1, ReduceTask reducetask2) {
				Integer rt1 = reducetask1.reduceRunTime + reducetask1.reduceSetupTime;
				Integer rt2 = reducetask2.reduceRunTime + reducetask2.reduceSetupTime;
				return rt1.compareTo(rt2);
				// return
				// reducetask1.getReduceRunTime().compareTo(reducetask2.getReduceRunTime());
			}
		});
		Collections.reverse(reduceTaskList);// ����
		// �����֤
		// for (ReduceTask mt : reduceTaskList) {
		// System.out.println("reduce LPT:"+mt.reduceRunTime);
		// }
		// return reduceTaskList;
	}

	// ��������

	// �ܵĵ��Ȳ������룬�����������
	/*
	 * w={0.1,0.3,0.5,0.7,0.9} ��Ӱ��johnson���� nodeNum={10,15,20,25,30} ���л��̶�
	 * mapSlotNum={2,4,6,8},reduceSlotNum=2 ��ҵ��Ҳ�ǲ���={50,100,150,200,250}
	 * ȡ���ڴ�����ļ�
	 */
	// ��ʼ������slot��map��reduce ��ǰʹ��ʱ�䡢�ٶ�
	// �������ڵ�������ÿ���ڵ�map slot������ÿ���ڵ��reduce�����̶�Ϊ2
	public static Map initSlots(int nodeNum, int mapSlotNum) {
		// �洢listMapSlot��listReduceSlot
		Map map = new HashMap();
		// nodeNum*mapSlotNum �ܵ�map slot�� �ȼ���ÿ���ڵ�mapSlot����Ϊ2
		// ���������ٶ� fr(30)��fn(50)��ռ0.3��0.3��fd(100)ռ0.4 �ٶ���ȫ�ֱ���
		// int nodeNum = 10, mapSlotNum = 2;
		listMapSlot = new ArrayList<MapSlot>();
		int sumMapSlot = nodeNum * mapSlotNum;// ÿ���ڵ��slot����ΪmapSlotNum

		// ͬ�ڵ� 100
		for (int i = 0; i < sumMapSlot * 0.4; i++) {
			MapSlot ms100 = new MapSlot();
			ms100.currentMapSlotTime = 0;
			ms100.mapSlotSpeed = fd;
			listMapSlot.add(ms100);
		}
		// ͬ���ܣ���ͬ�ڵ� 50
		for (int i = 0; i < sumMapSlot * 0.3; i++) {
			MapSlot ms50 = new MapSlot();
			ms50.currentMapSlotTime = 0;
			ms50.mapSlotSpeed = fr;
			listMapSlot.add(ms50);
		}
		// ��ͬ���ܣ�30
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
		// ��֤slot ��ֵ��ʼ�����------����֤
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

	// Reduceǰ��Ҫ��������sigma m�ķǽ����������� sigma-��stagetime
	public static List<Job> jobSortAsMapStageTime(List<Job> listjob) {
		// �Ƚ�����job��mapStageTime
		Collections.sort(listjob, new Comparator<Job>() {// ����
			public int compare(Job job1, Job job2) {
				return job1.getMapStageTime().compareTo(job2.getMapStageTime());
			}
		});
		// // ��֤���-------��ȷ
		// for (Job job : listjob) {
		// System.out.println("MapStageTime:" + job.getMapStageTime());
		// }
		return listjob;
	}

	// =========================�����㷨EASS��EFSS����ʽ�㷨==========================
	// �����slot���䣬����EASS��EFSS�㷨
	// ������mapslot���ϣ�reduceslot���ϣ���ҵ���ϣ�EA_EF����ѡ��
	// ����Cmax
	public static double taskToSlotScheduler_EA_EF(List<MapSlot> listMapSlot, List<ReduceSlot> listReduceSlot,
			List<Job> listJob, char m_r) {
		// Լ��һ����ҵ������map������ɺ��ٿ�ʼreduce����
		for (Job job : listJob) {
			// ʹ��LPT�����ÿ����ҵ��map��reduce������������ ,ִ��ʱ�����׼��ʱ�����,�ʱ�����ȣ�����
			sortMapTaskLPT(job.mapTask);
			sortReduceTaskLPT(job.reduceTask);

			// ��֤LPT����Ƿ���ȷ �Ӵ�С���� -----��ȷ
			// ��ֵsigma-��stagetime=0;
			job.mapStageTime = 0;
			// ��ÿ��map������
			for (MapTask maptask : job.mapTask) {
				// ��ǰ�����±�
				int indexMapTask = job.mapTask.indexOf(maptask);// ��ǰmap�����±�
				int indexMapSlot;
				if (m_r == 'A') {// EA
					// EA���������map slot
					// ������mapslot���ϣ�reduceslot���ϣ�map|reduce�׶ε�ѡ��
					// indexMapSlot = minSlotTime_EA_slot_index(listMapSlot,
					// null, 'm');// �������slot
					indexMapSlot = minMapSlotTime_EA_slot_index(listMapSlot);
					// System.out.println(" min indexMapSlot:" + indexMapSlot +
					// ",current time:"
					// + listMapSlot.get(indexMapSlot).hashCode());

				} else {// EF
						// EF:����������� map slot
					// ������mapslot���ϣ�reduceslot���ϣ���������ʱ�䣬map|reduce�׶ε�ѡ��
					indexMapSlot = minMapSlotTime_EF_slot_index(listMapSlot, maptask);
				}
				// ������������ҵ�������±꣬�����slot��ţ�map/reduce����ѡ��
				TAP(job, indexMapTask, indexMapSlot, 'm');// job,task_j,slot_k,m_r
				// ׼��ʱ���ǣ������ļ���С/��slot�ٶ�
				// int setupTime1 = (int) Math.floor((maptask.mapSize) /
				// (listMapSlot.get(indexMapSlot).mapSlotSpeed));

				// map �׶ε�sigma m ��Ϊ��Reduceǰ������
				// job.mapStageTime += (setupTime1 + maptask.mapRunTime);

				// ����������񣬸�slot�ĵ�ǰʱ��Ҫ�Ӹղ����е������ʱ��
				// listMapSlot.get(indexMapSlot).currentMapSlotTime +=
				// maptask.mapRunTime + setupTime1;

			}

			// System.out.println("-------------------------");// ����ҵ
		}
		// Reduce�׶Σ������е���ҵ����map sigma m�ķǽ���(����)��ϵ��-----stagetime
		listJob = jobSortAsMapStageTime(listJob);

		for (Job job : listJob) {
			// ����reduce slot��������map slot��ͬ������slot�ϵ�stageTime��ֵ��reduce slot
			job.reduceStageTime = job.mapStageTime;
			// ÿ����ҵ��map�׶����ʱ�䴫��reduce���ڴ˻����ϼ���ִ��reduce����
			// System.out.println("reduceStageTime init:"+job.reduceStageTime);

			// ��reduce������д���
			for (ReduceTask reduceTask : job.reduceTask) {
				// ��ǰreduce�����±�
				int indexReduceTask = job.reduceTask.indexOf(reduceTask);
				int indexReduceSlot;
				if (m_r == 'A') {// EA
					// EA���������reduce slot
					// indexReduceSlot = minSlotTime_EA_slot_index(null,
					// listReduceSlot, 'r');
					indexReduceSlot = minReduceSlotTime_EA_slot_index(listReduceSlot);
				} else {// EF
					// EF:����������� reduce slot
					// ����ִ�е����������ʱ�䣺����ʱ��+׼��ʱ��
					// int tmp_L_task = reduceTask.reduceRunTime +
					// reduceTask.reduceSetupTime;
					// ������mapslot���ϣ�reduceslot���ϣ���������ʱ�䣬map|reduce�׶ε�ѡ��
					indexReduceSlot = minReduceSlotTime_EF_slot_index(listReduceSlot, reduceTask);
				}
				// ������������ҵ�������±꣬�����slot��ţ�map/reduce����ѡ��
				TAP(job, indexReduceTask, indexReduceSlot, 'r');

			}
			// for (ReduceSlot rs : listReduceSlot) {
			// // rs.currentReduceSlotTime=0;
			// System.out.println("reduce slot currentime:" +
			// rs.currentReduceSlotTime);
			// }
			// System.out.println("-------------------------");// ����ҵ

		}
		// ��������reduce�׶εĴ���ʱ��
		double Cmax = maxReduceStageTime(listJob);// listReduceSlot.get(maxReduceSlotValue(listReduceSlot)).currentReduceSlotTime;
		// System.out.println("Cmax:" + Cmax);
		// ��������reduce slot ʱ�� ����һ���㷨�õ���ֵһ��?reduce slot��ʼִ��ʱ�䲻Ϊ0
		// double Cmax1 = maxReduceSlotTime(listReduceSlot);
		// System.out.println("Cmax1:" + Cmax1);
		return Cmax;
	}

	// ����������TAP
	// ������������ҵ�������±꣬�����slot��ţ�map/reduce����ѡ��
	// ���±겻����������ԭ���ǣ���Ϊ����������Ϊmaptask������Ϊreducetask�����±�����ڸ���ҵ�и���m_rѡ�񣬻�ȡ��Ӧ�����������
	public static void TAP(Job job, int j, int k, char m_r) {
		// ����ʱ��
		// double currentSlotKTime;// ��k ���slot
		double completeJob_i_j_Time;// ��ǰ��ҵ����J(i��j)�����ʱ��C��
		// double m_r_stageTime;// ��ǰ����������ҵ�׶����ʱ�䣬�����ҵ��map/reduce�׶�
		if (m_r == 'm') {
			// 1.ִ��ʱ��+׼��ʱ��
			// ׼��ʱ���ǣ������ļ���С/��slot�ٶ� ���¸ı�setup time ʱ��
			// map�����е�setuptime���Բ��ö��壬ֱ������ʱ����ȡ��
			job.mapTask.get(j).mapSetupTime = (int) Math
					.round((job.mapTask.get(j).mapSize) / (listMapSlot.get(k).mapSlotSpeed));

			// 2.��ǰslotʱ��
			// currentSlotKTime =
			// listMapSlot.get(k).currentMapSlotTime+(maptask.mapSetupTime +
			// runTime);// ���slot

			// 3.��ǰ������ҵ������׶Σ�m/r��ʱ��
			// map �׶ε�sigma m ��Ϊ��Reduceǰ������ �Ƿ������,�Ӹ��½������һЩ
			job.mapStageTime += (job.mapTask.get(j).mapSetupTime + job.mapTask.get(j).mapRunTime);//
			// mapĳ�����������Ҫ���ϸ������ʱ��---����
			// m_r_stageTime = job.mapStageTime; //
			// ��ǰ����������ҵ�׶����ʱ�䣬�����ҵ��map/reduce�׶�

			// 4.��ҵ���ʱ�� ���slot
			completeJob_i_j_Time = listMapSlot.get(k).currentMapSlotTime
					+ (job.mapTask.get(j).mapSetupTime + job.mapTask.get(j).mapRunTime);// ���slot

			if (completeJob_i_j_Time > job.mapStageTime) {
				job.mapStageTime = (int) completeJob_i_j_Time;
			}
			// 5.�����µĵ�ǰslotʱ��
			// ����������񣬸�slot�ĵ�ǰʱ��Ҫ�Ӹղ����е������ʱ��---����
			listMapSlot.get(k).currentMapSlotTime = (int) completeJob_i_j_Time;

			// TBS�㷨����Ҫ��¼��ӵ�mapTask��˳��,ÿ��Slot��Ҫ��¼maptask���� ��ֻ��map�׶Σ�û��reduce�׶�
			listMapSlot.get(k).maptaskSlotList.add(job.mapTask.get(j));

		} else {// reduce
			// 1.ִ��ʱ��+׼��ʱ��
			// ReduceTask reducetask = job.reduceTask.get(j);////
			// ͨ���±�ȡ������reduce����
			job.reduceTask.get(j).reduceSetupTime = (int) Math // ׼��ʱ��
					.round((job.reduceTask.get(j).reduceSize) / (listReduceSlot.get(k).reduceSlotSpeed));
					// 2.��ǰslotʱ��
					// currentSlotKTime =
					// listReduceSlot.get(k).currentReduceSlotTime
					// +(reducetask.reduceSetupTime + runTime);// ��k
					// ���slot
					// reduce�׶θ�������ɵ�ʱ�����

			// 4.��ҵ���ʱ�� ���slot
			completeJob_i_j_Time = Math.max(listReduceSlot.get(k).currentReduceSlotTime, job.mapStageTime)// job.mapStageTime
																											// job.reduceStageTime
					+ job.reduceTask.get(j).reduceSetupTime + job.reduceTask.get(j).reduceRunTime;

			// 3.��ǰ������ҵ������׶Σ�m/r��ʱ��
			// m_r_stageTime = job.mapStageTime; //
			// ��ǰ����������ҵ�׶����ʱ�䣬�����ҵ��map/reduce�׶�

			job.reduceStageTime += (job.reduceTask.get(j).reduceSetupTime + job.reduceTask.get(j).reduceRunTime);//
			if (completeJob_i_j_Time > job.reduceStageTime) {
				job.reduceStageTime = (int) completeJob_i_j_Time;
			}
			// 5.�����µĵ�ǰslotʱ��
			// ����������񣬸�slot�ĵ�ǰʱ��Ҫ�Ӹղ����е������ʱ��---����
			listReduceSlot.get(k).currentReduceSlotTime = (int) completeJob_i_j_Time;// +=
																						// (job.reduceTask.get(j).reduceSetupTime
																						// +
																						// job.reduceTask.get(j).reduceRunTime);
		}
		return;
	}

	// Cmax����
	// �õ�������ҵ��reduce�������ʱ��ֵ reduceStageTime
	public static double maxReduceStageTime(List<Job> listJob) {
		double Cmax = 0.0;
		for (Job job : listJob) {
			// ��֤�������reduce stage time
			// System.out.println("result reduce stage time:" +
			// job.reduceStageTime);
			if (job.reduceStageTime > Cmax) {
				Cmax = job.reduceStageTime;// ��̬�仯ȡ�������ֵ
			}
		}
		// System.out.println("listjob last reduce time:" +
		// listjob.get(49).getReduceStageTime());
		return Cmax;
	}

	// Cmax����
	// ��������Reduce slot��ֵ
	public static double maxReduceSlotTime(List<ReduceSlot> listReduceSlot) {
		double Cmax = 0.0;
		for (ReduceSlot rs : listReduceSlot) {
			if (rs.currentReduceSlotTime > Cmax) {
				Cmax = rs.currentReduceSlotTime;
			}
		}
		return Cmax;
	}

	// EA�㷨��������map|reduce slot��ѡ��ǰʱ����Сֵ
	// ����������map slot,����reduce slot,map|reduce�׶�ѡ��,û�в���Ϊnull
	// ���ض�Ӧ��map|reduce slot��List���±�
	public static Integer minMapSlotTime_EA_slot_index(List<MapSlot> listMapSlot) {
		// Ҫ���� ���±�ֵ
		Integer minIndex = 0;
		// ������Ϊ�˷����±�ֵ
		int count = -1;
		// �����һ��map slot�ĵ�ǰʱ����С
		Integer minMapSlotTime = listMapSlot.get(0).currentMapSlotTime;
		for (MapSlot ms : listMapSlot) {
			count++;
			// ����EA
			if (ms.currentMapSlotTime < minMapSlotTime) {// ����ȵ�һ��С����ȡ��
				minMapSlotTime = ms.currentMapSlotTime;// Ҫ��̬������Сֵ��Ϊ����һ�ֵıȽ�
				minIndex = count;
			}
		}
		return minIndex;
	}

	public static Integer minReduceSlotTime_EA_slot_index(List<ReduceSlot> listReduceSlot) {
		// Ҫ���� ���±�ֵ
		Integer minIndex = 0;
		// ������Ϊ�˷����±�ֵ
		int count = -1;
		// �����һ��map slot�ĵ�ǰʱ����С
		// �����һ��reduce slot�ĵ�ǰʱ����С
		Integer minReduceSlotTime = listReduceSlot.get(0).currentReduceSlotTime;
		for (ReduceSlot rs : listReduceSlot) {
			count++;
			// ��֤slotֵ��reduce slot���� slotֻ��reduce�׶�ǰreduce slot�ĸ���������Ч
			// System.out.println("reduce slot current
			// time:"+rs.currentReduceSlotTime);
			if (rs.currentReduceSlotTime < minReduceSlotTime) {
				minReduceSlotTime = rs.currentReduceSlotTime;// Ҫ��̬������Сֵ��Ϊ����һ�ֵıȽ�
				minIndex = count;
			}
		}
		return minIndex;
	}

	// public static Integer minMapSlotTime_EA_slot_index(List<MapSlot>
	// listMapSlot, List<ReduceSlot> listReduceSlot,
	// char m_r) {
	// // Ҫ���� ���±�ֵ
	// Integer minIndex = 0;
	// // ������Ϊ�˷����±�ֵ
	// int count = -1;
	// if (m_r == 'm') {// map �׶�
	// // �����һ��map slot�ĵ�ǰʱ����С
	// Integer minMapSlotTime = listMapSlot.get(0).currentMapSlotTime;
	// for (MapSlot ms : listMapSlot) {
	// count++;
	// // ����EA
	// if (ms.currentMapSlotTime < minMapSlotTime) {// ����ȵ�һ��С����ȡ��
	// minMapSlotTime = ms.currentMapSlotTime;// Ҫ��̬������Сֵ��Ϊ����һ�ֵıȽ�
	// minIndex = count;
	// }
	// }
	// } else {// reduce �׶�
	// // �����һ��reduce slot�ĵ�ǰʱ����С
	// Integer minReduceSlotTime = listReduceSlot.get(0).currentReduceSlotTime;
	// for (ReduceSlot rs : listReduceSlot) {
	// count++;
	// // ��֤slotֵ��reduce slot���� slotֻ��reduce�׶�ǰreduce slot�ĸ���������Ч
	// // System.out.println("reduce slot current
	// // time:"+rs.currentReduceSlotTime);
	// if (rs.currentReduceSlotTime < minReduceSlotTime) {
	// minReduceSlotTime = rs.currentReduceSlotTime;// Ҫ��̬������Сֵ��Ϊ����һ�ֵıȽ�
	// minIndex = count;
	// }
	// }
	// }
	//
	// return minIndex;
	// }

	// EF�㷨��������map slot��ѡ�������ʱ��
	// ������mapslot���ϣ���ǰ���������
	// ���ض�Ӧ��map slot��List���±�
	public static Integer minMapSlotTime_EF_slot_index(List<MapSlot> listMapSlot, MapTask maptask) {
		// ������Сֵ���±�
		Integer minIndex = 0;
		int count = -1;
		// ���������ʱ�䣺�����ٶ�+׼��ʱ��(�����С/�ٶ�)
		double L_time = 0;
		// �����һ��slot���ϵ�ǰ���������ʱ���ֵ��С
		// ����ִ�е����������ʱ�䣺����ʱ��+�ļ���С/��������
		// ��ͬslot�Ĵ����ٶȲ�ͬ�����������׼��ʱ�䲻ͬ
		MapSlot ms0 = listMapSlot.get(0);
		double minMapSlotTime = ms0.currentMapSlotTime + (maptask.mapRunTime + (maptask.mapSize / ms0.mapSlotSpeed));

		for (MapSlot ms : listMapSlot) {
			count++;
			L_time = maptask.mapRunTime + (maptask.mapSize / ms.mapSlotSpeed);
			// ����EF:��ǰ�����õ���
			if ((ms.currentMapSlotTime + L_time) < minMapSlotTime) {// ����ȵ�һ��С�������¸���Сֵ
				minMapSlotTime = ms.currentMapSlotTime + L_time;// ��Сֵ�Ƚ϶�̬�仯
				minIndex = count;
			}
		}
		return minIndex;
	}

	// EF�㷨��������reduce slot��ѡ�������ʱ��
	// ������reduceslot���ϣ���ǰ���������
	// ���ض�Ӧ��reduce slot��List���±�
	public static Integer minReduceSlotTime_EF_slot_index(List<ReduceSlot> listReduceSlot, ReduceTask reducetask) {
		// ������Сֵ���±�
		Integer minIndex = 0;
		int count = -1;
		// ���������ʱ�䣺�����ٶ�+׼��ʱ��(�����С/�ٶ�)
		double L_time = 0;
		// �����һ��reduce slot���ϵ�ǰ���������ʱ���ֵ��С
		ReduceSlot rs0 = listReduceSlot.get(0);
		double minReduceSlotTime = rs0.currentReduceSlotTime
				+ (reducetask.reduceRunTime + (reducetask.reduceSize / rs0.reduceSlotSpeed));

		for (ReduceSlot rs : listReduceSlot) {
			count++;
			L_time = reducetask.reduceRunTime + (reducetask.reduceSize / rs.reduceSlotSpeed);
			if ((rs.currentReduceSlotTime + L_time) < minReduceSlotTime) {// ����ȵ�һ��С�������¸���Сֵ
				minReduceSlotTime = rs.currentReduceSlotTime + L_time;// ��Сֵ�Ƚ϶�̬�仯
				minIndex = count;
			}
		}
		return minIndex;

	}

	// ------�õ�reduce ����slot�ϵ�ʱ��ֵ-------������Ҫ
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

	// ========================�����㷨TBS����ʽ�㷨=========================
	// �����slot���䣬����EASS��EFSS�㷨
	// ������mapslot���ϣ�reduceslot���ϣ���ҵ����
	// ����Cmax
	public static double taskToSlotScheduler_TBS(List<MapSlot> listMapSlot, List<ReduceSlot> listReduceSlot,
			List<Job> listJob) {
		// 1.��ʼ������slot initSlots �ú�����
		// 2.��ʼ��������ҵ����ȡ���ݳ�listjob readFromFile(path) �ú�����
		// �����㷨����ʱ���õ�startime��endtime
		long starTime = System.currentTimeMillis();
		// ����ֵ�Ķ���
		double Cmax = 0;
		// 3.��LPT(�����ʱ������)��������ҵ��map����������� ����μ�סÿ���������ڵ�job //������1ǰ����˳��Ӱ��
		// Ϊ�����������ھ����ĸ���ҵ��ȷ������һ���ֶ� Integer belongId
		// ����һ��List<MapTask>���������ҵ�ĵ�����map ����
		List<MapTask> maptaskAll = new ArrayList<MapTask>();

		for (Job jb : listJob) {
			// ����������ҵ��map������ӵ��µ�list�У�Ϊ��������
			for (MapTask mt : jb.mapTask) {
				maptaskAll.add(mt);
			}
		}
		//// System.out.println("maptaskAll size:" + maptaskAll.size());
		// ʹ��LPT�������� maptask runtime���� �Ƿ���Ҫ��׼��ʱ�䣡����������������������
		sortMapTaskLPT(maptaskAll);

		// for (MapTask mt : maptaskAll) {
		// ��֤---����֤�������map �����Ӧ��job��id
		// System.out.println("maptaskAll elem jobid:"+mt.getBelongJobId());
		// ��֤--����֤LPT������ȷ
		// System.out.println("maptaskAll elem runtime:"+mt.mapRunTime);//
		// }

		// �����е�������� arg min{��k+s+p} ��EFȡ��С��slot
		int indexMapSlot;
		int indexMapTask;// ����ҵ��λ��
		Integer jobId;
		// ͨ�������jobid�õ���Ӧ��job����
		Job jobInstance = new Job();
		for (MapTask mta : maptaskAll) {
			// 4.����������������ʱ���slot���� ����k+s+p ��ǰslotʱ��+׼��ʱ��+ִ��ʱ��
			// �õ�������õ�Map slot�±�
			indexMapSlot = minMapSlotTime_EF_slot_index(listMapSlot, mta);
			// ͨ������õ���ҵid���Ӷ��õ���Ӧ����ҵ
			jobId = mta.belongJobId;
			jobInstance = findJobByJobId(listJob, jobId);
			// ����ҵ��λ��
			indexMapTask = jobInstance.mapTask.indexOf(mta);
			// ��map�׶ε���TAP TAP(job,int indexTask,int slotk,m);
			TAP(jobInstance, indexMapTask, indexMapSlot, 'm');
		}

		// johnson ����ҵ����
		listJob = johnsonSort(listJob);// ����ҵ����
		// // ---------------����johnson����ҵ����-------------
		// // ��¼JR1�����µ�jobId������,johnson �㷨 �� ���������,����JR1�����Ŷ�Ӧ��job id

		// ---------------����johnson����ҵ����-------------
		// ��¼JR1�����µ�jobId������,johnson �㷨 �� ���������,����JR1�����Ŷ�Ӧ��job id
		List<Integer> jobIdJR1Sort = new ArrayList<Integer>();
		for (Job jb : listJob) {
			jobIdJR1Sort.add(jb.jobId);// JR1�����µ�jobId������
			// System.out.println("jobId:"+jb.jobId);
		}
		// 5.����johnson����ҵ�����ƶ����� jobIdJR1Sort ��ҵ��JR1����List
		// TAPÿ��slot����map task���ȵ�����
		for (MapSlot ms : listMapSlot) {
			// ��ÿ��map slot�ϵ����񵥶�����
			// һ��map slot�ϵĴ��� keyΪjobid,valueΪList<MapTask>
			Map<Integer, List<MapTask>> map_jobid_maptask = new HashMap<Integer, List<MapTask>>();
			for (MapTask mt : ms.maptaskSlotList) {// ����slot�ϵ�maptask����
				Integer jobid = mt.getBelongJobId();
				// System.out.println("jobid:" + jobid);
				// ͨ��key�õ�listmaptask
				List<MapTask> lmt = new ArrayList<MapTask>();
				if (map_jobid_maptask.get(jobid) == null) {
					lmt.add(mt);// Ϊnull��ֱ�����
				} else {
					lmt = map_jobid_maptask.get(jobid);// ������ԭ�Ȼ��������
					lmt.add(mt);
				}
				map_jobid_maptask.put(jobid, lmt);
			}
			// ��ÿ��map slot�ϵ�key�������򣬼��Զ�Ӧ��key-value����������johnsonһ������ҵ����
			// �½�һ��List<MapTask> �����ڴ洢��map slot������ֵ
			List<MapTask> newSlotMapTasks = new ArrayList<MapTask>();
			for (Integer jobElem : jobIdJR1Sort) {
				// ���������keyֵ����keyֵ��������
				for (Integer taskKeyId : map_jobid_maptask.keySet()) {
					if (jobElem == taskKeyId) {// ���id��ҵ���,����Ӷ�Ӧ��value,�ж��maptask��ֵ
						newSlotMapTasks.addAll(map_jobid_maptask.get(taskKeyId));
						// System.out.print(taskKeyId+",");
					}
				}

			}
			// ��������õ�map slot�ϵ�maptask����ֵ��ԭ�ȵ�listMapTask
			ms.maptaskSlotList = newSlotMapTasks;
			// System.out.println("-------slot
			// change---------size:"+ms.maptaskSlotList.size());
		}

		// 7������ÿ����������ʱ��(currentSlotTime)��������ҵ��Map�׶����ʱ��(stagetime) listJob
		// ͨ��map slot�ϵ�������������ȷ��ÿ����ҵ��stagetime��ÿ��slot�ĵ�ǰʱ���
		// ��ʼ��listjob�������ݣ�ͨ��map slot�Ϸ���õ�����ȷ������ִ��ʱ����䣬stagetime

		for (Job jbElem : listJob) {
			jbElem.mapStageTime = 0;// ���µ�����׼��
		}

		for (MapSlot ms_jb : listMapSlot) {
			// ��ʼ������MapSlot�ĵ�ǰʱ��Ϊ0�����ݾ������������ۼ�
			ms_jb.currentMapSlotTime = 0;
			// ���α���ÿ��map slot����������
			for (MapTask mt_jb : ms_jb.maptaskSlotList) {// slot�ϵ�map��������
				// ��֤map��setupʱ���Ƿ����(�Ѹ�ֵ)��setuptime-----����֤����
				// System.out.println("task setup time:"+mt_jb.mapSetupTime);
				// --ÿִ��һ��map����job������ִ�������stagetime����--
				// ���������ҵ���Ӧjob
				Integer job_id = mt_jb.belongJobId;
				Job jobCase = findJobByJobId(listJob, job_id);
				// jobCase.mapStageTime += (mt_jb.mapRunTime +
				// mt_jb.mapSetupTime);
				// ȷ����Ӧ��ҵ��stageTime�ı�
				Integer jobIndex = listJob.indexOf(jobCase);
				listJob.get(jobIndex).mapStageTime += (mt_jb.mapRunTime + mt_jb.mapSetupTime);
				// System.out.println("listJob.size:"+listJob.size());

				// --ÿִ��һ��map����slot ��ǰʱ�����--
				ms_jb.currentMapSlotTime += (mt_jb.mapRunTime + mt_jb.mapSetupTime);

				// ��ҵ�ĵ�ǰ���ʱ��stagetimeҪ���ǵ���slot�ϵ�ʱ�䣬ȡ���ʱ��Ϊ׼
				if (listJob.get(jobIndex).mapStageTime < ms_jb.currentMapSlotTime) {
					listJob.get(jobIndex).mapStageTime = ms_jb.currentMapSlotTime;
				}

			}
		}
		// System.out.println("listMapSlot.size:"+listMapSlot.size() );//= 0;//
		// ���µ�����׼��
		// System.out.println("------alltasksize:"+alltasksize);
		// ��֤map��stagetime ---����֤��ȷ
		// for(Job jbElem:listJob){
		// System.out.println("new job stage time:"+jbElem.mapStageTime);
		// }
		// -------------Reduce�׶�-----------
		// 8. ����ҵ��סsigma m(stagetime)�ǽ���(����)���� Reduce�׶�
		listJob = jobSortAsMapStageTime(listJob);
		// ������EFSS��������
		for (Job job : listJob) {
			// 9.��ÿ����ҵ��reduce������LPT����
			sortReduceTaskLPT(job.reduceTask);
			// ����reduce slot��������map slot��ͬ������slot�ϵ�stageTime��ֵ��reduce slot
			job.reduceStageTime = job.mapStageTime;
			// ��reduce������д���
			for (ReduceTask reduceTask : job.reduceTask) {
				// ��ǰreduce�����±�
				int indexReduceTask = job.reduceTask.indexOf(reduceTask);
				int indexReduceSlot;
				// EF:����������� reduce slot
				// ����ִ�е����������ʱ�䣺����ʱ��+׼��ʱ��
				// int tmp_L_task = reduceTask.reduceRunTime +
				// reduceTask.reduceSetupTime;
				// ������mapslot���ϣ�reduceslot���ϣ���������ʱ�䣬map|reduce�׶ε�ѡ��
				indexReduceSlot = minReduceSlotTime_EF_slot_index(listReduceSlot, reduceTask);
				// ������������ҵ�������±꣬�����slot��ţ�map/reduce����ѡ��
				TAP(job, indexReduceTask, indexReduceSlot, 'r');
			}
		}
		// ��������reduce slot ʱ�� ����һ���㷨�õ���ֵһ��
		// Cmax = maxReduceSlotTime(listReduceSlot);
		// �õ���ĵ���ʱ��
		// System.out.println("listjob size===:"+listJob.size());
		Cmax = maxReduceStageTime(listJob);
		// System.out.println("job size ===:"+listJob.size());
		return Cmax;
	}

	// ͨ��jobId����job����
	public static Job findJobByJobId(List<Job> listJob, Integer jobid) {
		Job job = new Job();
		for (Job jb : listJob) {
			if (jb.jobId == jobid) {
				job = jb;
			}
		}
		return job;
	}

	// =========================���۵���ָ��=============================
	// ���۵��ȵ��½磺 LB={max{LB1,LB2}};
	public static double lowerBound(List<Job> listJob, int nodeNum, int mapSlotNum) {
		// for(Job jb:listJob){
		// for(MapTask mt:jb.mapTask){
		// System.out.println("map setup time:"+mt.mapSetupTime);
		// }
		// for(ReduceTask rt:jb.reduceTask){
		// System.out.println("reduce setup time:"+rt.reduceSetupTime);
		// }
		// }
		// Mr��ʾReduce slot������
		// Mm��ʾMap Slot����
		int Mm = nodeNum * mapSlotNum;
		int Mr = nodeNum * 2;// ��Ϊÿ���ڵ�̶�����reduce slot��ĿΪ2
		// System.out.println("listJob size:"+listJob.size());
		// ��֤׼��ʱ���Ƿ��ǵ���ʱ����ʱ��
		// for(Job jb:listJob){
		// for(MapTask mt:jb.mapTask){
		// System.out.println("map setup time:"+mt.getMapSetupTime());
		// }
		// for(ReduceTask rt:jb.reduceTask){
		// System.out.println("reduce setup time:"+rt.getReduceSetupTime());
		// }
		// }

		// ������ҵmap|reduce�����������ʱ���ܺ� LB1,LB2��Ҫ�õ�
		int allTaskL_map_time = 0;
		int allTaskL_reduce_time = 0;
		// ����Ҫ�����
		double LB = 0.0, LB1 = 0, LB2 = 0;

		// (������ҵ��map|reduce�������������ʱ�� )����С����������ҵ LB2
		MapTask maptask = listJob.get(0).mapTask.get(0);// �����һ��ҵ�ĵ�һ��map���������ʱ����С

		// ��С��map ���������ʱ�� ׼��ʱ����С����������ٶ�ȡ�����������ٶ��᣿��Ϊû��������(maptask.mapSize / fd)
		int min_L_map_time = maptask.mapRunTime + maptask.mapSetupTime;

		ReduceTask reducetask = listJob.get(0).reduceTask.get(0);// �����һ��ҵ�ĵ�һ��reduce���������ʱ����С
		// ��С��reduce ���������ʱ��
		// ׼��ʱ����С����������ٶ�ȡ�����������ٶ��᣿��Ϊû��������(reducetask.reduceSize / fd);
		int min_L_reduce_time = reducetask.reduceRunTime + reducetask.reduceSetupTime;

		// ����֤��ȷ
		// System.out.println("����ǰ
		// min_L_map_time:"+min_L_map_time+",min_L_reduce_time:"+min_L_reduce_time);
		// Ҫʹ����ʱ���½��С������Ҫ�ű��أ������ٶ�ѡ����� 100 fd
		for (Job job : listJob) {
			// һ����ҵ�� map �׶�����ʱ���ܺ�
			job.L_map_sumTime = 0;

			// һ����ҵmap�׶��������ʱ������������ʱ��
			int max_L_map_time = 0;
			// һ����ҵ��map���������ʱ���ܺ�
			for (MapTask mt : job.mapTask) {
				// һ��map���������ʱ��
				int tmpMapTaskLtime = mt.mapRunTime + mt.mapSetupTime;
				// ����ʱ�䣺ִ��ʱ��P+��С׼��ʱ�� min S
				job.L_map_sumTime += tmpMapTaskLtime;

				// Ϊ�õ�����ҵ����map��������������ʱ������������ʱ�䡣LB2
				if (tmpMapTaskLtime > max_L_map_time) {
					max_L_map_time = tmpMapTaskLtime;
				}
			}
			// ������ҵ��map���������ʱ���ܺ�
			allTaskL_map_time += job.L_map_sumTime;

			// reduce �׶�����ʱ��
			job.L_reduce_sumTime = 0;
			// һ����ҵmap�׶��������ʱ�����������ʱ��
			int max_L_reduce_time = 0;
			// һ����ҵ��reduce���������ʱ���ܺ�
			for (ReduceTask rt : job.reduceTask) {
				// һ��reduce���������ʱ��
				int tmpReduceTaskLtime = rt.reduceRunTime + rt.reduceSetupTime;
				job.L_reduce_sumTime += tmpReduceTaskLtime;

				// Ϊ�õ�����ҵ����reduce��������������ʱ������������ʱ�� LB2
				if (tmpReduceTaskLtime > max_L_reduce_time) {
					max_L_reduce_time = tmpReduceTaskLtime;
				}
			}
			// ������ҵ��reduce���������ʱ���ܺ�
			allTaskL_reduce_time += job.L_reduce_sumTime;

			// (������ҵ��map|reduce�������������ʱ�� )����С����������ҵ LB2
			if (min_L_map_time > max_L_map_time) {// job�е���������ʱ��������
				min_L_map_time = max_L_map_time;// ȡСmap��������ʱ��
			}
			if (min_L_reduce_time > max_L_reduce_time) {
				min_L_reduce_time = max_L_reduce_time;
			}

		}
		//// System.out.println("allTaskL_map_time:" + allTaskL_map_time +
		//// ",allTaskL_reduce_time:" + allTaskL_reduce_time);
		// ��֤����ʱ�䣬��С����ʱ��
		//// System.out.println("min_L_map_time:" + min_L_map_time +
		//// ",min_L_reduce_time:" + min_L_reduce_time);
		// ������λ��Ч����
		DecimalFormat df = new DecimalFormat("#0.0000");
		// -------------------����LB1------------------------
		// ����ÿ����ҵMap�׶ε�����ʱ������ ��С���� ����---��֤��ȷ
		jobSortAsMapLBTime(listJob);
		double LB1_x = 0.0;// LB1=max{x,y},���е�x��
		// System.out.println("LB listJob size1:"+listJob.size());
		// ȡMr��map��ҵ����ҵԪ�� Hm(Mr)��Mr��reduce slot������
		for (int i = 0; i < Math.min(Mr,listJob.size()); i++) {

			LB1_x += listJob.get(i).L_map_sumTime;
		}
		LB1_x += allTaskL_reduce_time;// ���з����ۼ�ֵ ������ҵ��reduce������ʱ��
		LB1_x = LB1_x / Mr;// ���Է�ĸ
		df.format(LB1_x);

		// ����ÿ����ҵReduce�׶ε�����ʱ������ ��С���� ����
		jobSortAsReduceLBTime(listJob);
		//// ȡMm��reduce��ҵ����ҵԪ�� Hr(Mm)��Mm��Map slot������
		double LB1_y = 0;// LB1=max{x,y},���е�y��
		// ȡMm��reduce��ҵ����ҵԪ�� Hm(Mr)
		for (int i = 0; i < Math.min(Mm,listJob.size()); i++) {
			// ��֤�Ƿ��С����----����֤��ȷ
			// System.out.println("L_reduce_sumTime:"+i+","+listJob.get(i).L_reduce_sumTime);
			LB1_y += listJob.get(i).L_reduce_sumTime;
		}
		LB1_y += allTaskL_map_time;// ���з����ۼ�ֵ
		LB1_y = LB1_y / Mm;// ���Է�ĸ
		df.format(LB1_y);
		// ��֤�����LB1_x��LB1_y
		//// System.out.println("LB1_x:" + LB1_x + ",LB1_y:" + LB1_y);

		// ����LB1;
		LB1 = Math.max(LB1_x, LB1_y);

		// -------------------����LB2------------------------
		double LB2_x = min_L_map_time + (allTaskL_reduce_time / Mr);// LB2=max{x,y},���е�x��
		double LB2_y = min_L_reduce_time + (allTaskL_map_time / Mm);// LB2=max{x,y},���е�y��
		// ����LB2;
		LB2 = Math.max(LB2_x, LB2_y);

		// ��֤�����LB2_x��LB2_y
		//// System.out.println("LB2_x:" + LB2_x + ",LB2_y:" + LB2_y);

		// -------------------����LB------------------------
		LB = Math.max(LB1, LB2);
		df.format(LB);
		// ��֤LB
		//// System.out.println("LB:" + LB);
		return LB;
	}

	// LB������Ҫ��������ʱ��������� ��С�����˳�� Map�׶ε�������ʱ��
	// ������listjob
	public static void jobSortAsMapLBTime(List<Job> listjob) {
		// �Ƚ�����job��mapStageTime
		Collections.sort(listjob, new Comparator<Job>() {// ����
			public int compare(Job job1, Job job2) {
				// return job1.L_map_sumTime.compareTo(job2.getL_map_sumTime());
				return job1.getL_map_sumTime().compareTo(job2.getL_map_sumTime());

			}
		});
		// ��֤��� ---����֤��ȷ
		// for (Job job : listjob) {
		// Map�׶�
		// System.out.println("getL_map_sumTime:" + job.getL_map_sumTime());
		// }
	}

	// LB������Ҫ��������ʱ��������� ��С�����˳�� Reduce�׶ε�������ʱ��
	// ������listjob
	public static void jobSortAsReduceLBTime(List<Job> listjob) {
		// �Ƚ�����job��mapStageTime
		Collections.sort(listjob, new Comparator<Job>() {// ����
			public int compare(Job job1, Job job2) {
				// return
				// job1.L_reduce_sumTime.compareTo(job2.getL_reduce_sumTime());
				return job1.getL_reduce_sumTime().compareTo(job2.getL_reduce_sumTime());
			}
		});
	}

	// ����������׼�������� RE=(Cmax-LB)/LB*100% ���ԽСԽ�ã�Խ�ӽ��½�
	// ÿ����ҵ��{50,100,150,200,250}��30�ף�ȡ����ƽ����Ϊ��߽��׼ȷ��
	public static double relativeError(double Cmax, double LB) {
		double RE = (Cmax - LB) / LB;// 100%
		return RE;
	}


	// ����ÿ����ҵ30�μ����������ƽ��ֵ
	// ��������ҵ������{50,100,150,200,250}
	// public static void average30JobsEA_RE(int jobNum, int nodeNum, int
	// mapSlotNum, double w) {
	// // �洢LB_VALUE,���ں���DAG�㷨����
	// LB_VALUE = new double[jobNum];
	// // �������ڵ�������ÿ���ڵ�map slot������ÿ���ڵ��reduce�����̶�Ϊ2
	// // ��ȡ�ļ�·��
	// String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-"
	// + jobNum + "\\" + "Job-" + jobNum
	// + "-";
	// double averageRE = 0;
	// double tmpRE = 0;
	// // ƽ������ʱ��
	// long avgTime = 0;
	//
	// // 30�ζ�ȡ�ļ� ��30�μ���Cmax�� 30�μ���LB [���ÿ����ҵ��Ŀ���ļ�]----�ܲ���
	// for (int i = 0; i < 30; i++) {
	// // �����㷨����ʱ���õ�startime��endtime ����ƽ��ʱ��
	// long starTime = System.currentTimeMillis();
	// // ��ȡ�ļ� �� ���� jobNum(�ļ�����)��i �ļ�β������
	// String datafilePaht = pathCommon + i + ".txt";
	// List<Job> listJobs = readFromFile(datafilePaht);
	//
	// // ����ʱ����� duration time ���㣬���ǲ���w={0.1��0.3��0.7��0.9}
	// // ��������ҵ���ϣ��ڵ�����ÿ���ڵ��map slot��Ŀ������ʱ�������Ĳ���w
	// durationTime(listJobs, nodeNum, mapSlotNum, w);// w��ֵ
	// // johnson ����ҵ����
	// listJobs = johnsonSort(listJobs);// ����ҵ���򣡣�������
	//
	// // slot��ʼ������
	// Map map = initSlots(nodeNum, mapSlotNum);
	// List<MapSlot> listMapSlot = (List) map.get("listMapSlot");
	// List<ReduceSlot> listReduceSlot = (List) map.get("listReduceSlot");
	//
	// double Cmax;
	// // ---EA��EF�㷨��֤���----
	// // slot���� ������,����Cmax
	// // ������mapslot���ϣ�reduceslot���ϣ���ҵ���ϣ�EA��EF��TBS����ѡ��
	//
	// // EA
	// Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs,
	// 'A');
	//
	// //// System.out.println("Cmax:" + Cmax);
	//
	// // -------------����LB-----------------
	// // LB����Ҫ��Cmax����ó�֮��������ÿ�������setupTimeȷ����Ϊ�½��������ʱ�䷽��
	// double LB = lowerBound(listJobs, nodeNum, mapSlotNum);
	// // DAG��Ҫ��
	// LB_VALUE[i] = LB;
	//
	// // -------------��֤��Դ�����-------------
	// tmpRE = relativeError(Cmax, LB);
	// // tmpRE = Math.floor(tmpRE * 10000) * 0.0001d;// ������λ��Ч����
	// averageRE += tmpRE;
	// // System.out.println("tmpRE:" + tmpRE);
	// // System.out.println("------------------------");
	// long endTime = System.currentTimeMillis() - starTime;
	// avgTime += endTime;
	//
	// }
	// averageRE = averageRE / 30;
	// // ������λ��Ч����
	// DecimalFormat df = new DecimalFormat("#0.0000");
	// System.out.println("averageRE:" + df.format(averageRE));
	// System.out.println("��ҵƽ���������е�ʱ�䣺" + avgTime / 30);
	//
	// // ���ؼ���ƽ��
	// // return averageRE;
	// }
	//
	// public static void average30JobsEF_RE(int jobNum, int nodeNum, int
	// mapSlotNum, double w) {
	// // �������ڵ�������ÿ���ڵ�map slot������ÿ���ڵ��reduce�����̶�Ϊ2
	// // ��ȡ�ļ�·��
	// String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-"
	// + jobNum + "\\" + "Job-" + jobNum
	// + "-";
	// double averageRE = 0;
	// double tmpRE = 0;
	// // ƽ������ʱ��
	// long avgTime = 0;
	//
	// // 30�ζ�ȡ�ļ� ��30�μ���Cmax�� 30�μ���LB [���ÿ����ҵ��Ŀ���ļ�]----�ܲ���
	// for (int i = 0; i < 30; i++) {
	// // �����㷨����ʱ���õ�startime��endtime ����ƽ��ʱ��
	// long starTime = System.currentTimeMillis();
	// // ��ȡ�ļ� �� ���� jobNum(�ļ�����)��i �ļ�β������
	// String datafilePaht = pathCommon + i + ".txt";
	// List<Job> listJobs = readFromFile(datafilePaht);
	//
	// // ����ʱ����� duration time ���㣬���ǲ���w={0.1��0.3��0.7��0.9}
	// // ��������ҵ���ϣ��ڵ�����ÿ���ڵ��map slot��Ŀ������ʱ�������Ĳ���w
	// durationTime(listJobs, nodeNum, mapSlotNum, w);// w��ֵ
	// // johnson ����ҵ����
	// listJobs = johnsonSort(listJobs);// ����ҵ���򣡣�������
	//
	// // slot��ʼ������
	// Map map = initSlots(nodeNum, mapSlotNum);
	// List<MapSlot> listMapSlot = (List) map.get("listMapSlot");
	// List<ReduceSlot> listReduceSlot = (List) map.get("listReduceSlot");
	//
	// double Cmax;
	// // ---EA��EF�㷨��֤���----
	// // slot���� ������,����Cmax
	// // ������mapslot���ϣ�reduceslot���ϣ���ҵ���ϣ�EA��EF��TBS����ѡ��
	//
	// // EF
	// Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs,
	// 'F');
	// //// System.out.println("Cmax:" + Cmax);
	//
	// // -------------����LB-----------------
	// // LB����Ҫ��Cmax����ó�֮��������ÿ�������setupTimeȷ����Ϊ�½��������ʱ�䷽��
	// double LB = lowerBound(listJobs, nodeNum, mapSlotNum);
	// // DAG��Ҫ��
	// LB_VALUE[i] = LB;
	//
	// // -------------��֤��Դ�����-------------
	// tmpRE = relativeError(Cmax, LB);
	// // tmpRE = Math.floor(tmpRE * 10000) * 0.0001d;// ������λ��Ч����
	// averageRE += tmpRE;
	// // System.out.println("tmpRE:" + tmpRE);
	// // System.out.println("------------------------");
	// long endTime = System.currentTimeMillis() - starTime;
	// avgTime += endTime;
	//
	// }
	// averageRE = averageRE / 30;
	// // ������λ��Ч����
	// DecimalFormat df = new DecimalFormat("#0.0000");
	// System.out.println("averageRE:" + df.format(averageRE));
	// System.out.println("��ҵƽ���������е�ʱ�䣺" + avgTime / 30);
	//
	// // ���ؼ���ƽ��
	// // return averageRE;
	// }
	//
	//
	// public static void average30JobsTBS_RE(int jobNum, int nodeNum, int
	// mapSlotNum, double w) {
	//
	// // �������ڵ�������ÿ���ڵ�map slot������ÿ���ڵ��reduce�����̶�Ϊ2
	// // ��ȡ�ļ�·��
	// String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-"
	// + jobNum + "\\" + "Job-" + jobNum
	// + "-";
	// double averageRE = 0;
	// double tmpRE = 0;
	// // ƽ������ʱ��
	// long avgTime = 0;
	//
	// // 30�ζ�ȡ�ļ� ��30�μ���Cmax�� 30�μ���LB [���ÿ����ҵ��Ŀ���ļ�]----�ܲ���
	// for (int i = 0; i < 30; i++) {
	// // �����㷨����ʱ���õ�startime��endtime ����ƽ��ʱ��
	// long starTime = System.currentTimeMillis();
	// // ��ȡ�ļ� �� ���� jobNum(�ļ�����)��i �ļ�β������
	// String datafilePaht = pathCommon + i + ".txt";
	// List<Job> listJobs = readFromFile(datafilePaht);
	//
	// // ����ʱ����� duration time ���㣬���ǲ���w={0.1��0.3��0.7��0.9}
	// // ��������ҵ���ϣ��ڵ�����ÿ���ڵ��map slot��Ŀ������ʱ�������Ĳ���w
	// durationTime(listJobs, nodeNum, mapSlotNum, w);// w��ֵ
	// // johnson ����ҵ����
	// listJobs = johnsonSort(listJobs);// ����ҵ���򣡣�������
	//
	// // slot��ʼ������
	// Map map = initSlots(nodeNum, mapSlotNum);
	// List<MapSlot> listMapSlot = (List) map.get("listMapSlot");
	// List<ReduceSlot> listReduceSlot = (List) map.get("listReduceSlot");
	//
	// double Cmax;
	// // ---EA��EF�㷨��֤���----
	// // slot���� ������,����Cmax
	// // ������mapslot���ϣ�reduceslot���ϣ���ҵ���ϣ�EA��EF��TBS����ѡ��
	//
	// // TBS
	// Cmax = taskToSlotScheduler_TBS(listMapSlot, listReduceSlot, listJobs);
	// //// System.out.println("Cmax:" + Cmax);
	//
	// // -------------����LB-----------------
	// // LB����Ҫ��Cmax����ó�֮��������ÿ�������setupTimeȷ����Ϊ�½��������ʱ�䷽��
	// double LB = lowerBound(listJobs, nodeNum, mapSlotNum);
	// // DAG��Ҫ��
	// LB_VALUE[i] = LB;
	//
	// // -------------��֤��Դ�����-------------
	// tmpRE = relativeError(Cmax, LB);
	// // tmpRE = Math.floor(tmpRE * 10000) * 0.0001d;// ������λ��Ч����
	// averageRE += tmpRE;
	// // System.out.println("tmpRE:" + tmpRE);
	// // System.out.println("------------------------");
	// long endTime = System.currentTimeMillis() - starTime;
	// avgTime += endTime;
	//
	// }
	// averageRE = averageRE / 30;
	// // ������λ��Ч����
	// DecimalFormat df = new DecimalFormat("#0.0000");
	// System.out.println("averageRE:" + df.format(averageRE));
	// System.out.println("��ҵƽ���������е�ʱ�䣺" + avgTime / 30);
	//
	// // ���ؼ���ƽ��
	// // return averageRE;
	// }

	public static void avg30DAG(int jobNum, int nodeNum, int mapSlotNum,char EA_EF) {
		// ƽ������ʱ��
		long sumTime = 0;
		// ÿ�ε�������
		double tmpDAGRE = 0;
		// 30�ε�ƽ��������
		double avgDAGRE = 0;
		//double avgCmax=0;
		// ��ȡ�ļ�·��
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-" + jobNum + "\\" + "Job-" + jobNum
				+ "-";
		for (int i = 0; i < 30; i++) {
			// �����㷨����ʱ���õ�startime��endtime
			long starTime = System.currentTimeMillis();
			// ��ȡ�ļ� �� ���� jobNum(�ļ�����)��i �ļ�β������
			String datafilePaht = pathCommon + i + ".txt";
			List<Job> listJobs = SourceScheduler.readFromFile(datafilePaht);
			// List<Job> listJobs = readFromFile(datafilePaht);
			// ------------------------��ʼ������---------------------------------
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
			
			//����DAG���½�û�а���Johnson����ȡ�½�ֵ��������ǰ�����㷨�е��½���Ϊ�����½�
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

		System.out.println("��ҵƽ���������е�ʱ�䣺" + sumTime / 30);
		// return avgDAGRE;
	}

	public static void average30JobsRE(int jobNum, int nodeNum, int mapSlotNum, double w, char EA_EF_TBS) {
		// �洢LB_VALUE,���ں���DAG�㷨����
		LB_VALUE_EA = new double[jobNum];
		LB_VALUE_EF = new double[jobNum];
		// �������ڵ�������ÿ���ڵ�map slot������ÿ���ڵ��reduce�����̶�Ϊ2
		// ��ȡ�ļ�·��
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-" + jobNum + "\\" + "Job-" + jobNum
				+ "-";
		double averageRE = 0;
		double tmpRE = 0;
		// ƽ������ʱ��
		long avgTime = 0;
 
		//double avgCmax=0;
		// 30�ζ�ȡ�ļ� ��30�μ���Cmax�� 30�μ���LB [���ÿ����ҵ��Ŀ���ļ�]----�ܲ���
		for (int i = 0; i < 30; i++) {
			// �����㷨����ʱ���õ�startime��endtime ����ƽ��ʱ��
			long starTime = System.currentTimeMillis();
			// ��ȡ�ļ� �� ���� jobNum(�ļ�����)��i �ļ�β������
			String datafilePaht = pathCommon + i + ".txt";
			List<Job> listJobs = readFromFile(datafilePaht);

			// ����ʱ����� duration time ���㣬���ǲ���w={0.1��0.3��0.7��0.9}
			// ��������ҵ���ϣ��ڵ�����ÿ���ڵ��map slot��Ŀ������ʱ�������Ĳ���w
			durationTime(listJobs, nodeNum, mapSlotNum, w);// w��ֵ
			// johnson ����ҵ����
			listJobs = johnsonSort(listJobs);// ����ҵ���򣡣�������

			// slot��ʼ������
			Map map = initSlots(nodeNum, mapSlotNum);
			List<MapSlot> listMapSlot = (List) map.get("listMapSlot");
			List<ReduceSlot> listReduceSlot = (List) map.get("listReduceSlot");

			double Cmax, LB;
			// ---EA��EF�㷨��֤���----
			// slot���� ������,����Cmax
			// ������mapslot���ϣ�reduceslot���ϣ���ҵ���ϣ�EA��EF��TBS����ѡ��
			if (EA_EF_TBS == 'A') { // �ж�30��
				// EA
				Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs, 'A');
				// -------------����LB-----------------
				// LB����Ҫ��Cmax����ó�֮��������ÿ�������setupTimeȷ����Ϊ�½��������ʱ�䷽��
				LB = lowerBound(listJobs, nodeNum, mapSlotNum);
				// DAG��Ҫ��
				LB_VALUE_EA[i] = LB;
			} else {// if (EA_EF_TBS == 'F')
				// EF
				Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs, 'F');
				// -------------����LB-----------------
				// LB����Ҫ��Cmax����ó�֮��������ÿ�������setupTimeȷ����Ϊ�½��������ʱ�䷽��
				LB = lowerBound(listJobs, nodeNum, mapSlotNum);
				// DAG��Ҫ��
				LB_VALUE_EF[i] = LB; 
		//		System.out.println("LB_VALUE_EF:"+LB_VALUE_EF[i]);
			} 
			
		//	System.out.println("LB:"+i+",value:"+LB);
			// -------------��֤��Դ�����-------------
			tmpRE = relativeError(Cmax, LB);
			// tmpRE = Math.floor(tmpRE * 10000) * 0.0001d;// ������λ��Ч����
			averageRE += tmpRE;
			// System.out.println("tmpRE:" + tmpRE);
			// System.out.println("------------------------");
			long endTime = System.currentTimeMillis() - starTime;
			avgTime += endTime;
		//	avgCmax+=Cmax;
		}
		//System.out.println("EAEF Cmax:"+avgCmax/30);
		averageRE = averageRE / 30;
		// ������λ��Ч����
		DecimalFormat df = new DecimalFormat("#0.0000");
		System.out.println("averageRE:" + df.format(averageRE));
		System.out.println("��ҵƽ���������е�ʱ�䣺" + avgTime / 30);

		// ���ؼ���ƽ��
		// return averageRE;
	}

	public static void average30JobsTBS_RE(int jobNum, int nodeNum, int mapSlotNum, double w) {
		// �洢LB_VALUE,���ں���DAG�㷨����
		// LB_VALUE = new double[jobNum];
		// �������ڵ�������ÿ���ڵ�map slot������ÿ���ڵ��reduce�����̶�Ϊ2
		// ��ȡ�ļ�·��
		String pathCommon = "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\" + "Job-" + jobNum + "\\" + "Job-" + jobNum
				+ "-";
		double averageRE = 0;
		double tmpRE = 0;
		// ƽ������ʱ��
		long avgTime = 0;
	//	double avgCmax=0;
		// 30�ζ�ȡ�ļ� ��30�μ���Cmax�� 30�μ���LB [���ÿ����ҵ��Ŀ���ļ�]----�ܲ���
		for (int i = 0; i < 30; i++) {
			// �����㷨����ʱ���õ�startime��endtime ����ƽ��ʱ��
			long starTime = System.currentTimeMillis();
			// ��ȡ�ļ� �� ���� jobNum(�ļ�����)��i �ļ�β������
			String datafilePaht = pathCommon + i + ".txt";
			List<Job> listJobs = readFromFile(datafilePaht);

			// slot��ʼ������
			Map map = initSlots(nodeNum, mapSlotNum);
			List<MapSlot> listMapSlot = (List) map.get("listMapSlot");
			List<ReduceSlot> listReduceSlot = (List) map.get("listReduceSlot");
			// ����ʱ����� duration time ���㣬���ǲ���w={0.1��0.3��0.7��0.9}
			// ��������ҵ���ϣ��ڵ�����ÿ���ڵ��map slot��Ŀ������ʱ�������Ĳ���w
			durationTime(listJobs, nodeNum, mapSlotNum, w);// w��ֵ

			double Cmax = 0.0;
			// ---EA��EF�㷨��֤���----
			// slot���� ������,����Cmax
			// ������mapslot���ϣ�reduceslot���ϣ���ҵ���ϣ�EA��EF��TBS����ѡ��
			// TBS
			Cmax = taskToSlotScheduler_TBS(listMapSlot, listReduceSlot, listJobs);
			// // johnson ����ҵ����
			// -------------����LB-----------------
			// LB����Ҫ��Cmax����ó�֮��������ÿ�������setupTimeȷ����Ϊ�½��������ʱ�䷽��
			// for(Job jb:listJobs){
			// System.out.println("jb:"+jb.mapStageTime);
			// }
			double LB = lowerBound(listJobs, nodeNum, mapSlotNum);
			// System.out.println("LB:"+i+",value:"+LB);
			// DAG��Ҫ��
			// LB_VALUE[i] = LB;

			// -------------��֤��Դ�����-------------
			tmpRE = relativeError(Cmax, LB);
			// tmpRE = Math.floor(tmpRE * 10000) * 0.0001d;// ������λ��Ч����
			averageRE += tmpRE;
			// System.out.println("tmpRE:" + tmpRE);
			// System.out.println("------------------------");
			long endTime = System.currentTimeMillis() - starTime;
			avgTime += endTime;
	//		avgCmax+=Cmax;
		}
	//	System.out.println("TBS avgCax:"+avgCmax/30);
		averageRE = averageRE / 30;
		// ������λ��Ч����
		DecimalFormat df = new DecimalFormat("#0.0000");
		System.out.println("averageRE:" + df.format(averageRE));
		System.out.println("��ҵƽ���������е�ʱ�䣺" + avgTime / 30);

		// ���ؼ���ƽ��
		// return averageRE;
	}

	public static void main(String[] args) {

		// ---------------�������ݼ�����----------------------
		// �������� 150��ʵ������ҵ{50,100,150,200,250}
		// produceDataset();

		// ---------------������---------------------
		// 30����ҵ�����������ƽ��ֵ���
		// �������ڵ�������ÿ���ڵ�map slot������ÿ���ڵ��reduce�����̶�Ϊ2
		int jobNum = 250;// {50,100,150,200,250}
		int nodeNum = 10;// {10,15,20,25,30}
		int mapSlotNum =8 ;// {2,4,6,8}
		double w = 0.5;// {0.1,0.3,0.5,0.7,0.9}
		System.out.println("-----------------��Դ�����㷨�Ƚ�-----------------");
		System.out.println("��ҵ����" + jobNum);
		System.out.println("�ڵ�����" + nodeNum);
		System.out.println("ÿ���ڵ�map slot����" + mapSlotNum);
		System.out.println("����ʱ����Ȳ�����" + w);
		// ---------EA��EF��TBS�㷨���----------------
		// ��������ҵ���������Ŀ��ÿ���ڵ�map slot��Ŀ������ʱ��w 
		
		System.out.println("-----------------�㷨EA-----------------------");
		average30JobsRE(jobNum, nodeNum, mapSlotNum, w, 'A');
		System.out.println("-----------------�㷨DAG EA-----------------");
		avg30DAG(jobNum, nodeNum, mapSlotNum,'A');
		
		System.out.println("-----------------�㷨EF-----------------------");
		average30JobsRE(jobNum, nodeNum, mapSlotNum, w, 'F');
		System.out.println("-----------------�㷨DAG  EF-----------------");
		avg30DAG(jobNum, nodeNum, mapSlotNum,'F');
		System.out.println("-----------------�㷨TBS----------------------");
		average30JobsTBS_RE(jobNum, nodeNum, mapSlotNum, w);

		// �������
		// queue();
		// �ݹ��ļ���������
		// String path="D:\\JaveWeb\\MapReduceScheduler\\DataSet";
		// getFiles(path);
	}
}
// ����
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

// Ԥ�ȹ�������map��reduce���������ʱ���
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
// System.out.println("������ҵ��map��������ʱ���ܺͣ� "+mapLtime/20);
// System.out.println("������ҵ��reduce��������ʱ���ܺͣ� "+reduceLtime/20);

// ÿ����ҵ����������
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
// ��ҵ����
// 11,4,24,41,29,50,13,43,48,2,17,16,9,27,3,40,15,20,26,21,28,30,31,36,5,34,12,18,45,7,25,23,39,6,47,22,33,38,46,14,49,8,44,37,35,42,19,10,1,32
//
// ����������
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

// һ�εĲ���------������ҵ������
/*
 * String dataFilePath50 =
 * "D:\\JaveWeb\\MapReduceScheduler\\DataSet\\Job-50\\Job-50-0.txt";// �ȹ̶�
 * List<Job> listJobs = readFromFile(dataFilePath50); // ����Cmax
 * durationTime(listJobs, nodeNum, mapSlotNum, w);// w��ֵ listJobs =
 * johnsonSort(listJobs); // ����֤
 * 
 * // slot��ʼ������ Map map = initSlots(nodeNum, mapSlotNum); List<MapSlot>
 * listMapSlot = (List) map.get("listMapSlot"); List<ReduceSlot> listReduceSlot
 * = (List) map.get("listReduceSlot"); // ---EA��EF�㷨��֤���---- // EA double Cmax =
 * taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, listJobs, 'A'); // EF
 * // double Cmax = taskToSlotScheduler_EA_EF(listMapSlot, listReduceSlot, //
 * listJobs, 'F'); // TBS // double Cmax = taskToSlotScheduler_TBS(listMapSlot,
 * listReduceSlot, // listJobs);
 * 
 * // -----��֤LB--------LB����Ҫ��Cmax����ó�֮��������ÿ�������setupTimeȷ����Ϊ�½��������ʱ�䷽��
 * System.out.println("LB"); double LB = lowerBound(listJobs, nodeNum,
 * mapSlotNum); System.out.println("Cmax:" + Cmax); // ��֤��Դ����� tmpRE =
 * relativeError(Cmax, LB); tmpRE = Math.round(tmpRE * 10000) * 0.0001d;//
 * ������λ��Ч���� System.out.println("tmpRE:" + tmpRE);
 */
