/**
 * 类说明
 *
 * @author:Amy
 * @version:2016年8月18日上午11:22:14
 */
public class ReduceTask {
	// 执行时间
	public Integer reduceRunTime;

	// 文件大小
	public int reduceSize;
	// 准备时间
	public double reduceSetupTime;

	public Integer getReduceRunTime() {
		return reduceRunTime;
	}

	public void setReduceRunTime(Integer reduceRunTime) {
		this.reduceRunTime = reduceRunTime;
	}

	public int getReduceSize() {
		return reduceSize;
	}

	public void setReduceSize(int reduceSize) {
		this.reduceSize = reduceSize;
	}

	public double getReduceSetupTime() {
		return reduceSetupTime;
	}

	public void setReduceSetupTime(int reduceSetupTime) {
		this.reduceSetupTime = reduceSetupTime;
	}

}
