/**
 * 类说明
 *
 * @author:Amy
 * @version:2016年8月18日上午11:21:59
 */
public class MapTask {
	// 执行时间
	public Integer mapRunTime;

	// 文件大小
	public int mapSize;
	// 准备时间
	public int mapSetupTime;

	// TBS算法，为了让任务属于具体哪个作业明确，加上一个字段
	public Integer belongJobId;

	public Integer getBelongJobId() {
		return belongJobId;
	}

	public void setBelongJobId(Integer belongJobId) {
		this.belongJobId = belongJobId;
	}

	public int getMapSize() {
		return mapSize;
	}

	public Integer getMapRunTime() {
		return mapRunTime;
	}

	public void setMapRunTime(Integer mapRunTime) {
		this.mapRunTime = mapRunTime;
	}

	public void setMapSize(int mapSize) {
		this.mapSize = mapSize;
	}

	public int getMapSetupTime() {
		return mapSetupTime;
	}

	public void setMapSetupTime(int mapSetupTime) {
		this.mapSetupTime = mapSetupTime;
	}

}
