/**
 * ��˵��
 *
 * @author:Amy
 * @version:2016��8��18������11:21:59
 */
public class MapTask {
	// ִ��ʱ��
	public Integer mapRunTime;

	// �ļ���С
	public int mapSize;
	// ׼��ʱ��
	public double mapSetupTime;

	// TBS�㷨��Ϊ�����������ھ����ĸ���ҵ��ȷ������һ���ֶ�
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

	public double getMapSetupTime() {
		return mapSetupTime;
	}

	public void setMapSetupTime(int mapSetupTime) {
		this.mapSetupTime = mapSetupTime;
	}

}
