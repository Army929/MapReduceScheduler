import java.util.ArrayList;
import java.util.List;

/**
 * ��˵��
 *
 * @author:Amy
 * @version:2016��9��1������3:42:45
 */
public class MapSlot {
	// map��reudce slot��������һ������Ϊ�����ڱ�
	// ÿ̨�ڵ�map slot������2��4��6��8 �����ڵ����޹�
	public Double currentMapSlotTime;

	// map slot���ٶ�
	public Integer mapSlotSpeed;

	// TBS �㷨��Ҫ��¼��ӵ�mapTask��˳��,ÿ��Slot��Ҫ��¼maptask����ֻ��map�׶Σ�û��reduce�׶�
	List<MapTask> maptaskSlotList = new ArrayList<MapTask>();

	public Integer getMapSlotSpeed() {
		return mapSlotSpeed;
	}

	public void setMapSlotSpeed(Integer mapSlotSpeed) {
		this.mapSlotSpeed = mapSlotSpeed;
	}

	public Double getCurrentMapSlotTime() {
		return currentMapSlotTime;
	}

	public void setCurrentMapSlotTime(Double currentMapSlotTime) {
		this.currentMapSlotTime = currentMapSlotTime;
	}

}
