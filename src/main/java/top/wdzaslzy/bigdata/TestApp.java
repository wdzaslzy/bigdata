package top.wdzaslzy.bigdata;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhongyou_li
 */
public class TestApp {

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(-1);
        list.add(-1);
        list.add(-1);
        list.add(-1);
        list.add(3);
        list.add(8);
        List<Integer> target = new ArrayList<>();

        int last = -999;
        for (int i = 0; i< list.size(); i++) {
            Integer index = list.get(i);
            if (last == index) {
                Integer lastTarget = target.get(i - 1);
                target.add(lastTarget - 1);
            } else {
                target.add(index);
            }
            last = index;
        }

        for (int i : target) {
            System.out.println(i);
        }

    }

}
