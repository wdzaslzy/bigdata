package designpatterns.strategy;

/**
 * @author zhongyou_li
 */
public class AsianDisplayImpl implements DisplayInterface {
    @Override
    public void display() {
        System.out.println("黄种人");
    }
}
