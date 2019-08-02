package designpatterns.strategy;

/**
 * @author zhongyou_li
 */
public class BlackPeopleDisplayImpl implements DisplayInterface {

    @Override
    public void display() {
        System.out.println("黑种人");
    }
}
