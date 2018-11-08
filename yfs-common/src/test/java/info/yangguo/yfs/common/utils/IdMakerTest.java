package info.yangguo.yfs.common.utils;

import org.junit.Test;

import java.util.Arrays;

public class IdMakerTest {
    @Test
    public void test1() {
        String id = IdMaker.INSTANCE.next("group01", "store1");
        System.out.println(id);
        Arrays.stream(IdMaker.INSTANCE.split(id)).forEach(part -> System.out.println(part));
    }
}
