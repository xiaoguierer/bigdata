import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class JavaCoolecotion {
    public static void main(String[] args) {
        HashMap<Integer,Integer> map = new HashMap<Integer, Integer>();
        for(Integer i=1;i<1000000000;i++){
            map.put(i,i);
        }
        System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));
        System.out.print(map.size()+"------------------"+map.get(98412355));
        System.out.println(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));
    }
}
