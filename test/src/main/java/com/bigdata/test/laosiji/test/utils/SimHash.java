package com.bigdata.test.laosiji.test.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class SimHash {
    private String tokens;

    private BigInteger intSimHash;

    private String strSimHash;

    private int hashbits = 64;

    public SimHash(String tokens) {
        this.tokens = tokens;
        this.intSimHash =this.simHash();
    }

    public SimHash(String tokens, BigInteger inthashbits) {
        this.tokens = tokens;
        this.hashbits = hashbits;
        this.intSimHash =this.simHash();
    }

    HashMap<String, Integer> wordMap =new HashMap<String, Integer>();

    public BigInteger simHash() {
        // 定义特征向量/数组
        int[] v =new int[this.hashbits];
        // 1、将文本去掉格式后, 分词.
        StringTokenizer stringTokens =new StringTokenizer(this.tokens);
        while(stringTokens.hasMoreTokens()) {
            String temp = stringTokens.nextToken();
            BigInteger t =this.hash(temp);
            // 2、将每一个分词hash为一组固定长度的数列.比如 64bit 的一个整数.
            for(int i = 0; i < this.hashbits; i++) {
                BigInteger bitmask =new BigInteger("1").shiftLeft(i);
                // 3、建立一个长度为64的整数数组(假设要生成64位的数字指纹,也可以是其它数字),
                // 对每一个分词hash后的数列进行判断,如果是1000...1,那么数组的第一位和末尾一位加1,
                // 中间的62位减一,也就是说,逢1加1,逢0减1.一直到把所有的分词hash数列全部判断完毕.
                if(t.and(bitmask).signum() != 0) {
                    // 这里是计算整个文档的所有特征的向量和
                    // 这里实际使用中需要 +- 权重，而不是简单的 +1/-1，
                    v[i] +=1;
                }else {
                    v[i] -=1;
                }
            }
        }
        BigInteger fingerprint =new BigInteger("0");
        StringBuffer simHashBuffer =new StringBuffer();
        for(int i = 0; i < this.hashbits; i++) {
            // 4、最后对数组进行判断,大于0的记为1,小于等于0的记为0,得到一个 64bit 的数字指纹/签名.
            if(v[i] >= 0) {
                fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
                simHashBuffer.append("1");
            }else {
                simHashBuffer.append("0");
            }
        }
        this.strSimHash = simHashBuffer.toString();
        System.out.println("SimHash:     "+this.strSimHash +" length " + this.strSimHash.length());
        return fingerprint;
    }

    private BigInteger hash(String source) {
        if(source == null|| source.length() == 0) {
            return new BigInteger("0");
        }else {
            char[] sourceArray = source.toCharArray();
            BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
            BigInteger m =new BigInteger("1000003");
            BigInteger mask =new BigInteger("2").pow(this.hashbits).subtract(new BigInteger("1"));
            for(char item : sourceArray) {
                BigInteger temp = BigInteger.valueOf((long) item);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(source.length())));
            if(x.equals(new BigInteger("-1"))) {
                x =new BigInteger("-2");
            }
            return x;
        }
    }

    public int hammingDistance(SimHash other) {

        BigInteger x =this.intSimHash.xor(other.intSimHash);
        int tot = 0;

        // 统计x中二进制位数为1的个数
        // 我们想想，一个二进制数减去1，那么，从最后那个1（包括那个1）后面的数字全都反了，对吧，然后，n&(n-1)就相当于把后面的数字清0，
        // 我们看n能做多少次这样的操作就OK了。

        while(x.signum() != 0) {
            tot +=1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    public int getDistance(String str1, String str2) {
        int distance;
        if(str1.length() != str2.length()) {
            distance = -1;
        }else {
            distance =0;
            for(int i = 0; i < str1.length(); i++) {
                if(str1.charAt(i) != str2.charAt(i)) {
                    distance++;
                }
            }
        }
        return distance;
    }

    public List subByDistance(SimHash simHash, int distance) {
        // 分成几组来检查
        int numEach = this.hashbits / (distance +1);
        List characters =new ArrayList();

        StringBuffer buffer =new StringBuffer();

        int k = 0;
        for(int i = 0; i < this.intSimHash.bitLength(); i++) {
            // 当且仅当设置了指定的位时，返回 true
            boolean sr = simHash.intSimHash.testBit(i);

            if(sr) {
                buffer.append("1");
            }else {
                buffer.append("0");
            }

            if((i + 1) % numEach ==0) {
                // 将二进制转为BigInteger
                BigInteger eachValue =new BigInteger(buffer.toString(),2);
                System.out.println("----"+ eachValue);
                buffer.delete(0, buffer.length());
                characters.add(eachValue);
            }
        }

        return characters;
    }

    public static void main(String[] args) {
        String ss ="天津市明正工程咨询有限公司受天津大学的委托，就天津大学数学学院室内改造工程（第二次）项目（项目编号：TDHQ201700071）组织采购，评标工作已经结束，中标结果如下：\n" +
                "一、项目信息\n" + "项目编号：TDHQ201700071\n" + "项目名称：天津大学数学学院室内改造工程（第二次）\n" + "项目联系人：孔老师\n" + "联系方式：022-27407597\n" + "二、采购单位信息\n" +
                "采购单位名称：天津大学\n" + "采购单位地址：天津市南开区卫津路92号\n" + "采购单位联系方式：王老师 022-83613835\n" + "三、项目用途、简要技术要求及合同履行日期：\n" +
                "1、项目用途和简单技术要求：天津大学数学学院室内改造工程（第二次），本项目建设地点在天津大学卫津路校区院内，具体内容见招标文件。\n" + "2、合同履行日期：\n" + "计划工期：30天\n" +
                "计划开工日期：2017年09月04日\n" + "计划竣工日期：2017年10月03日\n" + "四、采购代理机构信息\n" + "采购代理机构全称：天津市明正工程咨询有限公司\n" +
                "采购代理机构地址：天津市南开区复康路23号增1号15层\n" + "采购代理机构联系方式：李非凡 022-23611619-806\n" + "五、中标信息\n" + "招标公告日期：2017年08月08日\n" +
                "中标日期：2017年08月29日\n" + "总中标金额：25.6185 万元（人民币）\n" + "中标供应商名称、联系地址及中标金额：\n" + "中标供应商名称：天津科源工程设计有限公司\n" +
                "中标供应商联系地址： 天津市河西区郁江道17号209室\n" + "中标金额：25.6185万元（人民币）\n" + "评审专家名单：\n" + "时亭、华舒、李玲、郝旭光、杨俊、滕戬、裴佳良\n" +
                "中标标的名称、规格型号、数量、单价、服务要求：\n" + "天津大学数学学院室内改造工程（第二次），本项目建设地点在天津大学卫津路校区院内，具体内容见招标文件。\n" +
                "六、其它补充事宜\n" + "各投标人如对上述结果有异议，请在中标结果公示后7个工作日内以书面方式（公司法人或投标代表签字并加盖公章）向天津大学招标采购监督小组办公室投诉。公示期满无投诉，不再另行公告中标结果。\n" +
                "联系电话：022-27403794（招标采购监督小组办公室）022-27407597（招标办）\n" + "联系人：陈老师  孔老师\n" + "天津大学招投标管理办公室\n" + "天津市明正工程咨询有限公司\n" + "2017年08月29日";
     //   String s ="北京,上海,香港,深圳,广州,台北,南京,大连,苏州,青岛,无锡,佛山,重庆,宁波,杭州,成都,武汉,澳门,天津,沈阳,需要创建一个session来启动这个图，例如上面代码中，只有在session中启动这个图才能真正的进行矩阵相乘的运算";

        SimHash hash1 =new SimHash(ss, BigInteger.valueOf(64));
        System.out.println("hash1:  "+hash1.intSimHash +"  " + hash1.intSimHash.bitLength()+"  "+hash1.intSimHash.bitCount());
        hash1.subByDistance(hash1,3);


        String s1 =ss + ".";
        SimHash hash2 =new SimHash(s1, BigInteger.valueOf(64));
        System.out.println("hash2:   "+hash2.intSimHash +"  " + hash2.intSimHash.bitLength()+"  "+hash1.intSimHash.bitCount());
        hash1.subByDistance(hash2,3);


        String s2 =ss+"北京,上海,香港,深圳,广州,台北,南京,大连,苏州,青岛,无锡,佛山,重庆,宁波,杭州,成都,武汉,澳门,天津,沈阳";
        SimHash hash3 =new SimHash(s2, BigInteger.valueOf(64));
        System.out.println("hash3:  "+hash3.intSimHash +"  " + hash3.intSimHash.bitLength()+"  "+hash1.intSimHash.bitCount());
        hash1.subByDistance(hash3,3);

        System.out.println("============================");

        int dis = hash1.getDistance(hash1.strSimHash, hash2.strSimHash);
        System.out.println(hash1.hammingDistance(hash2) +" " + dis);

        int dis2 = hash1.getDistance(hash1.strSimHash, hash3.strSimHash);
        System.out.println(hash1.hammingDistance(hash3) +" " + dis2);

        //通过Unicode编码来判断中文
        String str ="中国chinese";
        for(int i = 0; i < str.length(); i++) {
            System.out.println(str.substring(i, i +1).matches("[\\u4e00-\\u9fbb]+"));
        }
    }
}
