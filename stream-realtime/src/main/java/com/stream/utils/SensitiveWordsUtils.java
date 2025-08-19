package com.stream.utils;

import com.alibaba.fastjson.JSONObject;
import org.checkerframework.common.reflection.qual.GetClass;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @BelongsProject: dev-test
 * @BelongsPackage: com.stream.utils
 * @Author: cuijiangqi
 * @CreateTime: 2025-08-19  14:58
 * @Description: TODO
 * @Version: 1.0
 */
public class SensitiveWordsUtils {
    public static ArrayList<String> getSensitiveWordsLists(){
        ArrayList<String> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("/Users/zhouhan/dev_env/work_project/java/stream-dev/stream-realtime/src/main/resources/Identify-sensitive-words.txt"))){
            String line ;
            while ((line = reader.readLine()) != null){
                res.add(line);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        return res;
    }

    public static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size());
        return list.get(randomIndex);
    }

    public static void main(String[] args) {
        System.err.println(getRandomElement(getSensitiveWordsLists()));
    }
}
