package com.performance.domain.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.performance.domain.dao.UserDao;
import com.performance.domain.entity.UserHobby;
import com.performance.domain.entity.UserInfo;
import com.performance.domain.entity.UserMaster;

@Service
public class PerformanceService {

    final static Logger log = LogManager.getLogger(PerformanceService.class);

    private final String MEASURE_FLAG_ON  = "1";

    private GoogleApiService googleService;

    private UserDao userDao;
    
    private Map<String, Long> resultMap = new HashMap<String, Long>();
    private Map<String, Boolean> assertionResultMap = new HashMap<String, Boolean>();

    public PerformanceService(GoogleApiService googleService, UserDao userDao) {
        this.googleService = googleService;
        this.userDao = userDao;
    }

    @Async("perfomanceExecutor")
    public void execute(String uuid, String measureFlag) {

        resultMap.clear();
        resultMap.put(uuid, null);

        Long start = System.currentTimeMillis();

        List<UserMaster> matchingUserList = uploadExecute();

        Long end = System.currentTimeMillis();
        Long executeTime = end - start;

        resultMap.put(uuid, executeTime);
        // アサーション入れる
        Boolean assertionResult = assertion(matchingUserList);
        assertionResultMap.put(uuid, assertionResult);
        
        // 計測実施かつアサーションが成功している場合のみ送る
        if(MEASURE_FLAG_ON.equals(measureFlag) && assertionResult) {
            try {
                googleService.execute(executeTime);
            } catch (Exception e) {
                log.error("スプレッドシートの更新でエラーが発生しました。", e);
            }
        }
        return;
    }
    public List<UserMaster> uploadExecute() {
        // テーブル情報を空にする
        /** 変更不可 **/
        truncateTable();
        /** 変更不可 **/
        
        // CSVを取得・CSVファイルをDBに登録する
        //ファイル読み込みで使用するクラス
        BufferedReader br = null;
        InputStreamReader fs = null;
        List<String> csvFile = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        try {

            //読み込みファイルのインスタンス生成
            //ファイル名を指定する
            fs = new InputStreamReader(new FileInputStream(new File("data/userInfo.csv")), StandardCharsets.UTF_8);
            br = new BufferedReader(fs);            

            //読み込み行
            String readLine;

            //読み込み行数の管理
            int i = 0;

            //1行ずつ読み込みを行う
            while ((readLine = br.readLine()) != null) {
                i++;
                //データ内容をコンソールに表示する
                log.info("-------------------------------");

                //データ件数を表示
                sb.append("データ読み込み");
                sb.append(i);
                sb.append("件目");
                log.info(sb.toString());
                sb.setLength(0);
                
                csvFile.add(readLine);
            }
        } catch (Exception e) {
            log.info("csv read error", e);
        } finally {
            try {
                br.close();
            } catch (Exception e) {
            }
        }

        try {
            int i = 0;
            List<UserInfo> userInfoList = new ArrayList<>();
            List<UserHobby> UserHobbyList = new ArrayList<>();
            String id;
            String lineSeparator = System.getProperty("line.separator");
            Pattern pattern = Pattern.compile(".新潟県,上越市.");
            Map<String, String> map = new LinkedHashMap<String, String>();
            for(String line : csvFile) {
                //カンマで分割した内容を配列に格納する
                String[] data = line.split(",", -1);

                map.put("ユーザー姓:", data[1]);
                map.put("出身都道府県:", data[0]);
                map.put("ユーザー名:", data[2]);
                map.put("出身市区町村:", data[3]);
                map.put("血液型:", data[4]);
                map.put("趣味1:", data[5]);
                map.put("趣味2:", data[6]);
                map.put("趣味3:", data[7]);
                map.put("趣味4:", data[8]);
                map.put("趣味5:", data[9]);
                
                //データ内容をコンソールに表示する
                //配列の中身を順位表示する。列数(=列名を格納した配列の要素数)分繰り返す
                logoutput(map, sb, lineSeparator);
                sb.setLength(0);
                map.clear();
                
                UserInfo userInfo = new UserInfo();
                UserHobby userHobby = new UserHobby();

                id = UUID.randomUUID().toString();
                userInfo.setId(id);
                userInfo.setLastName(data[0]);
                userInfo.setFirstName(data[1]);
                userInfo.setPrefectures(data[2]);
                userInfo.setCity(data[3]);
                userInfo.setBloodType(data[4]);
                userHobby.setId(id);
                userHobby.setHobby1(data[5]);
                userHobby.setHobby2(data[6]);
                userHobby.setHobby3(data[7]);
                userHobby.setHobby4(data[8]);
                userHobby.setHobby5(data[9]);
                // 特定の件のみインサートするようにする
                Matcher matcher = pattern.matcher(line);
                if(matcher.find()) {
                    // 行数のインクリメント
                    i++;
                    sb.append("データ書き込み");
                    sb.append(i);
                    sb.append("件目");
                    log.info(sb.toString());
                    sb.setLength(0);
                    userInfoList.add(userInfo);
                    UserHobbyList.add(userHobby);
                }
            }

            // 条件に合致するユーザーをまとめて登録
            userDao.batchInsertUserInfo(userInfoList);
            // ユーザーに紐づく趣味もまとめて登録
            userDao.batchInsertUserHobby(UserHobbyList);

        } catch (Exception e) {
            log.info("csv read error", e);
        }
        // 対象情報取得
        UserInfo targetUserInfo = userDao.getTargetUserInfo();
        UserHobby targetUserHobby = userDao.getTargetUserHobby(targetUserInfo);
        UserMaster targetUserMaster = new UserMaster();
        
        targetUserMaster.setId(targetUserInfo.getId());
        targetUserMaster.setLastName(targetUserInfo.getLastName());
        targetUserMaster.setFirstName(targetUserInfo.getFirstName());
        targetUserMaster.setPrefectures(targetUserInfo.getPrefectures());
        targetUserMaster.setCity(targetUserInfo.getCity());
        targetUserMaster.setBloodType(targetUserInfo.getBloodType());
        targetUserMaster.setHobby1(targetUserHobby.getHobby1());
        targetUserMaster.setHobby2(targetUserHobby.getHobby2());
        targetUserMaster.setHobby3(targetUserHobby.getHobby3());
        targetUserMaster.setHobby4(targetUserHobby.getHobby4());
        targetUserMaster.setHobby5(targetUserHobby.getHobby5());
        
        // DBから検索する
        List<UserInfo> userInfoList = userDao.searchUserInfo();
        List<UserHobby> userHobbyList = userDao.searchUserHobby(targetUserHobby);
        
        List<UserMaster> userMasterList = new ArrayList<UserMaster>();
        
        for(int i = 0; i < userInfoList.size(); i++) {
            UserMaster userMaster = new UserMaster();
            userMaster.setId(userInfoList.get(i).getId());
            userMaster.setLastName(userInfoList.get(i).getLastName());
            userMaster.setFirstName(userInfoList.get(i).getFirstName());
            userMaster.setPrefectures(userInfoList.get(i).getPrefectures());
            userMaster.setCity(userInfoList.get(i).getCity());
            userMaster.setBloodType(userInfoList.get(i).getBloodType());
            for(int j = 0; j < userHobbyList.size(); j++) {
                if(userMaster.getId().equals(userHobbyList.get(j).getId())) {
                    userMaster.setHobby1(userHobbyList.get(j).getHobby1());
                    userMaster.setHobby2(userHobbyList.get(j).getHobby2());
                    userMaster.setHobby3(userHobbyList.get(j).getHobby3());
                    userMaster.setHobby4(userHobbyList.get(j).getHobby4());
                    userMaster.setHobby5(userHobbyList.get(j).getHobby5());
                    break;
                }
            }
            userMasterList.add(userMaster);
        }
        
        List<UserMaster> bloodMatchingUserList = new ArrayList<UserMaster>();
        // 同じ血液型ユーザー
        for(UserMaster user : userMasterList) {
            if(user.getBloodType().equals(targetUserMaster.getBloodType())) {
                bloodMatchingUserList.add(user);
            }
        }
        List<UserMaster> matchingUserList = new ArrayList<UserMaster>();
        // 趣味1に同じ趣味を持っているユーザー
        for(UserMaster user : bloodMatchingUserList) {
            if(user.getHobby1().equals(targetUserMaster.getHobby1()) || user.getHobby1().equals(targetUserMaster.getHobby2()) || user.getHobby1().equals(targetUserMaster.getHobby3()) || user.getHobby1().equals(targetUserMaster.getHobby4()) || user.getHobby1().equals(targetUserMaster.getHobby5())) {
                if(!matchingUserList.contains(user)) {
                    matchingUserList.add(user);
                }
            }
        }
        // 趣味2に同じ趣味を持っているユーザー
        for(UserMaster user : bloodMatchingUserList) {
            if(user.getHobby2().equals(targetUserMaster.getHobby1()) || user.getHobby2().equals(targetUserMaster.getHobby2()) || user.getHobby2().equals(targetUserMaster.getHobby3()) || user.getHobby2().equals(targetUserMaster.getHobby4()) || user.getHobby2().equals(targetUserMaster.getHobby5())) {
                if(!matchingUserList.contains(user)) {
                    matchingUserList.add(user);
                }
            }
        }
        // 趣味3に同じ趣味を持っているユーザー
        for(UserMaster user : bloodMatchingUserList) {
            if(user.getHobby3().equals(targetUserMaster.getHobby1()) || user.getHobby3().equals(targetUserMaster.getHobby2()) || user.getHobby3().equals(targetUserMaster.getHobby3()) || user.getHobby3().equals(targetUserMaster.getHobby4()) || user.getHobby3().equals(targetUserMaster.getHobby5())) {
                if(!matchingUserList.contains(user)) {
                    matchingUserList.add(user);
                }
            }
        }
        // 趣味4に同じ趣味を持っているユーザー
        for(UserMaster user : bloodMatchingUserList) {
            if(user.getHobby4().equals(targetUserMaster.getHobby1()) || user.getHobby4().equals(targetUserMaster.getHobby2()) || user.getHobby4().equals(targetUserMaster.getHobby3()) || user.getHobby4().equals(targetUserMaster.getHobby4()) || user.getHobby4().equals(targetUserMaster.getHobby5())) {
                if(!matchingUserList.contains(user)) {
                    matchingUserList.add(user);
                }
            }
        }
        // 趣味5に同じ趣味を持っているユーザー
        for(UserMaster user : bloodMatchingUserList) {
            if(user.getHobby5().equals(targetUserMaster.getHobby1()) || user.getHobby5().equals(targetUserMaster.getHobby2()) || user.getHobby5().equals(targetUserMaster.getHobby3()) || user.getHobby5().equals(targetUserMaster.getHobby4()) || user.getHobby5().equals(targetUserMaster.getHobby5())) {
                if(!matchingUserList.contains(user)) {
                    matchingUserList.add(user);
                }
            }
        }
        return matchingUserList;
    }

    public void logoutput(Map<String, String> map, StringBuilder sb, String lineSeparator) {
        //データ件数を表示
        sb.append("-------------------------------");
        sb.append(lineSeparator);
       
        for(Iterator<Map.Entry<String, String>> iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, String> entry = iterator.next();
            sb.append(entry.getKey());
            sb.append(entry.getValue());
            if(iterator.hasNext()) {
                sb.append(lineSeparator);
            }
        }
        log.info(sb.toString());
    }
    
    public void truncateTable() {
        userDao.truncateUserInfo();
        userDao.truncateUserHobby();
    }

    public Long referenceExecuteTime(String uuid) {
        
        Long result = null;
        if(resultMap.containsKey(uuid)) {
            result = resultMap.get(uuid);
        }
        
        return result;
    }
    
    public String referenceUuid() {
        
        String uuid = null;
        
        for(String key : resultMap.keySet()) {
            uuid = key;
        }
        
        return uuid;
    }

    private Boolean assertion(List<UserMaster> matchingUserList) {
        Boolean assertionResult = true;
        
        int count = userDao.searchCount();
        
        if(count != 10000) {
            return false;
        }
        
        if(matchingUserList.size() != 2072) {
            return false;
        }
        
        // CSVを取得・CSVファイルをDBに登録する
        //ファイル読み込みで使用するクラス
        InputStreamReader fs = null;
        BufferedReader br = null;
        List<String> csvFile = new ArrayList<String>();
        try {

            //読み込みファイルのインスタンス生成
            //ファイル名を指定する
            fs = new InputStreamReader(new FileInputStream(new File("data/assertionData.csv")), StandardCharsets.UTF_8);
            br = new BufferedReader(fs);

            //読み込み行
            String readLine;
            //1行ずつ読み込みを行う
            while ((readLine = br.readLine()) != null) {
                csvFile.add(readLine);
            }
        } catch (Exception e) {
            log.info("csv read error", e);
        } finally {
            try {
                br.close();
            } catch (Exception e) {
            }
        }
        for(String line : csvFile) {
            boolean exsits = false;
            UserMaster userMaster = new UserMaster();
            String[] data = line.split(",", -1);

            userMaster.setLastName(data[0]);
            userMaster.setFirstName(data[1]);
            userMaster.setPrefectures(data[2]);
            userMaster.setCity(data[3]);
            userMaster.setBloodType(data[4]);
            userMaster.setHobby1(data[5]);
            userMaster.setHobby2(data[6]);
            userMaster.setHobby3(data[7]);
            userMaster.setHobby4(data[8]);
            userMaster.setHobby5(data[9]);
            for(UserMaster user : matchingUserList) {
                if(user.toString().equals(userMaster.toString())) {
                    exsits = true;
                    break;
                }
            }
            if(!exsits) {
                assertionResult = false;
            }
        }
        truncateTable();
        return assertionResult;
    }

    public Boolean referenceAssertionResult(String uuid) {
        Boolean assertionResult = assertionResultMap.get(uuid);
        return assertionResult;
    }
}
