package com.performance.domain.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


@Service
public class GoogleApiService {

    @Value("${app.google.oauth2-url}")
    private String OAUTH2_URL;
    @Value("${app.google.api-url}")
    private String API_URL;
    @Value("${app.google.sheet-id}")
    private String SHEET_ID;
    @Value("${app.google.client-id}")
    private String CLIENT_ID;
    @Value("${app.google.client-secret}")
    private String CLIENT_SECRET;
    @Value("${app.google.refresh-token}")
    private String REFRESH_TOKEN;
    @Value("${app.execute.user}")
    private String EXECUTE_USER;
    
    RestTemplate restTemplate;
    ObjectMapper mapper;
    
    public GoogleApiService(RestTemplateBuilder restTemplateBuilder, ObjectMapper mapper) {
        this.restTemplate = restTemplateBuilder.build();
        this.mapper = mapper;
    }
    
    public enum Column {
        B("B", 1),
        C("C", 2),
        D("D", 3),
        E("E", 4),
        F("F", 5),
        G("G", 6),
        H("H", 7),
        I("I", 8);
        
        private String columnId;
        private int columnNumber;
        
        private Column(String columnId, int columnNumber) {
            this.columnId = columnId;
            this.columnNumber = columnNumber;
        }
        
        public String getColumnId() {
            return columnId;
        }

        public int getColumnNumber() {
            return columnNumber;
        }
        
        public static Column getByColumnNumber(int columnNumber) {

            for(Column column : Column.values()) {
                if( column.getColumnNumber() == columnNumber ) {
                    return column;
                }
            }
            return null;
        }
    }
    
    public void execute(Long executeTime) throws ClientProtocolException, IOException, URISyntaxException {

        // Googleの認証情報を取得
        GoogleOauthResponse googleOauth = getGoogleOauth();

        // 対象ユーザーの列情報を取得
        Column targetColumn = getTargetColumun(googleOauth);

        // 更新対象の行番号を取得
        String targetRowCount = getTargetRowCount(googleOauth, targetColumn);

        updateSpreadSheet(googleOauth, targetColumn, targetRowCount, executeTime);
    }

    private void updateSpreadSheet(GoogleOauthResponse googleOauth, Column targetColumn, String targetRowCount, Long executeTime) throws URISyntaxException {

        Map<String, List<Long[]>> valueMap = new HashMap<String, List<Long[]>>();
        Long[] updateValue = new Long[]{executeTime};
        List<Long[]> updateValues = new ArrayList<Long[]>();
        updateValues.add(updateValue);
        valueMap.put("values", updateValues);
        String postUrl = API_URL + SHEET_ID + "/values/" + targetColumn.getColumnId() + targetRowCount +":append?valueInputOption=USER_ENTERED";
        RequestEntity<Map<String, List<Long[]>>> request = RequestEntity.post(new URI(postUrl))
                .header("Authorization", googleOauth.getTokenType() + " " + googleOauth.getAccessToken())
                .contentType(MediaType.APPLICATION_JSON)
                .body(valueMap);
        restTemplate.exchange(request,GoogleSpreadSheetRowResponse.class);
    }

    private String getTargetRowCount(GoogleOauthResponse googleOauth, Column targetColumn) throws URISyntaxException, JsonMappingException, JsonProcessingException {

        String getUrl = API_URL + SHEET_ID + "/values/シート1!" + targetColumn.getColumnId() + "4:" + targetColumn.getColumnId() +"2000000";
        GoogleSpreadSheetRowResponse response = null;
        RequestEntity<Void> request = RequestEntity.get(new URI(getUrl)).header("Authorization", googleOauth.getTokenType() + " " + googleOauth.getAccessToken()).build();
        ResponseEntity<GoogleSpreadSheetRowResponse> responseEntity = restTemplate.exchange(request, GoogleSpreadSheetRowResponse.class);
        if(responseEntity.getStatusCodeValue() == HttpStatus.OK.value()) {
            response = responseEntity.getBody();
        }
        int intTargetRowCount = 4;
        if(response.getValues() != null) {
            intTargetRowCount = response.getValues().get(0).length + 4;
        }
        
        String targetRowCount = String.valueOf(intTargetRowCount);
        return targetRowCount;
    }

    private Column getTargetColumun(GoogleOauthResponse googleOauth) throws URISyntaxException, JsonMappingException, JsonProcessingException {

        String getUrl = API_URL + SHEET_ID + "/values/シート1!B1:I1";
        GoogleSpreadSheetRowResponse response = null;
        RequestEntity<Void> request = RequestEntity.get(new URI(getUrl)).header("Authorization", googleOauth.getTokenType() + " " + googleOauth.getAccessToken()).build();
        ResponseEntity<GoogleSpreadSheetRowResponse> responseEntity = restTemplate.exchange(request, GoogleSpreadSheetRowResponse.class);
        if(responseEntity.getStatusCodeValue() == HttpStatus.OK.value()) {
            response = responseEntity.getBody();
        }
        int targetColumnCount = 1;
        for(String user : response.getValues().get(0)) {
            if(!EXECUTE_USER.equals(user)) {
                targetColumnCount++;
            } else {
                break;
            }
        }
        Column targetColumn = Column.getByColumnNumber(targetColumnCount);
        return targetColumn;
    }

    private GoogleOauthResponse getGoogleOauth() throws URISyntaxException, JsonMappingException, JsonProcessingException {

        MultiValueMap<String, String> requestMap = new LinkedMultiValueMap<>();
        requestMap.add("client_id", CLIENT_ID); //コンソールにAPI認証情報のクライアントID
        requestMap.add("client_secret", CLIENT_SECRET); //コンソールにAPI認証情報のクライアントシークレット
        requestMap.add("refresh_token", REFRESH_TOKEN);
        requestMap.add("grant_type", "refresh_token"); //固定

        RequestEntity<MultiValueMap<String, String>> request = RequestEntity.post(new URI(OAUTH2_URL))
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(requestMap);

        ResponseEntity<GoogleOauthResponse> responseEntity = restTemplate.exchange(request, GoogleOauthResponse.class);

        GoogleOauthResponse googleOauth = null;
        if(responseEntity.getStatusCodeValue() == HttpStatus.OK.value()) {
            googleOauth = responseEntity.getBody();
        }
        return googleOauth;
    }
}
