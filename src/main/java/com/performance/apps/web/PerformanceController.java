package com.performance.apps.web;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.performance.domain.service.GoogleApiService;
import com.performance.domain.service.PerformanceService;

@Controller
public class PerformanceController {

    final static Logger log = LogManager.getLogger(PerformanceController.class);

    PerformanceService service;
    GoogleApiService googleService;

    public PerformanceController(PerformanceService service, GoogleApiService googleService) {
        this.service = service;
        this.googleService = googleService;
    }

    @GetMapping(value = "/index")
    public String index() {
        return "index";
    }

    @PostMapping(value = "/execute")
    public String execute(@RequestParam("measureFlag")String measureFlag, Model model) {

        String uuid = UUID.randomUUID().toString();
        String message = null;
        try {
            service.execute(uuid, measureFlag);
            message = "非同期にて処理を実行しています。処理結果は結果参照ボタンから取得してください。";
        } catch (TaskRejectedException e) {
            log.info("非同期処理実行中");
            message = "非同期にて処理を実行中です。処理時間は結果参照ボタンから取得してください。";
            uuid = service.referenceUuid();
        }

        model.addAttribute("message", message);
        model.addAttribute("uuid", uuid);

        return "processing";
    }

    @PostMapping(value = "/reference")
    public String reference(@RequestParam("uuid")String uuid, Model model) {

        Long executeTime = service.referenceExecuteTime(uuid);
        Boolean assertionResult = service.referenceAssertionResult(uuid);

        String message = null;
        if(executeTime == null) {
            message = "まだ実行中みたいです。";
        }
        String resultMessage = null;
        if(assertionResult != null) {
            if(assertionResult) {
                resultMessage = "OK";
            } else {
                resultMessage = "NG";
            }
        }
        
        model.addAttribute("executeTime", executeTime);
        model.addAttribute("resultMessage", resultMessage);
        model.addAttribute("message", message);

        return "result";
    }
}
