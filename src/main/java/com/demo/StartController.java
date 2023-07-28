package com.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/start")
public class StartController {

    @Autowired
    private DatabaseConfig databaseConfig;

    private  final RestApi restApi;

    private final OrderBookManager orderBookManager;

    public StartController(RestApi restApi, OrderBookManager orderBookManager) {
        this.restApi = restApi;
        this.orderBookManager = orderBookManager;
    }

    @GetMapping
    public String startApp(@RequestParam String eventSymbol,
                           @RequestParam int limitCount,
                           @RequestParam int updateSpeed,
                           @RequestParam boolean update_parameter) {
        AppConfig appConfig = new AppConfig();
        appConfig.setEventSymbol(eventSymbol);
        appConfig.setLimitCount(limitCount);
        appConfig.setUpdateSpeed(updateSpeed);
        try {
            restApi.startRestApi(databaseConfig, appConfig, update_parameter);
            orderBookManager.startOrderBookManage(databaseConfig, appConfig, update_parameter);
            return "Systems started successfully.";
        }
        catch (Exception e){
            System.out.println(e.getMessage());
            return "Something wrong. Check logs";
        }
    }
}
