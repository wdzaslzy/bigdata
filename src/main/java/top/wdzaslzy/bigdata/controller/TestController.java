package top.wdzaslzy.bigdata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import top.wdzaslzy.bigdata.kafka.KafkaSender;

/**
 * @author lizy
 **/
@Controller
@RequestMapping("/v1/")
public class TestController {
    @ResponseBody
    @RequestMapping("/test")
    public String test(){
        return "success";
    }

}
