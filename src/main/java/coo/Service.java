package coo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Crabit Coo
 */
@Configuration
@ComponentScan
@EnableAutoConfiguration
public class Service {

    public static void main(String[] args) {
        SpringApplication.run(Service.class, args);
    }

}
