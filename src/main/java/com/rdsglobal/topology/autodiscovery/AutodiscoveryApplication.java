package com.rdsglobal.topology.autodiscovery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class AutodiscoveryApplication {

	public static void main(String[] args) {
		SpringApplication.run(AutodiscoveryApplication.class, args);
	}

}
