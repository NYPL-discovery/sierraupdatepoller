package org.nypl.harvester.sierra;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SierraItemUpdatePoller {

  public static void main(String[] args) {
    SpringApplication.run(SierraItemUpdatePoller.class, args);
  }
}
