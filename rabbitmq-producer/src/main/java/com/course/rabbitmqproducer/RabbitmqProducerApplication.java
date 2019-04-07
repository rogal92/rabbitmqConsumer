package com.course.rabbitmqproducer;

import com.course.rabbitmqproducer.entity.Employee;
import com.course.rabbitmqproducer.producer.EmployeeJsonProducer;
import com.course.rabbitmqproducer.producer.HelloRabbitProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SpringBootApplication
//@EnableScheduling
public class RabbitmqProducerApplication implements CommandLineRunner {

//	@Autowired
//	private HelloRabbitProducer rabbitProducer;

	@Autowired
	private EmployeeJsonProducer employeeJsonProducer;

	public static void main(String[] args) {
		SpringApplication.run(RabbitmqProducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		rabbitProducer.sendHello("Bartosz " + Math.random());
		IntStream.range(0,5)
				.forEach(i -> {
					Employee emp = new Employee("emp" + i, "Employee " + i, new Date());
					try {
						employeeJsonProducer.sendMessage(emp);
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
				});
	}
}
