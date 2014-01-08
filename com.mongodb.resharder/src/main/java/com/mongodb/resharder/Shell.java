package com.mongodb.resharder;

import asg.cliche.Command;
import asg.cliche.ShellFactory;

import java.io.IOException;
import java.net.UnknownHostException;

public class Shell {

	@Command
	public void set(String prop, String val) {
		val = val.replace("hashed", "\"hashed\"");
		System.out.println(Config.setProperty(prop, val));
	}

	@Command
	public void help() {
		System.out.println("Available commands are:");
		System.out.println("set <property> <value>");
		System.out.println("\t source <hostname:port>\t\t\t- address of the source server");
		System.out.println("\t target <hostname:port>\t\t\t- address of the target server");
		System.out.println("\t log <hostname:port>\t\t\t- address of the log server");
		System.out.println("\t namespace <database.collection>\t- source collection namespace");
		System.out.println("\t targetns <database.collection>\t\t- target collection namespace");
		System.out.println("\t readSecondary <true/false>\t\t- use delayed secondaries for copy");
		System.out.println("\t reshard <true/false>\t\t\t- shard target collection on copy");
		System.out.println("\t key <shard key>\t\t\t- new shard key for clone");
		System.out.println("\t readBatch <integer>\t\t\t- number of documents to read per batch");
		System.out.println("\t writeBatch <integer>\t\t\t- number of documents to write per batch");
		System.out.println();
		System.out.println("conf\t\t\t\t\t\t- print current configuration");
		System.out.println("execute\t\t\t\t\t\t- run current configuration");
		System.out.println("shutdown\t\t\t\t\t- stop source/target synchronization");
	}

	@Command
	public void conf() {
		Config.print();
	}

	@Command
	public void execute() throws UnknownHostException {
		Config.init(null);
		
		if (Config.validate()) {
			System.out.println("Executing...");
			
			Launcher._tp.execute(new Resharder());
		} else {
			System.out.println("Invalid Configuration");
		}
	}
	
	@Command
	public void shutdown() throws InterruptedException, IOException {
		if (OpLogWriter.isActive()) {
			System.out.println("Cloning in progress, try again later.");
		} else
			Resharder.shutdown();
	}

	public static void runCLI() throws IOException {
		ShellFactory.createConsoleShell("resharder", "MongoDB Resharder", new Shell()).commandLoop();
	}
}
