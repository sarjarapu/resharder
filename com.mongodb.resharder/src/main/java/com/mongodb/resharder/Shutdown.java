package com.mongodb.resharder;

import java.io.IOException;

public class Shutdown implements Runnable {

	@Override
	public void run() {
			try {
				Resharder.shutdown();
			} catch (InterruptedException | IOException e) {
				// if we are here then all hope is probably lost
			}
	}

}
