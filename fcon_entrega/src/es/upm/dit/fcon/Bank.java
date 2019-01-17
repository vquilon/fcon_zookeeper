package es.upm.dit.fcon;
/**
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.Address;
 **/

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper; 
import org.apache.zookeeper.data.Stat;

public class Bank implements Watcher{

	private BankClientDB bankClientDB;
	private boolean isLeader = false;

	private ZooKeeper zk = null;

	private final String BANK_PATH = "/bank";
	private final String CLIENT_PATH = "/client";
	private final String UPDATE_PATH = "/update";
	private Integer mutex = -1;
	private static final int SESSION_TIMEOUT = 5000;

	private List<String> listUpdates = null;

	public Bank() {
		bankClientDB = new BankClientDB();
		try {
			new zkLeader(this);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// This is static. A list of zookeeper can be provided for decide where to connect
		String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"};

		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create the session
		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, this);
				// We initialize the mutex Integer just after creating ZK.
				try {
					// Wait for creating the session. Use the object lock
					synchronized(mutex) {
						mutex.wait();
					}
					//zk.exists("/", false);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} catch (Exception e) {
			System.out.println("Exception in constructor");
		}

		// Add the process to the members in zookeeper

		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create the /members znode
				// Create a folder, if it is not created
				Stat s = zk.exists(BANK_PATH, false);
				if (s == null) {
					// Created the znode, if it is not created.
					zk.create(BANK_PATH, new byte[0],
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}

			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}


	}

	public boolean isLeader() {
		return isLeader;
	}

	public void setLeader() {
		isLeader = true;;
	}

	public void close() throws InterruptedException {
		zk.close();
	}

	public boolean createBankClient(BankClient client) {
		//Crear cliente nodo zookeeper

		return bankClientDB.createClient(client);
	}

	public BankClient readBankClient(int account) {
		// Handling locally. No need for distributing
		return bankClientDB.readClient(account);
	}

	public boolean updateBankClient(int account, int balance) {
		boolean isCorrect = bankClientDB.updateClient(account, balance);
		//Need to send message to make consensus

		return isCorrect;
	}

	public boolean deleteBankClient(int account) {

		return bankClientDB.deleteClient(account);
	}

	/*Watcher clientWatcher = new Watcher() {
		public void process(WatchedEvent event) { 

			Stat s = null;

			System.out.println("------------------Watcher Product ------------------");
			System.out.println("Client: " + event.getType() + ", " + event.getPath());
			try {
				if (event.getPath().equals(BANK_PATH + CLIENT_PATH)) {
					listUpdates = zk.getChildren(BANK_PATH + CLIENT_PATH, false, s);
					printList(listUpdates);

					synchronized (mutex) {
						mutex.notify();
					}
				} else {
					System.out.println("Product: Received a watcher with a path not expected");
				}

			} catch (Exception e) {
				System.out.println("Unexpected Exception process");
			}
		}
	};*/

	private void printList (List<String> list) {
		System.out.println("Size: " + list.size());
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}

	public void start() {
		Stat s = null;
		String path = null;
		String data = "";

		while(true) {
			//Tiene que manejar los watchers de los clientes /bank/client
			if(this.isLeader) {
				try {
					listUpdates = zk.getChildren(BANK_PATH + CLIENT_PATH, false, s); 
				} catch (Exception e) {
					System.out.println("Unexpected Exception process barrier");
					break;
				}
				/*
				 * 
				 * FALTA COMPROBAR EL WATCHER Y AQUI EL QUE RECIBE LOS DATOS*/

				if (listUpdates.size() > 0) {
					try {
						//System.out.println(listUpdates.get(0));
						path = BANK_PATH + CLIENT_PATH+"/"+listUpdates.get(0);
						//System.out.println(path);
						byte[] b = zk.getData(path, false, s);
						s = zk.exists(path, false);
						//System.out.println(s.getVersion());
						zk.delete(path, s.getVersion());

						// Generate random delay
						Random rand = new Random();
						int r = rand.nextInt(10);
						// Loop for rand iterations
						for (int j = 0; j < r; j++) {
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {

							}
						}

						ByteBuffer buffer = ByteBuffer.wrap(b);
						data = new String(buffer.array(), "ASCII");
						System.out.println("++++ Leader Recieve. Data: " + data + "; Path: " + path);
						
						//Guardar en su base de datos y crear un nodo en el que están escuchando el 
						//resto de bancos followers
						
						
					} catch (Exception e) {
						// The exception due to a race while getting the list of children, get data and delete. Another
						// consumer may have deleted a child while the previous access. Then, the exception is simply
						// implies that the data has not been produced.
						System.out.println("Exception when accessing the data");
						//System.err.println(e);
						//e.printStackTrace();
						//break;
					}
				} else {
					try {
						zk.getChildren(BANK_PATH + CLIENT_PATH, clientWatcher, s);
						synchronized(mutex) {
							mutex.wait();
						}
					} catch (Exception e) {
						System.out.println("Unexpected Exception process barrier");
						break;
					}
				}			



				//Al no ser un lider escucha al lider si manda ordenes en el znode /bank/update
			} else {
				
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		Stat s = null;

		System.out.println("------------------Watcher PROCESS ------------------");
		System.out.println("Member: " + event.getType() + ", " + event.getPath());
		try {
			if (event.getPath() == null) {			
				//if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
				System.out.println("SyncConnected");
				synchronized (mutex) {
					mutex.notify();
				}
			}
			System.out.println("-----------------------------------------------");
		} catch (Exception e) {
			System.out.println("Unexpected Exception process");
		}

	}


}
