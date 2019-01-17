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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper; 
import org.apache.zookeeper.data.Stat;

public class Bank implements Watcher{
	//VARIABLES BANK
	private BankClientDB bankClientDB;
	private boolean isLeader = false;

	//VARIABLES ZOOKEEPER
	private ZooKeeper zk = null;
	private final String BANKS_PATH = "/banks";
	//private final String CLIENT_PATH = "/client";
	private final String CHANGES_PATH = "/changes";
	private Integer mutex = -1;
	private static final int SESSION_TIMEOUT = 5000;
	private List<String> listChanges = null;

	private String myId;
	private zkLeader zkl;
	//VARIABLES SERVIDOR SOCKETS
	//static ServerSocket variable
	private static ServerSocket server;
	//socket server port on which it will listen
	private static int port = 3000;

	public Bank() {
		bankClientDB = new BankClientDB();
		try {
			zkl = new zkLeader(this);
			myId = zkl.getMyId();
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
				/*try {
					// Wait for creating the session. Use the object lock
					synchronized(mutex) {
						mutex.wait();
					}
					//zk.exists("/", false);
				} catch (Exception e) {
					// TODO: handle exception
				}*/
			}
		} catch (Exception e) {
			System.out.println("Exception in constructor");
		}

		// Add the process to the members in zookeeper

		if (zk != null) {
			// Create a folder for members and include this process/server
			try {
				// Create the /banks/changes znode
				// Create a folder, if it is not created
				Stat s = zk.exists(BANKS_PATH + CHANGES_PATH, false);
				if (s == null) {
					// Created the znode, if it is not created.
					zk.create(BANKS_PATH + CHANGES_PATH, new byte[0],
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
		boolean isCorrect = bankClientDB.createClient(client);
		if(isLeader) {
			try {
				String content = client.toString();
				content = "CREATE:"+content;
				ByteBuffer b = ByteBuffer.wrap(content.getBytes("UTF-8"));
				byte[] value = b.array();
				
				// Create a znode for registering as create
				zk.create(BANKS_PATH + CHANGES_PATH + "/create-", value,
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

			} catch (Exception e) {
				System.out.println(e.getMessage());
				System.out.println("Unexpected Exception process barrier");
			}
		}
		return isCorrect;
	}

	public BankClient readBankClient(int account) {
		// Handling locally. No need for distributing
		return bankClientDB.readClient(account);
	}

	public boolean updateBankClient(int account, int balance) {
		boolean isCorrect = bankClientDB.updateClient(account, balance);
		if(isLeader) {
			try {
				String content = "["+account+","+balance+"]";
				content = "UPDATE:"+content;
				ByteBuffer b = ByteBuffer.wrap(content.getBytes("UTF-8"));
				byte[] value = b.array();
	
				// Create a znode for registering as member and get my id
				zk.create(BANKS_PATH + CHANGES_PATH + "/update-", value,
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
	
			} catch (Exception e) {
				System.out.println("Unexpected Exception process barrier");
			}
		}
		return isCorrect;
	}

	public boolean deleteBankClient(int account) {
		boolean isCorrect = bankClientDB.deleteClient(account);
		if(isLeader) {
			try {
				String content = Integer.toString(account);
				content = "DELETE:"+content;
				ByteBuffer b = ByteBuffer.wrap(content.getBytes("UTF-8"));
				byte[] value = b.array();
	
				// Create a znode for registering as member and get my id
				zk.create(BANKS_PATH + CHANGES_PATH + "/delete-", value,
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
	
			} catch (Exception e) {
				System.out.println("Unexpected Exception process barrier");
			}
		}
		return isCorrect;
	}



	public void start() throws IOException, ClassNotFoundException {
		Stat s = null;
		String path = null;
		String data = "";

		while(true) {
			if(server == null && isLeader) {
				//create the socket server object
				server = new ServerSocket(port);
			}
			//keep listens indefinitely until receives 'exit' call or program terminates
			if(isLeader) {
				System.out.println("BANK CHANGES-LEADER "+myId+" :: Waiting for client request");

				//creating socket and waiting for client connection
				Socket socket = server.accept();
				//read from socket to ObjectInputStream object
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
				//convert ObjectInputStream object to String
				String message = (String) ois.readObject();
				System.out.println("BANK CHANGES-LEADER "+myId+" :: Message Received: " + message);

				String responseMessage="";
				if(message.contains("READ") && message.indexOf("READ")==0) {
					//READ:12
					System.out.println("BANK CHANGES-LEADER "+myId+" :: READ REQUEST FROM CLIENT.");
					BankClient bc = readBankClient(Integer.parseInt(message.substring(5)));
					if(bc == null) {
						responseMessage = "Null";
					} else {
						responseMessage = bc.toString();
					}
					

				} else if(message.contains("CREATE") && message.indexOf("CREATE")==0) {
					System.out.println("BANK CHANGES-LEADER "+myId+" :: CREATE REQUEST FROM CLIENT.");
					//CREATE:[12,Hola,123]
					//message.indexOf("[");
					String arrayString = message.substring(7);
					String[] array = arrayString.replace("[", "").replace("]", "").split(",");

					BankClient bc = new BankClient(Integer.parseInt(array[0]),array[1].replace(" ",""),Integer.parseInt(array[2].replace(" ","")));
					boolean createdBC = createBankClient(bc);
					responseMessage = ""+createdBC;

				} else if(message.contains("UPDATE") && message.indexOf("UPDATE")==0) {
					System.out.println("BANK CHANGES-LEADER "+myId+" :: UPDATE REQUEST FROM CLIENT.");
					//UPDATE:[12,122]
					//message.indexOf("[");
					String arrayString = message.substring(7);
					String[] array = arrayString.replace("[", "").replace("]", "").split(",");

					boolean updatedBC = updateBankClient(Integer.parseInt(array[0]),Integer.parseInt(array[1].replace(" ","")));
					responseMessage = ""+updatedBC;

				} else if(message.contains("DELETE") && message.indexOf("DELETE")==0) {
					System.out.println("LEADER :: DELETE REQUEST FROM CLIENT.");
					//DELETE:12
					boolean deletedBC = deleteBankClient(Integer.parseInt(message.substring(7)));
					responseMessage = ""+deletedBC;
				}

				//create ObjectOutputStream object
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				//write object to Socket
				oos.writeObject(responseMessage);

				//close resources
				ois.close();
				oos.close();
				socket.close();


				//terminate the server if client sends exit request
				///if(message.equalsIgnoreCase("exit")) break;
				//System.out.println("Shutting down Socket server!!");
				//close the ServerSocket object
				//server.close();

				//Al ser un banco follower escucha al lider si manda ordenes en el znode /bank/update
			} else {

				try {
					listChanges = zk.getChildren(BANKS_PATH + CHANGES_PATH, false, s); 
				} catch (Exception e) {
					System.out.println("Unexpected Exception process barrier");
					break;
				}

				if (listChanges.size() > 0) {
					try {
						//System.out.println(listProducts.get(0));
						path = BANKS_PATH + CHANGES_PATH+"/"+listChanges.get(0);
						//System.out.println(path);
						byte[] b = zk.getData(path, false, s);
						
						
						
						s = zk.exists(path, false);
						//System.out.println(s.getVersion());
						

						/*// Generate random delay
						Random rand = new Random();
						int r = rand.nextInt(10);
						// Loop for rand iterations
						for (int j = 0; j < r; j++) {
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {

							}
						}*/
						
						//HAY QUE ASEGURARSE DE QUE CUANDO LO LEAN TODOS SE BORRE
						zk.delete(path, s.getVersion());
						
						ByteBuffer buffer = ByteBuffer.wrap(b);
						data = new String(buffer.array(), "UTF-8");
						
						System.out.println("++++ BANK CHANGES "+myId+" :: Data: " + data + "; Path: " + path);
						
						//Dependiendo de que tipo sea se hace una cosa u otra	
						if(data.contains("CREATE") && data.indexOf("CREATE")==0) {
							System.out.println("BANK CHANGES "+myId+" :: CREATE REQUEST FROM LEADER.");
							//CREATE:[12,Hola,123]
							//message.indexOf("[");
							String arrayString = data.substring(7);
							String[] array = arrayString.replace("[", "").replace("]", "").split(",");

							BankClient bc = new BankClient(Integer.parseInt(array[0]),array[1].replace(" ", ""),Integer.parseInt(array[2].replace(" ","")));
							boolean createdBC = createBankClient(bc);
							
							System.out.println("BANK CHANGES "+myId+" :: CREATE = "+createdBC);

						} else if(data.contains("UPDATE") && data.indexOf("UPDATE")==0) {
							System.out.println("BANK CHANGES "+myId+" :: UPDATE REQUEST FROM LEADER.");
							//UPDATE:[12,122]
							//message.indexOf("[");
							String arrayString = data.substring(7);
							String[] array = arrayString.replace("[", "").replace("]", "").split(",");

							boolean updatedBC = updateBankClient(Integer.parseInt(array[0]),Integer.parseInt(array[1]));

							System.out.println("BANK CHANGES "+myId+" :: UPDATE = "+updatedBC);

						} else if(data.contains("DELETE") && data.indexOf("DELETE")==0) {
							System.out.println("BANK CHANGES "+myId+" :: DELETE REQUEST FROM LEADER.");
							//DELETE:12
							boolean deletedBC = deleteBankClient(Integer.parseInt(data.substring(7)));
							
							System.out.println("BANK CHANGES "+myId+" :: DELETE = "+deletedBC);
						}
						
					} catch (Exception e) {
						// The exception due to a race while getting the list of children, get data and delete. Another
						// consumer may have deleted a child while the previous access. Then, the exception is simply
						// implies that the data has not been produced.
						System.out.println("Exception when accessing the data in the node, maybe the znode is deleted");
						System.err.println(e);
						//e.printStackTrace();
						//break;
					}
				} else {
					try {
						zk.getChildren(BANKS_PATH + CHANGES_PATH, changesWatcher, s);
						synchronized(mutex) {
							mutex.wait();
						}
					} catch (Exception e) {
						System.out.println("Unexpected Exception process barrier");
						break;
					}
				}
			}
		}
		server.close();
	}

	private void printList (List<String> list) {
		//System.out.println("Size: " + list.size());
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}

	@Override
	public void process(WatchedEvent event) {
		//Stat s = null;

		System.out.println("------------------BANK WATCHER PROCESS ------------------");
		System.out.println("BANK CHANGES"+myId+" :: " + event.getType() + ", " + event.getPath());
		try {
			if (event.getPath() == null) {			
				//if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
				System.out.println("SyncConnected");
				/*synchronized (mutex) {
					mutex.notify();
				}*/
			}
			System.out.println("-----------------------------------------------");
		} catch (Exception e) {
			System.out.println("Unexpected Exception process");
		}

	}

	Watcher changesWatcher = new Watcher() {
		public void process(WatchedEvent event) { 

			Stat s = null;

			System.out.println("------------------Watcher Update ------------------");
			System.out.println("BANK "+myId+" :: Leader: " + event.getType() + ", " + event.getPath());
			try {
				if (event.getPath().equals(BANKS_PATH + CHANGES_PATH)) {
					//Obtener los Bank que han recibido 
					listChanges = zk.getChildren(BANKS_PATH + CHANGES_PATH, false, s);
					System.out.println("Remaining # changes: "+listChanges.size());
					printList(listChanges);

					synchronized (mutex) {
						mutex.notify();
					}
				} else {
					System.out.println("BANK "+myId+" :: Received a watcher with a path not expected");
				}

			} catch (Exception e) {
				System.out.println("Unexpected Exception process");
			}
		}
	};

}
