package es.upm.dit.fcon;


import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

// This is a simple application for detecting the correct processes using ZK. 
// Several instances of this code can be created. Each of them detects the 
// valid numbers.

// Two watchers are used:
// - cwatcher: wait until the session is created. 
// - watcherMember: notified when the number of members is updated

// the method process has to be created for implement Watcher. However
// this process should never be invoked, as the "this" watcher is used

//public class zkMember implements Watcher{
public class zkLeader implements Watcher{

	private static final int SESSION_TIMEOUT = 5000;

	private String myId;

	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"};

	private ZooKeeper zk;
	private Integer mutex = -1;
	private final String PATH = "/election";

	private Bank bank;

	public zkLeader(Bank bank) throws IOException, KeeperException, InterruptedException{

		/********************
		 * STARTING ZOOKEEPER
		 ********************/
		this.bank = bank;
		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				System.out.println("BANK STARTING");
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				System.out.println("BANK FINISHED STARTING\n");
				try {
					// Wait for creating the session. Use the object lock
					synchronized(mutex) {
						mutex.wait();
					}
					//zk.exists("/",false);
				} catch (Exception e) {
					// TODO: handle exception
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
		}

		// Leader Election
		leaderElection();

		//int pause = new Scanner(System.in).nextInt();
	}

	// Notified when the session is created
	private Watcher cWatcher = new Watcher() {
		public void process (WatchedEvent e) {
			System.out.println("Created session");
			System.out.println(e.toString());
			notify();
		}
	};

	// Notified when the number of children in /election is updated
	/*private Watcher  watcherMember = new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("------------------Watcher Client------------------\n");		
				try {
					System.out.println("        Update!!");
					List<String> list = zk.getChildren(PATH,  watcherMember); //this);
					printListMembers(list);
				} catch (Exception e) {
					System.out.println("Exception: wacherMember");
				}
			}
		};*/

	/**
	 * Leader Election
	 * @throws InterruptedException 
	 * @throws KeeperException
	 */
	public void leaderElection() throws KeeperException, InterruptedException{
		// If is the first client, then it should create the znode "/election"
		Stat stat = zk.exists(PATH, false);
		if(stat == null){
			System.out.println("Im the first bank, creating " + PATH + ".");
			//String bank = "/bank";
			String r = zk.create(PATH, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			System.out.println(r + " created.");
		}

		// Create znode z with path "BANK/n_" with both SEQUENCE and EPHEMERAL flags
		String childPath = PATH + "/n_";

		childPath = zk.create(childPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		myId = childPath;
		// Let C be the children of "BANK", and i be the sequence number of z;
		// Watch for changes on "BANK/n_j", where j is the smallest sequence
		// number such that j < i and n_j is a znode in C;
		List<String> children = zk.getChildren(PATH, false);

		String tmp = children.get(0);

		for(String s_children : children){
			if(tmp.compareTo(s_children) > 0)
				tmp = s_children;	
		}
		
		System.out.println("BANK "+myId+":: My leader proposal created. Path = " + childPath + ".");

		// i contains the smallest sequence number
		//String leader = PATH + "/n_" + i;
		String leader = PATH + "/" + tmp;
		Stat s = zk.exists(leader, true);

		// syso
		System.out.println("BANK "+myId+":: Leader is the owner of znode: " + leader);
		System.out.println("BANK "+myId+":: Leader id: " + s.getEphemeralOwner());

		//Comprobar si el bank es lider del banco y cambiar la variable isLeader
		if(childPath.equals(leader)) {
			bank.setLeader();
		}

		/*
		Let BANK be a path of choice of the application. To volunteer to be a leader:
		1.Create znode z with path "BANK/n_" with both SEQUENCE and EPHEMERAL flags;
		2.Let C be the children of "BANK", and i be the sequence number of z;
		3.Watch for changes on "BANK/n_j", where j is the smallest sequence number such that j < i and n_j is a znode in C;
		Upon receiving a notification of znode deletion:
		1.Let C be the new set of children of BANK;
		2.If z is the smallest node in C, then execute leader procedure;
		3.Otherwise, watch for changes on "BANK/n_j", where j is the smallest sequence number such that j < i and n_j is a znode in C;
		 */
	}

	public void newLeaderElection() throws KeeperException, InterruptedException{

		List<String> children = zk.getChildren(PATH, false);

		String tmp = children.get(0);

		for(String s : children){
			if(tmp.compareTo(s) > 0)
				tmp = s;	
		}
		myId = tmp;
		// i contains the smallest sequence number
		String leader = PATH + "/" + tmp;
		Stat s = zk.exists(leader, true);

		// syso
		System.out.println("BANK "+myId+":: Leader is the owner of znode: " + leader);
		System.out.println("BANK "+myId+":: Leader id: " + s.getEphemeralOwner());

		//Comprobar si el bank es lider del banco y cambiar la variable isLeader
		if(myId.equals(leader)) {
			bank.setLeader();
		}
	}


	@Override
	public void process(WatchedEvent event) {

		switch (event.getType()){

		case NodeChildrenChanged:
			System.out.println("BANK "+myId+" :: NodeChildrenChanged | ZNode: " + event.getPath());

			break;

		case NodeCreated:
			System.out.println("BANK "+myId+" :: NodeCreated | ZNode: " + event.getPath());
			break;

		case NodeDataChanged:
			System.out.println("BANK "+myId+" :: NodeDataChanged | ZNode: " + event.getPath());
			break;

		case NodeDeleted:
			System.out.println("BANK "+myId+" :: NodeDeleted | ZNode: " + event.getPath());
			System.out.println("BANK "+myId+" :: Leader was lost, newLeaderElection started.");
			try {
				newLeaderElection();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			break;

		case None:

			switch (event.getState()){

			case Disconnected:
				System.out.println("BANK "+myId+" :: Disconnected.");
				break;

			case Expired:
				System.out.println("BANK "+myId+" :: Expired.");
				break;

			case NoSyncConnected:
				System.out.println("BANK "+myId+" :: NoSyncConnected - Deprecated");
				break;

			case SyncConnected:
				System.out.println("BANK "+myId+" :: SyncConnected.");
				break;

			case Unknown:
				System.out.println("BANK "+myId+" :: Unknown - Deprecated");
				break;
			}

		}

	}

}
