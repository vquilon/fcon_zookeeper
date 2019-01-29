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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
//import java.util.Random;
import java.util.Set;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
//import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Bank implements Watcher{
	//VARIABLES BANK
	private String ip = "localhost";
	private BankClientDB bankClientDB;
	private boolean isLeader = false;

	//VARIABLES ZOOKEEPER
	private ZooKeeper zk = null;
	private final String ELECTION_PATH="/election";
	private final String BANKS_PATH = "/banks";
	private final String DB_PATH = "/db";
	private final String CHANGES_PATH = "/changes";
	private final String ACKS_PATH = "/acks";
	private Integer mutex = -1;
	private List<String> listChanges = null;

	private boolean flagNuevo = true;

	private String myId;
	private zkLeader zkl;

	//VARIABLES SERVIDOR SOCKETS
	//socket server port on which it will listen
	//static ServerSocket variable
	private static ServerSocket server = null;
	private static ServerSocket serverDB = null;
	private static int port = 3000;
	private static int portDB = 4000;
	public Bank(String ip) {
		this.ip = ip;

		bankClientDB = new BankClientDB();
		try {
			zkl = new zkLeader(this);
			myId = zkl.getMyId();
			zk = zkl.getZK();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


		if (zk != null) {
			// Create a folder for banks/changes
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

			try {
				// Create the /banks/db
				// Create a folder, if it is not created
				Stat sBD = zk.exists(BANKS_PATH + DB_PATH, false);
				if (sBD == null) {
					// Created the znode, if it is not created.
					zk.create(BANKS_PATH + DB_PATH, new byte[0],
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

			try {
				// Create the /banks/acks
				// Create a folder, if it is not created
				Stat sBD = zk.exists(BANKS_PATH + ACKS_PATH, false);
				if (sBD == null) {
					// Created the znode, if it is not created.
					zk.create(BANKS_PATH + ACKS_PATH, new byte[0],
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
	public Integer getMutex() {
		return mutex;
	}

	public boolean createBankClient(BankClient client) {
		boolean isCorrect = bankClientDB.createClient(client);
		List<String> banksFollowers = new ArrayList<String>();
		if(isLeader) {
			try {
				banksFollowers = zk.getChildren(BANKS_PATH+ELECTION_PATH, false);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(banksFollowers.size()>1) {
				try {
					String content = client.toString();
					content = "CREATE:"+content;
					ByteBuffer b = ByteBuffer.wrap(content.getBytes("UTF-8"));
					byte[] value = b.array();

					// Create a znode for registering as create
					System.out.println("BANK-LEADER CHANGES "+myId+" :: New CREATED_znode for the followers");
					String change = zk.create(BANKS_PATH + CHANGES_PATH + "/create-", value,
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

					// Created the znode acks, if it is not created.
					//Se crea con la lista de nodos que había en ese momento online

					String listStringFollowerAllowed = "";
					for(String follower : banksFollowers) {
						//n_00001 -> 00001,00002,00004
						if(!follower.equals(myId)) {
							listStringFollowerAllowed = listStringFollowerAllowed+(listStringFollowerAllowed.equals("") ? follower.substring(2) : ","+follower.substring(2));
						}
					}
					byte[] listBanksString = ByteBuffer.wrap(listStringFollowerAllowed.getBytes("UTF-8")).array();



					String ackChange = zk.create(change.replace(CHANGES_PATH, ACKS_PATH), listBanksString,
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					//condicion de carrera, crean el ack pero el lider no estaba escuchando
					zk.getChildren(ackChange, acksWatcher, null);
				} catch (Exception e) {
					System.out.println(e.getMessage());
					System.out.println("Unexpected Exception process barrier");
				}
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

		List<String> banksFollowers = new ArrayList<String>();
		if(isLeader) {
			try {
				banksFollowers = zk.getChildren(BANKS_PATH+ELECTION_PATH, false);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(banksFollowers.size()>1) {
				try {
					String content = "["+account+","+balance+"]";
					content = "UPDATE:"+content;
					ByteBuffer b = ByteBuffer.wrap(content.getBytes("UTF-8"));
					byte[] value = b.array();

					// Create a znode for registering as member and get my id
					System.out.println("BANK-LEADER CHANGES "+myId+" :: New UPDATED_znode for the followers");
					String change = zk.create(BANKS_PATH + CHANGES_PATH + "/update-", value,
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

					String listStringFollowerAllowed = "";
					for(String follower : banksFollowers) {
						//n_00001 -> 00001,00002,00004
						if(!follower.equals(myId)) {
							listStringFollowerAllowed = listStringFollowerAllowed+(listStringFollowerAllowed.equals("") ? follower.substring(2) : ","+follower.substring(2));
						}
					}
					byte[] listBanksString = ByteBuffer.wrap(listStringFollowerAllowed.getBytes("UTF-8")).array();
					// Created the znode acks, if it is not created.
					String ackChange = zk.create(change.replace(CHANGES_PATH, ACKS_PATH), listBanksString,
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					zk.getChildren(ackChange, acksWatcher, null);

				} catch (Exception e) {
					System.out.println("Unexpected Exception process barrier");
				}
			}
		}
		return isCorrect;
	}

	public boolean deleteBankClient(int account) {
		boolean isCorrect = bankClientDB.deleteClient(account);
		List<String> banksFollowers = new ArrayList<String>();
		if(isLeader) {
			try {
				banksFollowers = zk.getChildren(BANKS_PATH+ELECTION_PATH, false);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(banksFollowers.size()>1) {
				try {
					String content = Integer.toString(account);
					content = "DELETE:"+content;
					ByteBuffer b = ByteBuffer.wrap(content.getBytes("UTF-8"));
					byte[] value = b.array();

					// Create a znode for registering as member and get my id
					System.out.println("BANK-LEADER CHANGES "+myId+" :: New DELETED_znode for the followers");
					String change = zk.create(BANKS_PATH + CHANGES_PATH + "/delete-", value,
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

					String listStringFollowerAllowed = "";
					for(String follower : banksFollowers) {
						//n_00001 -> 00001,00002,00004
						if(!follower.equals(myId)) {
							listStringFollowerAllowed = listStringFollowerAllowed+(listStringFollowerAllowed.equals("") ? follower.substring(2) : ","+follower.substring(2));
						}
					}
					byte[] listBanksString = ByteBuffer.wrap(listStringFollowerAllowed.getBytes("UTF-8")).array();

					// Created the znode acks, if it is not created.
					String ackChange = zk.create(change.replace(CHANGES_PATH, ACKS_PATH), listBanksString,
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

					zk.getChildren(ackChange, acksWatcher, null);

				} catch (Exception e) {
					System.out.println("Unexpected Exception process barrier");
				}
			}
		}
		return isCorrect;
	}



	public void start() throws IOException, ClassNotFoundException {
		Stat s = null;
		String path = null;
		String data = "";
		boolean firstimeLeader = true;

		while(true) {
			if(server == null && isLeader) {
				//create the socket server object
				server = new ServerSocket(port);
			}
			//keep listens indefinitely until receives 'exit' call or program terminates
			if(isLeader) {
				if(firstimeLeader) {
					firstimeLeader = false;
					//Lanza el watcher que maneja los cambios en la base de datos
					try {
						zk.getChildren(BANKS_PATH + DB_PATH, dbWatcher); 
					} catch (Exception e) {
						System.out.println("The path not exists");
						System.out.println(e);
					}

					//Ver que los acks esten todos y borrar los cambios y acks que esten completos si no crear un watcher y borrar su ack si estuviera
					try {
						List<String> banks = new ArrayList<String>();
						try {
							banks = zk.getChildren(BANKS_PATH+ELECTION_PATH, false);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
						List<String> listChanges = new ArrayList<String>();
						try {
							listChanges = zk.getChildren(BANKS_PATH+CHANGES_PATH, false);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
						if(listChanges.size()>0) {
							//For para todos los changes
							for(String change : listChanges) {
								//Lista de acks enviados por los bancos znodes a un cambio
								List<String> listIndACKs = new ArrayList<String>();
								try {
									//Obtiene la lista de los acks
									listIndACKs = zk.getChildren(BANKS_PATH+ACKS_PATH + "/" + change, false, s);
								} catch (KeeperException | InterruptedException e) { 
									e.printStackTrace();
								}


								s = zk.exists(BANKS_PATH+ACKS_PATH + "/" + change, false);
								byte[] b = zk.getData(BANKS_PATH+ACKS_PATH + "/" + change, false, s);


								ByteBuffer buffer = ByteBuffer.wrap(b);
								String listIdStringBanksACKChanges = new String(buffer.array(), "UTF-8");

								String[] listIdBanksACKChanges = listIdStringBanksACKChanges.split(",");

								//Compara la lista de ACKS que hay con la lista de followers que enviaron ACKS 
								//guardado dentro del znode .../acks/update-0001 menos el mismo

								//Numero de Followers admitidos que hay para enviar ACK en este cambio
								int numberOfFollowers = listIdBanksACKChanges.length;
								//Numero de ACKs solo se muestran para los nodos en linea que sean Followers y Lider si ha habido reeleccion
								int numberOfACKs = listIndACKs.size();
								for(String oneFollower : listIdBanksACKChanges) {

									//El lider borra sus acks
									if(("n_"+oneFollower).equals(myId)) {
										//Quiere decir que se decrementa ya que el propio lider esta como ack
										numberOfFollowers = numberOfFollowers-1;
										numberOfACKs = numberOfACKs - 1;
										//Primero borra su ack enviado como follower antes de ser Lider
										s = zk.exists(BANKS_PATH+ACKS_PATH+"/"+change+"/ack-"+myId, false);
										if(s!=null) {
											zk.delete(BANKS_PATH+ACKS_PATH+"/"+change+"/ack-"+myId, s.getVersion());
										}
									}
									//OJO si en la lista de los follower que deberian mandar ACKs si no esta online(/election)

									//Se debe elminar /changes y /acks, hay que ajustar el numero de followers
									//-Esta en la lista del znode de acks/update-001
									//-No esta en linea por lo que No esta en ACKs(Ephemeral)
									if(!banks.contains(("n_"+oneFollower)) && !listIndACKs.contains(("n_"+oneFollower))) {
										numberOfFollowers = numberOfFollowers - 1;
									}

									//ESCENARIO #123129: Un Follower conectado con el lider anterior recibe cambios pero antes de efectuarlos
									//el lider muere, el continua con los cambios y envia los acks(ya el nuevo lider -que puede ser el- se encargara
									//de gestionar esos acks y borrar los znodes)

									//No se debe eliminar /changes y /acks
									//-Esta en la lista del zonde de acks/update-001
									//-Esta en linea
									//-No esta en ACKs (en este momento pero eventualmente lo enviará)

								}
								//AHORA SI, comprueba que el numero de ACKs sea el mismo que el que deberia ser(todos los followers enviaron ACK)
								if (numberOfACKs == numberOfFollowers) {
									for(String ack_n : listIndACKs) {
										try {

											byte[] bACK = zk.getData(BANKS_PATH+ACKS_PATH+"/"+change + "/" + ack_n, false, s);

											s = zk.exists(BANKS_PATH+ACKS_PATH+"/"+change + "/" + ack_n, false);
											//zk.delete(path, s.getVersion());

											ByteBuffer bufferACK = ByteBuffer.wrap(bACK);
											String dataACK = new String(bufferACK.array(), "UTF-8");
											//El lider borra todos los ACKS para borrar después el contenedor de ACKs de ese change
											if(dataACK.equals("received")){
												zk.delete(BANKS_PATH+ACKS_PATH+"/"+change +"/"+ack_n, s.getVersion());
											}
										} catch (Exception e) {
											e.printStackTrace();
										}
									}

									try {
										//SE BORRA EL ZNODE DE LOS ACKS
										s = zk.exists(BANKS_PATH+ACKS_PATH+"/"+change, false);
										zk.delete(BANKS_PATH+ACKS_PATH+"/"+change, s.getVersion());

										//SE BORRA EL ZNODE DE LOS Changes
										s = zk.exists(BANKS_PATH+CHANGES_PATH+"/"+change, false);
										zk.delete(BANKS_PATH+CHANGES_PATH+"/"+change, s.getVersion());

									} catch (Exception e) {
										e.printStackTrace();
									}
								} else {
									//Lanza el Watcher debido a que aun no han contestado todos los integrantes, 
									//asi cuando conteste otro se lance el watcher

									//Crea el watcher de acks debido a que hay un follower que aun esta guardando los cambios
									//y este ha sido reelegido como lider
									zk.getChildren(BANKS_PATH+ACKS_PATH+"/"+change, acksWatcher);
								}
							}
						}

					} catch (Exception e) {
						System.out.println("Unexpected Exception process");
					}


				}

				System.out.println("BANK CHANGES-LEADER "+myId+" :: Waiting for client request");

				try {
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
						String arrayString = message.substring(7);
						int accN = Integer.parseInt(arrayString.substring(arrayString.indexOf("ACCN:")+5,arrayString.indexOf("BAL:")));
						int bal = Integer.parseInt(arrayString.substring(arrayString.indexOf("BAL:")+4,arrayString.indexOf("NAME:")));
						String name = arrayString.substring(arrayString.indexOf("NAME:")+5);

						BankClient bc = new BankClient(accN, name, bal);
						boolean createdBC = createBankClient(bc);
						responseMessage = ""+createdBC;

					} else if(message.contains("UPDATE") && message.indexOf("UPDATE")==0) {
						System.out.println("BANK CHANGES-LEADER "+myId+" :: UPDATE REQUEST FROM CLIENT.");
						//UPDATE:[12,122]
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
				} catch(Exception e) {
					e.printStackTrace();
				}



				//terminate the server if client sends exit request
				///if(message.equalsIgnoreCase("exit")) break;
				//System.out.println("Shutting down Socket server!!");
				//close the ServerSocket object
				//server.close();

				//Al ser un banco follower escucha al lider si manda ordenes en el znode /bank/changes
			} else {

				//Solicitar la base de datos por Sockets
				if(flagNuevo) {
					flagNuevo = false;
					String dirIp = ip;
					try {

						String content = dirIp+":"+portDB;
						ByteBuffer b = ByteBuffer.wrap(content.getBytes("UTF-8"));
						byte[] value = b.array();

						zk.create(BANKS_PATH + DB_PATH + "/req-", value,
								Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

					} catch (Exception e) {
						System.out.println(e.getMessage());
						System.out.println("Unexpected Exception process barrier");
					}

					if(serverDB == null) {
						//create the socket server object
						serverDB = new ServerSocket(portDB);
					}
					boolean remainingData = true;
					//Recibir mensajes del lider mientras aun no se haya recibido entera
					while(remainingData) {
						System.out.println("BANK DB "+myId+" :: Waiting for reacieve DB from leader");
						//creating socket and waiting for client connection or 60 seconds
						serverDB.setSoTimeout(60000);
						try {

							System.out.println("Esperando respuesta");
							Socket socket=serverDB.accept();

							//read from socket to ObjectInputStream object
							ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
							//convert ObjectInputStream object to String
							String message = (String) ois.readObject();
							System.out.println("BANK DB "+myId+" :: Message Length Received: " + message.length());

							//COMPLETAR CON EL ALMACENAMIENTO DE LA BASE DE DATOS ENTERA
							//[1, Pepito, 1000];[4, Fulanito, 300]...
							String[] arrayDB = message.split("\\];\\[");
							arrayDB[0] = arrayDB[0].replaceAll("\\[","");
							arrayDB[arrayDB.length-1] = arrayDB[arrayDB.length-1].replaceAll("\\]","");
							//[1, esr, 100]END
							if(arrayDB[arrayDB.length-1].substring(arrayDB[arrayDB.length-1].length()-3).equals("END")) {
								remainingData = false;
								arrayDB[arrayDB.length-1] = arrayDB[arrayDB.length-1].substring(0, arrayDB[arrayDB.length-1].length()-3);
							}
							boolean rM = true;
							for(String dbCli : arrayDB) {								
								int accN = Integer.parseInt(dbCli.substring(0,dbCli.indexOf(",")));
								int bal = Integer.parseInt(dbCli.substring(dbCli.lastIndexOf(",")+2, dbCli.length()));
								String name = dbCli.substring(dbCli.indexOf(",")+2,dbCli.lastIndexOf(","));

								BankClient bcli = new BankClient(accN, name, bal);
								rM = rM && createBankClient(bcli) ;
							}

							String responseMessage=""+rM;
							//create ObjectOutputStream object
							ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
							//write object to Socket
							oos.writeObject(responseMessage);

							//close resources
							ois.close();
							oos.close();
							socket.close();

						} catch ( java.io.InterruptedIOException e ) {
							System.err.println( "Timed Out (60 sec)!" );
							remainingData = false;
						}
					}
					serverDB.close();
				}

				try {
					listChanges = zk.getChildren(BANKS_PATH + CHANGES_PATH, changesWatcher, s); 
				} catch (Exception e) {
					System.out.println("The path not exists");
					System.out.println(e);
					break;
				}

				//Comprueba siempre el ultimo de la lista

				if (listChanges.size() > 0) {
					for(String change : listChanges) {
						try {

							//CADA FOLLOWER ENVIA UN "ACK" DE YA LO HE HECHO
							List<String> banks = new ArrayList<String>();
							try {
								banks = zk.getChildren(BANKS_PATH+ELECTION_PATH, false);
							} catch (KeeperException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							if(banks.size()>1) {
								try {
									String content = "received";
									byte[] value = ByteBuffer.wrap(content.getBytes("UTF-8")).array();

									Stat sACKChange = zk.exists(BANKS_PATH + ACKS_PATH + "/" + change, false);
									//Comprobar que el Leader ha creado la carpeta de acks correspondiente al cambio
									if(sACKChange != null) {
										Stat sACK = zk.exists(BANKS_PATH + ACKS_PATH + "/" + change + "/ack-"+myId, false);
										if(sACK == null) {

											path = BANKS_PATH + CHANGES_PATH+"/"+change;

											s = zk.exists(path, false);

											byte[] b = zk.getData(path, false, s);
											ByteBuffer buffer = ByteBuffer.wrap(b);
											data = new String(buffer.array(), "UTF-8");

											System.out.println("++++ BANK CHANGES "+myId+" :: Data: " + data + "; Path: " + path);

											//Dependiendo de que tipo sea se hace una cosa u otra	
											if(data.contains("CREATE") && data.indexOf("CREATE")==0) {
												System.out.println("BANK CHANGES "+myId+" :: CREATE REQUEST FROM LEADER.");
												//CREATE:[12, Hola, 123]
												//message.indexOf("[");
												String arrayString = data.substring(7).replace("[", "").replace("]", "");
												int accN = Integer.parseInt(arrayString.substring(0,arrayString.indexOf(",")));
												int bal = Integer.parseInt(arrayString.substring(arrayString.lastIndexOf(",")+2, arrayString.length()));
												String name = arrayString.substring(arrayString.indexOf(",")+2,arrayString.lastIndexOf(","));

												BankClient bc = new BankClient(accN,name,bal);
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

											// CREAR UN ZNODE ACK PARA LA PETICION DE CAMBIO QUE GENERO EL LIDER
											System.out.println("BANK CHANGES "+myId+" :: Created a ACK znode to the Leader");

											zk.create(BANKS_PATH + ACKS_PATH + "/" + change + "/ack-"+myId, value,
													Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

											//Paramos la ejecucion hasta que se le mande otro cambio
											/*synchronized(mutex) {
												mutex.wait();
											}*/
										}
									}

								} catch (Exception e) {
									e.printStackTrace();
								}
							}

						} catch (Exception e) {
							// The exception due to a race while getting the list of children, get data and delete. Another
							// consumer may have deleted a child while the previous access. Then, the exception is simply
							// implies that the data has not been produced.
							System.out.println("Exception when accessing the data in the znode, maybe the znode is deleted");
							System.err.println(e);
							//e.printStackTrace();
							//break;
						}
					}

				} else {
					try {
						//k.getChildren(BANKS_PATH + CHANGES_PATH, changesWatcher, s);
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
		//server.close();
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

	Watcher acksWatcher = new Watcher() {
		public void process(WatchedEvent event) {
			Stat s = null;

			System.out.println("------------------Watcher ACKs Individual------------------");
			System.out.println("BANK "+myId+" :: " + event.getType() + ", " + event.getPath());

			try {
				//Lista de los znodes bancos que hay en la red
				List<String> banks = new ArrayList<String>();
				try {
					banks = zk.getChildren(BANKS_PATH+ELECTION_PATH, false);
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
				//Lista de acks enviados por los bancos znodes a un cambio
				List<String> listIndACKs = new ArrayList<String>();
				try {
					//Obtiene la lista de los acks
					listIndACKs = zk.getChildren(event.getPath(), false, s);
				} catch (KeeperException | InterruptedException e) { 
					e.printStackTrace();
				} 
				s = zk.exists(event.getPath(), false);

				byte[] b = zk.getData(event.getPath(), false, s);

				ByteBuffer buffer = ByteBuffer.wrap(b);
				String listIdStringBanksACKChanges = new String(buffer.array(), "UTF-8");

				String[] listIdBanksACKChanges = listIdStringBanksACKChanges.split(",");

				//Compara la lista de ACKS que hay con la lista de followers que enviaron ACKS 
				//guardado dentro del znode .../acks/update-0001 menos el mismo
				int numberOfFollowers = listIdBanksACKChanges.length;//Numero de Followers admitidos que hay para enviar ACK en este cambio
				int numberOfACKs = listIndACKs.size();//Numero de ACKs solo se muestran para los nodos en linea que sean Followers
				for(String oneFollower : listIdBanksACKChanges) {

					//El lider borra sus acks
					if(("n_"+oneFollower).equals(myId)) {
						//Quiere decir que se decrementa ya que el propio lider esta como ack
						numberOfFollowers = numberOfFollowers-1;
						//Ya no hace falta borrar su ack pero se debe tener en cuenta ya que su id esta en la lista
					}
					//OJO si en la lista de los follower que deberian mandar ACKs si no esta online(/election)

					//Se debe elminar /changes y /acks, hay que ajustar el numero de followers
					//-Esta en la lista del znode de acks/update-001
					//-No esta en linea por lo que No esta en ACKs(Ephemeral)
					if(!banks.contains(("n_"+oneFollower)) && !listIndACKs.contains(("n_"+oneFollower))) {
						numberOfFollowers = numberOfFollowers - 1;
					}

					//ESCENARIO #123129: Un Follower conectado con el lider anterior recibe cambios pero antes de efectuarlos
					//el lider muere, el continua con los cambios y envia los acks(ya el nuevo lider -que puede ser el- se encargara
					//de gestionar esos acks y borrar los znodes)

					//No se debe eliminar /changes y /acks
					//-Esta en la lista del zonde de acks/update-001
					//-Esta en linea
					//-No esta en ACKs (en este momento pero eventualmente lo enviará)

				}
				//AHORA SI, comprueba que el numero de ACKs sea el mismo que el que deberia ser(todos los followers enviaron ACK)
				if (numberOfACKs == numberOfFollowers) {
					//SE BORRA EL ZNODE DE LOS Changes
					s = zk.exists(event.getPath().replace(ACKS_PATH, CHANGES_PATH), false);
					zk.delete(event.getPath().replace(ACKS_PATH, CHANGES_PATH), s.getVersion());

					for(String ack_n : listIndACKs) {
						try {
							String path = event.getPath() + "/" + ack_n;
							byte[] bACK = zk.getData(path, false, s);

							s = zk.exists(path, false);
							//zk.delete(path, s.getVersion());

							ByteBuffer bufferACK = ByteBuffer.wrap(bACK);
							String data = new String(bufferACK.array(), "UTF-8");

							if(data.equals("received")){
								zk.delete(event.getPath() +"/"+ack_n, s.getVersion());
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

					//SE BORRA EL ZNODE DE LOS ACKS
					s = zk.exists(event.getPath(), false);
					zk.delete(event.getPath(), s.getVersion());


				} else {
					//Lanza el Watcher debido a que aun no han contestado todos los integrantes, 
					//asi cuando conteste otro se lance el watcher
					zk.getChildren(event.getPath(), acksWatcher);
				}


			} catch (Exception e) {
				System.out.println("Unexpected Exception process");
			}
		}
	};

	Watcher changesWatcher = new Watcher() {
		public void process(WatchedEvent event) { 
			if(!isLeader) {
				Stat s = null;

				System.out.println("------------------Watcher Changes ------------------");
				System.out.println("BANK "+myId+" :: " + event.getType() + ", " + event.getPath());
				try {
					if (event.getPath().equals(BANKS_PATH + CHANGES_PATH)) {

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
		}
	};

	Watcher dbWatcher = new Watcher() {
		public void process(WatchedEvent event) { 

			Stat s = null;
			System.out.println("------------------Watcher DB ------------------");
			System.out.println("BANK "+myId+" :: " + event.getType() + ", " + event.getPath());

			List<String> dbReq;
			try {
				dbReq = zk.getChildren(BANKS_PATH + DB_PATH, dbWatcher, s);

				if(dbReq.size()>0) {
					//OBTENCION DE LA BASE DE DATOS EN UN STRING
					boolean remainingData = true;
					int pre = 0;

					while(remainingData) {
						String message="";
						//CONSTRUCCION DEL MENSAJE STRING A ENVIAR DE 1000 CARACTERES EN 1000
						//Enviar Sockets de 1000 en 1000 entradas de la db
						java.util.HashMap <Integer, BankClient> db = bankClientDB.getClientDB();
						Set<Integer> keysSet = db.keySet();
						Integer[] keys = keysSet.toArray(new Integer[keysSet.size()]);
						int i=pre;
						for(i=pre;i<1000&&i<db.size();i++) {
							//[1, Pepito, 1000];[4, Fulanito, 300]...
							message=message == "" ? db.get(keys[i]).toString() : message+";"+db.get(keys[i]).toString();
						}
						if(db.size()>i) {
							pre = i;
						} else {
							remainingData = false;
							message=message+"END";
						}

						for(String dbReq_i : dbReq) {
							boolean watingResponse = true;
							while(watingResponse) {
								boolean erasedDBReq = false;
								String path = BANKS_PATH + DB_PATH+"/"+dbReq_i;
								//System.out.println(path);
								byte[] b = zk.getData(path, false, s);

								s = zk.exists(path, false);

								//System.out.println(s.getVersion());
								//Tiene que enviar toda la base de datos a cada znode que haya, a la direccion que hay guardad en el znode
								Socket socket = null;
								ObjectOutputStream oos = null;
								ObjectInputStream ois = null;
								//Get the Ip and port of the bank_follower where the leader have to send the db
								ByteBuffer buffer = ByteBuffer.wrap(b);
								String[] data = new String(buffer.array(), "UTF-8").split(":");

								//establish socket connection to server
								try{
									socket = new Socket(data[0], Integer.parseInt(data[1]));
									System.out.println("Server follower bank avalible");
									//write to socket using ObjectOutputStream
									oos = new ObjectOutputStream(socket.getOutputStream());
									System.out.println("Sending db to Socket Server (Follower Bank)");
									oos.writeObject(message);

									//Aqui debe esperar a que responda el otro follower
									//read the server response message
									ois = new ObjectInputStream(socket.getInputStream());
									String messageResp = (String) ois.readObject();
									if(messageResp.equals("true")) {
										watingResponse=false;
									}
									System.out.println("Follower Bank RESPONSE: " + messageResp);
									//close resources
									ois.close();
									oos.close();

									socket.close();
								} catch (Exception e) {
									//La peticion de base de datos proviene de un servidor que no
									//esta disponible
									System.out.println("Server follower bank not avalible");
									Stat sDB = zk.exists(path, false);
									//Si se ha borrado el follower para la ejcucion de todos
									if(sDB==null) {
										remainingData=false;
										watingResponse=false;
										zk.delete(path, s.getVersion());
										erasedDBReq = true;
									}

								}


								if(!remainingData && !erasedDBReq && !watingResponse) {
									//BORRAR EL ZNODE QUE CREO LA PETICION DE BASE DE DATOS SI YA NO HAY MAS DATOS
									zk.delete(path, s.getVersion());
								}
							}
						}
					}



				} else {
					//Aqui se acaba y se ejcuta cuando se borra el nodo y se tiene que volver a lanzar el watcher
					System.out.println("------------------Watcher DB END------------------");
					System.out.println("BANK CHANGES-LEADER "+myId+" :: Waiting for client request");
				}


			} catch (Exception e) {
				System.out.println("Unexpected Exception process");
				e.printStackTrace();
			}

		}

	};

}
