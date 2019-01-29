package es.upm.dit.fcon;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
import java.net.Socket;
import java.net.UnknownHostException;

public class Comm {

	//socket server port on which it will listen
	private static int port = 3000;
	//Se comporta como un balanceador, si uno no esta disponible se lo envia a otro hasta que haya conexión
	//Se debe añadir como argumentos en la ejecución las direcciones ip separadas, de todas ls maquinas que habra (direcciones ip a balancear)
	private String[] loadB;
	
	public Comm(String[] ip) throws IOException{
		if(ip.length==0) {
			this.loadB = new String[1];
			this.loadB[0] = "localhost";
		} else {
			this.loadB = ip;
		}
	}


	public boolean createBankClient(BankClient bankClient) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
		//InetAddress host = InetAddress.getLocalHost();
		Socket socket = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;

		String message = "";
		boolean conn = true;
		while(conn) {
			for(String ip : loadB) {
				try {
					socket = new Socket(ip, port);
					//write to socket using ObjectOutputStream
					oos = new ObjectOutputStream(socket.getOutputStream());
					System.out.println("Sending request to Socket Server (Leader Bank)");

					int accN = bankClient.getAccountNumber();
					int bal = bankClient.getBalance();
					String name = bankClient.getName();
					oos.writeObject("CREATE:"+"ACCN:"+accN+"BAL:"+bal+"NAME:"+name);
					//read the server response message
					ois = new ObjectInputStream(socket.getInputStream());
					message = (String) ois.readObject();
					System.out.println("LEADER RESPONSE: " + message);

					//close resources
					ois.close();
					oos.close();

					socket.close();

					if(!message.equals("")) {
						conn=false;
						break;
					}
				} catch(Exception e) {
					//El servidor no existe
				}
			}
		}
		return Boolean.parseBoolean(message);
	}

	public BankClient readBankClient(Integer accNumber) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
		//InetAddress host = InetAddress.getLocalHost();
		Socket socket = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;

		String message = "";
		boolean conn = true;
		while(conn) {
			for(String ip : loadB) {
				try {
					//establish socket connection to server
					socket = new Socket(/*host.getHostName()*/ip, port);
					//write to socket using ObjectOutputStream
					oos = new ObjectOutputStream(socket.getOutputStream());
					System.out.println("Sending request to Socket Server (Leader Bank)");

					//if(i==4)oos.writeObject("exit");
					//else oos.writeObject(""+i);

					oos.writeObject("READ:"+accNumber);
					//read the server response message
					ois = new ObjectInputStream(socket.getInputStream());
					message = (String) ois.readObject();
					System.out.println("LEADER RESPONSE: " + message);
					//close resources

					//close resources
					ois.close();
					oos.close();

					socket.close();

					if(!message.equals("")) {
						conn=false;
						break;
					}
				} catch(Exception e) {
					//El servidor no existe
					System.out.println("La ip "+ip+" no esta disponible");
				}
			}
		}
		int accN = Integer.parseInt(message.substring(1,message.indexOf(",")));
		int bal = Integer.parseInt(message.substring(message.lastIndexOf(",")+2, message.length()-1));
		String name = message.substring(message.indexOf(",")+2,message.lastIndexOf(","));

		BankClient bc = new BankClient(accN, name, bal);
		return bc; 
	}

	public boolean updateBankClient(int accNumber, int balance) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
		//InetAddress host = InetAddress.getLocalHost();
		Socket socket = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;

		String message = "";
		boolean conn = true;
		while(conn) {
			for(String ip : loadB) {
				try {
					//establish socket connection to server
					socket = new Socket(/*host.getHostName()*/ip, port);
					//write to socket using ObjectOutputStream
					oos = new ObjectOutputStream(socket.getOutputStream());
					System.out.println("Sending request to Socket Server (Leader Bank)");

					//if(i==4)oos.writeObject("exit");
					//else oos.writeObject(""+i);

					oos.writeObject("UPDATE:["+accNumber+","+balance+"]");
					//read the server response message
					ois = new ObjectInputStream(socket.getInputStream());
					message = (String) ois.readObject();
					System.out.println("LEADER RESPONSE: " + message);
					//close resources
					ois.close();
					oos.close();

					socket.close();
					if(!message.equals("")) {
						conn=false;
						break;
					}
				} catch(Exception e) {
					//El servidor no existe
				}
			}
		}
		return Boolean.parseBoolean(message);
	}

	public boolean deleteBankClient(int accNumber) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
		//InetAddress host = InetAddress.getLocalHost();
		Socket socket = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;

		String message = "";
		boolean conn = true;
		while(conn) {
			for(String ip : loadB) {
				try {
					//establish socket connection to server
					socket = new Socket(/*host.getHostName()*/ip, port);
					//write to socket using ObjectOutputStream
					oos = new ObjectOutputStream(socket.getOutputStream());
					System.out.println("Sending request to Socket Server (Leader Bank)");

					//if(i==4)oos.writeObject("exit");
					//else oos.writeObject(""+i);

					oos.writeObject("DELETE:"+accNumber);
					//read the server response message
					ois = new ObjectInputStream(socket.getInputStream());
					message = (String) ois.readObject();
					System.out.println("LEADER RESPONSE: " + message);
					//close resources
					ois.close();
					oos.close();

					socket.close();
					if(!message.equals("")) {
						conn=false;
						break;
					}
				} catch(Exception e) {
					//El servidor no existe
				}
			}
		}
		return Boolean.parseBoolean(message);
	}

}
