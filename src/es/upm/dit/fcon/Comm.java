package es.upm.dit.fcon;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassNotFoundException;
import java.net.Socket;
import java.net.UnknownHostException;

public class zkComm {

    //socket server port on which it will listen
    private static int port = 3000;

	public zkComm() throws IOException{
        
	}
	
	
	public boolean createBankClient(BankClient bankClient) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
        //InetAddress host = InetAddress.getLocalHost();
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
            
        //establish socket connection to server
        socket = new Socket(/*host.getHostName()*/"localhost", port);
        //write to socket using ObjectOutputStream
        oos = new ObjectOutputStream(socket.getOutputStream());
        System.out.println("Sending request to Socket Server (Leader Bank)");
        
        //if(i==4)oos.writeObject("exit");
        //else oos.writeObject(""+i);
        
        oos.writeObject("CREATE:"+bankClient.toString());
        //read the server response message
        ois = new ObjectInputStream(socket.getInputStream());
        String message = (String) ois.readObject();
        System.out.println("LEADER RESPONSE: " + message);
        //close resources
        ois.close();
        oos.close();
        
        socket.close();        
        //Thread.sleep(100);
		return Boolean.parseBoolean(message);
	}
	
	public BankClient readBankClient(Integer accNumber) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
        //InetAddress host = InetAddress.getLocalHost();
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
            
        //establish socket connection to server
        socket = new Socket(/*host.getHostName()*/"localhost", port);
        //write to socket using ObjectOutputStream
        oos = new ObjectOutputStream(socket.getOutputStream());
        System.out.println("Sending request to Socket Server (Leader Bank)");
        
        //if(i==4)oos.writeObject("exit");
        //else oos.writeObject(""+i);
        
        oos.writeObject("READ:"+accNumber);
        //read the server response message
        ois = new ObjectInputStream(socket.getInputStream());
        String message = (String) ois.readObject();
        System.out.println("LEADER RESPONSE: " + message);
        //close resources
        ois.close();
        oos.close();
        
        socket.close();
        //Thread.sleep(100);
		String[] array = message.replace("[", "").replace("]", "").split(",");
		BankClient bc = new BankClient(Integer.parseInt(array[0]),array[1].replace(" ",""),Integer.parseInt(array[2].replace(" ","")));
		return bc; 
	}
	
	public boolean updateBankClient(int accNumber, int balance) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
        //InetAddress host = InetAddress.getLocalHost();
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
            
        //establish socket connection to server
        socket = new Socket(/*host.getHostName()*/"localhost", port);
        //write to socket using ObjectOutputStream
        oos = new ObjectOutputStream(socket.getOutputStream());
        System.out.println("Sending request to Socket Server (Leader Bank)");
        
        //if(i==4)oos.writeObject("exit");
        //else oos.writeObject(""+i);
        
        oos.writeObject("UPDATE:["+accNumber+","+balance+"]");
        //read the server response message
        ois = new ObjectInputStream(socket.getInputStream());
        String message = (String) ois.readObject();
        System.out.println("LEADER RESPONSE: " + message);
        //close resources
        ois.close();
        oos.close();
        
        socket.close();
        //Thread.sleep(100);
		return Boolean.parseBoolean(message);
	}
	
	public boolean deleteBankClient(int accNumber) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException {
		//get the localhost IP address, if server is running on some other IP, you need to use that
        //InetAddress host = InetAddress.getLocalHost();
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
            
        //establish socket connection to server
        socket = new Socket(/*host.getHostName()*/"localhost", port);
        //write to socket using ObjectOutputStream
        oos = new ObjectOutputStream(socket.getOutputStream());
        System.out.println("Sending request to Socket Server (Leader Bank)");
        
        //if(i==4)oos.writeObject("exit");
        //else oos.writeObject(""+i);
        
        oos.writeObject("DELETE:"+accNumber);
        //read the server response message
        ois = new ObjectInputStream(socket.getInputStream());
        String message = (String) ois.readObject();
        System.out.println("LEADER RESPONSE: " + message);
        //close resources
        ois.close();
        oos.close();
        
        socket.close();
        //Thread.sleep(100);
		return Boolean.parseBoolean(message);
	}
	
}
