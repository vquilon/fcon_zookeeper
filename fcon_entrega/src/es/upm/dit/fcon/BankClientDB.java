package es.upm.dit.fcon;

import java.io.Serializable;

public class BankClientDB implements Serializable {

	private static final long serialVersionUID = 1L;

	private java.util.HashMap <Integer, BankClient> clientDB; 

	public BankClientDB (BankClientDB bankClientDB) {
		this.clientDB = bankClientDB.getClientDB();
	}
	
	public BankClientDB() {
		clientDB = new java.util.HashMap <Integer, BankClient>();
	}

	public java.util.HashMap <Integer, BankClient> getClientDB() {
		return this.clientDB;
	}
	
	public boolean createClient(BankClient bankClient) {		
		if (clientDB.containsKey(bankClient.getAccountNumber())) {
			return false;
		} else {
			clientDB.put(bankClient.getAccountNumber(), bankClient);
			return true;
		}		
	}

	public BankClient readClient(Integer accountNumber) {
		if (clientDB.containsKey(accountNumber)) {
			return clientDB.get(accountNumber);
		} else {
			return null;
		}		
	}

	public boolean updateClient (int accNumber, int balance) {
		if (clientDB.containsKey(accNumber)) {
			BankClient bankClient = clientDB.get(accNumber);
			bankClient.setBalance(balance);
			clientDB.put(bankClient.getAccountNumber(), bankClient);
			return true;
		} else {
			return false;
		}	
	}

	public boolean deleteClient(Integer accountNumber) {
		if (clientDB.containsKey(accountNumber)) {
			clientDB.remove(accountNumber);
			return true;
		} else {
			return false;
		}	
	}

	public boolean createBank(BankClientDB bankClientDB) {
		System.out.println("createBank");
		this.clientDB = bankClientDB.getClientDB();
		System.out.println(bankClientDB.toString());
		return true;
	}
	
	public String toString() {
		String aux = new String();

		for (java.util.HashMap.Entry <Integer, BankClient>  entry : clientDB.entrySet()) {
			aux = aux + entry.getValue().toString() + "\n";
		}
		return aux;
	}
}



