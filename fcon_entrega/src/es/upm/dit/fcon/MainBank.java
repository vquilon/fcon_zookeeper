package es.upm.dit.fcon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Scanner;

import org.apache.zookeeper.KeeperException;

public class MainBank {
	public MainBank() {
		// TODO Auto-generated constructor stub
	}

	public void initMembers(Bank bank) {
		//Solo se ejecuta si es un Banco y es el lider
		
		if (!bank.createBankClient(new BankClient(1, "Angel Alarcón", 100))) {
			return;
		}
		if (!bank.createBankClient(new BankClient(2, "Bernardo Bueno", 200))) {
			return;
		}
		if (!bank.createBankClient(new BankClient(3, "Carlos Cepeda", 300))) {
			return;
		}
		if (!bank.createBankClient(new BankClient(4, "Daniel Díaz", 400))) {
			return;
		}
		if (!bank.createBankClient(new BankClient(5, "Eugenio Escobar", 500))) {
			return;
		}
		if (!bank.createBankClient(new BankClient(6, "Fernando Ferrero", 600))) {
			return;
		}
	}

	public BankClient readClient(Scanner sc) {
		int accNumber = 0;
		String name   = null;
		int balance   = 0;

		System. out .print(">>> Enter account number (int) = ");
		if (sc.hasNextInt()) {
			accNumber = sc.nextInt();
		} else {
			System.out.println("The provised text provided is not an integer");
			sc.next();
			return null;
		}

		System. out .print(">>> Enter name (String) = ");
		name = sc.next();

		System. out .print(">>> Enter balance (int) = ");
		if (sc.hasNextInt()) {
			balance = sc.nextInt();
		} else {
			System.out.println("The provised text provided is not an integer");
			sc.next();
			return null;
		}
		return new BankClient(accNumber, name, balance);
	}

	public static void main(String[] args) {

		boolean correct_pre = false;
		int     menuKey_pre = 0;
		boolean correct = false;
		int     menuKey = 0;
		boolean exit    = false;
		Scanner sc      = new Scanner(System.in);
		int accNumber   = 0;
		int balance     = 0;
		BankClient bankClient   = null;
		boolean status  = false;
		String cluster  = null;
		
		boolean bank_znode = false;
		/*if (args.length == 0) {
			System.out.println("Incorrect arguments: dht.Main <Number_Group>");
			sc.close();
			return;
		} else {
			cluster = args[0];
		}*/


		MainBank mainBank = new MainBank();
		
		
		while (!correct_pre) {
			System.out.println(">>> Enter opn cliente.: 1) Create BANK node. 2) Start CLI");
			if (sc.hasNextInt()) {
				menuKey_pre = sc.nextInt();
				correct_pre = true;
			} else {
				sc.next();
				System.out.println("The provised text provided is not an integer");
			}
			switch (menuKey_pre) {
			case 1: // Create BANK node
				//Create Bank with Election Leader with ZooKeeper
				
				bank_znode = true;
				
				break;
			case 2: // Start CLI - Significa que hay que se crean aqu� los nodos para actualizar la BBDD
				bank_znode = false;
				
				break;
			default:
				break;
			}
		}
		
		if(bank_znode) {
			sc.close();
			
			Bank bank = new Bank();
			if (bank.isLeader()) mainBank.initMembers(bank);
			
			bank.start();
			//Loop para que el watcher se re-ejecute cada vez que haya un cambio
			//Si es lider tiene un watcher de los clientes
			//Si no es lider tiene solo el watcher de escucha al lider
		}
		
		else {
			zkComm comm = new zkComm();
			while (!exit && !bank_znode) {
				try {
					correct = false;
					menuKey = 0;
					while (!correct) {
						System.out.println(">>> Enter opn cliente.: 1) Create. 2) Read. 3) Update. 4) Delete. 5) BankDB. 6) Exit");				
						if (sc.hasNextInt()) {
							menuKey = sc.nextInt();
							correct = true;
						} else {
							sc.next();
							System.out.println("The provised text provided is not an integer");
						}
					}

					switch (menuKey) {
					case 1: // Create client
						//Crear un znodo para avisar al resto que tienen que actualizar su Bank
						//bank.createBankClient(mainBank.readClient(sc));
						comm.createBankClient(mainBank.readClient(sc));

						break;
					case 2: // Read client
						System. out .print(">>> Enter account number (int) = ");
						if (sc.hasNextInt()) {
							accNumber = sc.nextInt();
							//bankClient = bank.readBankClient(accNumber);
							bankClient = comm.readBankClient(accNumber);
							System.out.println(bankClient);
						} else {
							System.out.println("The provised text provided is not an integer");
							sc.next();
						}
						break;
					case 3: // Update client
						System. out .print(">>> Enter account number (int) = ");
						if (sc.hasNextInt()) {
							accNumber = sc.nextInt();
						} else {
							System.out.println("The provised text provided is not an integer");
							sc.next();
						}
						System. out .print(">>> Enter balance (int) = ");
						if (sc.hasNextInt()) {
							balance = sc.nextInt();
						} else {
							System.out.println("The provised text provided is not an integer");
							sc.next();
						}
						bank.updateBankClient(accNumber, balance);
						break;
					case 4: // Delete client
						System. out .print(">>> Enter account number (int) = ");
						if (sc.hasNextInt()) {
							accNumber = sc.nextInt();
							status = bank.deleteBankClient(accNumber);
						} else {
							System.out.println("The provised text provided is not an integer");
							sc.next();
						}
						break;
					case 5:
						String aux = bank.toString();
						System.out.println(bank.toString());
						break;
					case 6:
						exit = true;	
						bank.close();
					default:
						break;
					}
				} catch (Exception e) {
					System.out.println("Exception at Main. Error read data");
				}

			}

			sc.close();
		}
		
	}
}

