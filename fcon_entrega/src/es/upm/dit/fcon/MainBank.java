package es.upm.dit.fcon;

import java.util.Scanner;

public class MainBank {
	public MainBank() {
		// TODO Auto-generated constructor stub
	}

	public void initMembers(Bank bank) {

		if (!bank.createClient(new BankClient(1, "Angel Alarcón", 100))) {
			return;
		}
		if (!bank.createClient(new BankClient(2, "Bernardo Bueno", 200))) {
			return;
		}
		if (!bank.createClient(new BankClient(3, "Carlos Cepeda", 300))) {
			return;
		}
		if (!bank.createClient(new BankClient(4, "Daniel Díaz", 400))) {
			return;
		}
		if (!bank.createClient(new BankClient(5, "Eugenio Escobar", 500))) {
			return;
		}
		if (!bank.createClient(new BankClient(6, "Fernando Ferrero", 600))) {
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
		
		//Create Members zokeeper
		zkLeader zk = new zkLeader();

		try {
			Thread.sleep(300000); 			
		} catch (Exception e) {
			// TODO: handle exception
		}

		
		boolean correct = false;
		int     menuKey = 0;
		boolean exit    = false;
		Scanner sc      = new Scanner(System.in);
		int accNumber   = 0;
		int balance     = 0;
		BankClient bankClient   = null;
		boolean status  = false;
		String cluster  = null;
		
		if (args.length == 0) {
			System.out.println("Incorrect arguments: dht.Main <Number_Group>");
			sc.close();
			return;
		} else {
			cluster = args[0];
		} 
		
		Bank bank = new Bank(cluster);
		MainBank mainBank = new MainBank();

		//System. out .println(">>> Enter opn cliente.: 1) Start bank");
		//sc.next();
		if (bank.isLeader()) mainBank.initMembers(bank);
		while (!exit) {
			try {
				correct = false;
				menuKey = 0;
				while (!correct) {
					System. out .println(">>> Enter opn cliente.: 1) Create. 2) Read. 3) Update. 4) Delete. 5) BankDB. 6) Exit");				
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
					bank.createClient(mainBank.readClient(sc));
					break;
				case 2: // Read client
					System. out .print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
						bankClient = bank.readClient(accNumber);
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
					bank.updateClient(accNumber, balance);
					break;
				case 4: // Delete client
					System. out .print(">>> Enter account number (int) = ");
					if (sc.hasNextInt()) {
						accNumber = sc.nextInt();
						status = bank.deleteClient(accNumber);
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

