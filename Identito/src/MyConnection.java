import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.spell.*;
import org.apache.lucene.util.Version;

public class MyConnection {

	public static void OuvrirConnection ()
	{
		Connection con;
		ResultSet res;
		ResultSet id_recup;
		Statement selection;
		Statement attribuer_id;
		Statement recup_id;
		Statement inserer_id;
		Statement inserer_id2;
		Statement changer_flag;
		try { 

			//Class.forName("com.mysql.jdbc.Driver").newInstance();
			Class.forName("com.mysql.jdbc.Driver");

			//DriverManager.registerDriver (new oracle.jdbc.OracleDriver());

			con=DriverManager.getConnection("jdbc:mysql://localhost:3306/etl_projet", "root","");

			selection = con.createStatement();

			res = selection.executeQuery("SELECT Sexe,Prenom,Nom,DateNaissance,NumINCR FROM `table_patient` where `IDref` IS NULL ");
			res.next();
			attribuer_id =con.createStatement();
			attribuer_id.executeUpdate("insert into patient_id(flag) values (0)");

			recup_id=con.createStatement();
			id_recup=recup_id.executeQuery("Select Max(NumID) from patient_id");
			id_recup.next();
			int id_ref= id_recup.getInt(1);

			String Sexe = res.getString(1);
			String Prenom = res.getString(2);
			String Nom = res.getString(3);
			String DDN = res.getString(4);
			String PN=Prenom.concat(Nom);
			int IDpatient=res.getInt(5);
			DDN= DDN.replaceAll("-", "");
			PN = PN.replaceAll("\\s","" );

			inserer_id=con.createStatement();
			inserer_id.executeUpdate("update  table_patient set IDref="+id_ref+" where numINCR="+IDpatient);

			while (res.next()){
				
				String PrenomComp = res.getString(2);
				String NomComp = res.getString(3);
				
				int IDpatient_comp=res.getInt(5);
				String PNcomp=PrenomComp.concat(NomComp);
				
				PNcomp = PNcomp.replaceAll("\\s","" );

				float distance_lev_PN = new LuceneLevenshteinDistance().getDistance(PNcomp,PN);

				if (distance_lev_PN >= 0.80)
				{
					String SexeComp = res.getString(1);
					String DDNComp = res.getString(4);
					DDNComp= DDN.replaceAll("-", "");
					float distance_lev_S = new LuceneLevenshteinDistance().getDistance(Sexe,SexeComp);
					float distance_lev_DDN = new LuceneLevenshteinDistance().getDistance(DDN,DDNComp);
					float score = (float) (0.70*distance_lev_PN  + 0.15*distance_lev_S+0.15*distance_lev_DDN);
					if (score >= 0.95)
					{
						inserer_id2=con.createStatement();
						inserer_id2.executeUpdate("update  table_patient set IDref="+id_ref+" where numINCR="+IDpatient_comp);
					}	
					if (score < 0.95 & score > 0.60 ) {
						inserer_id2=con.createStatement();
						inserer_id2.executeUpdate("update table_patient  set IDref="+id_ref+" where numINCR="+IDpatient_comp);
						changer_flag=con.createStatement();
						changer_flag.executeUpdate("UPDATE patient_id SET Flag=1 WHERE numID="+id_ref);
					}
				}
			}
		}catch (ClassNotFoundException e){

			System.out.println("Driver spécifié non trouvé");
			e.printStackTrace();

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		finally {

		}
	}
	public static void NettoyageBDD() {
		Statement Selection_tout;
		Connection con2;
		ResultSet res3;
		

		try { 

			//Class.forName("com.mysql.jdbc.Driver").newInstance();
			Class.forName("com.mysql.jdbc.Driver");

			//DriverManager.registerDriver (new oracle.jdbc.OracleDriver());

			con2=DriverManager.getConnection("jdbc:mysql://localhost:3306/etl_projet", "root","");
			Selection_tout = con2.createStatement();
			res3 = Selection_tout.executeQuery("SELECT Sexe,Prenom,Nom,DateNaissance,NumINCR FROM `table_patient` where `IDref` IS NULL ");
			while (res3.next()){
				if (res3.getString(1)==null)
				{
					break;
				}
				else {
					OuvrirConnection();
					System.out.println("Patient process !");
				}
				}
			System.out.println("Doublons identifiés !");
		}


		catch (ClassNotFoundException e){

			System.out.println("Driver spécifié non trouvé");
			e.printStackTrace();

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		finally {
			
		}
	}
	
	public static void ConfirmationPatient() {
		try {
			Desktop d = Desktop.getDesktop();
			d.browse(new URI("http://localhost/Vianney/confirmation.php"));

			} catch (URISyntaxException e) {
			} catch (IOException e) {
			}
	}

	public static void main(String[] args) {
			
		NettoyageBDD();
		ConfirmationPatient();
	}
}
