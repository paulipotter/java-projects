import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
	private static Properties configProps = new Properties();

	private static String MySqlServerDriver;
	private static String MySqlServerUrl;
    private static String MySqlServerUser;
	private static String MySqlServerPassword;
	
	private static String PsotgreSqlServerDriver;
	private static String PostgreSqlServerUrl;
	private static String PostgreSqlServerUser;
	private static String PostgreSqlServerPassword;


	// DB Connection
	private Connection _mySqlDB; //IMDB
    private Connection _postgreSqlDB; //customer_DB

	// Canned queries
    
    //search
	private String _search_sql = "SELECT * FROM movie_info WHERE movie_name like ? ORDER BY movie_id";
	private PreparedStatement _search_statement;

	private String _search_actor = "SELECT actor_ids.actor_id, actor_ids.actor_name, movie_info.movie_id, movie_info.movie_name" 
			+"FROM actor_ids" 
			+"INNER JOIN actor_movies" 
			+"	ON actor_ids.actor_id = actor_movies.actor_id" 
			+"INNER JOIN movie_info" 
			+"	ON actor_movies.movie_id = movie_info.movie_id" 
			+"WHERE movie_info.movie_name LIKE ? "
			+"ORDER BY movie_info.movie_id";
	private PrepaedStatement _search_actor_statement;
	
	private String _producer_id_sql = "SELECT y.* "
					 + "FROM producer_movies x, producer_ids y "
					 + "WHERE x.movie_id = ? and x.producer_id = y.producer_id";
	private PreparedStatement _producer_id_statement;              

	private String _customer_login_sql = "SELECT * FROM customer WHERE login = ? and password = ?";
	private PreparedStatement _customer_login_statement;

	private String _begin_transaction_read_write_sql = "START TRANSACTION";
	private PreparedStatement _begin_transaction_read_write_statement;

	private String _commit_transaction_sql = "COMMIT";
	private PreparedStatement _commit_transaction_statement;

	private String _rollback_transaction_sql = "ROLLBACK";
	private PreparedStatement _rollback_transaction_statement;

    private String _check_rent = "SELECT max(times_rented) from rental WHERE cid = ? and movie_id = ?";
    private String _insert_rent = "INSERT INTO rental (cid, movie_id, status,times_rented)"
    								+" VALUES (?,?,'open', 1)";
    private PreparedStatement _new_rent_statement;

   

    //check remaining rentals
    private String _total_rentals = "SELECT max_rentals FROM plan INNER JOIN customer "
									+"ON customer.plan = plan.pidcustomer.cid = ? WHERE ";
    private PreparedStatement _total_rentals_statement;
    
    private String _current_rentals = "SELECT count(*) FROM rental WHERE cid = ? and status = 'open'";
    private PreparedStatement _current_rentals_statement;
    
    //return customer's name
    private String _customer_name = "SELECT first_name || ' ' || last_name "
			+" FROM customer WHERE cid= ?";
    private PreparedStatement _customer_name_statement;

    //check if p id is valid
  	private String _check_plan_id = "SELECT plan_id FROM plan WHERE plan_id LIKE ?";
  	private PreparedStatement _check_plan_id_statement;
    
    //check if movie id is valid
	private String _check_movie_id = "SELECT movie_id FROM movie_info WHERE movie_id LIKE ?";
	private PreparedStatement _check_movie_id_statement;

	// find who has this movie
	private String _find_renter = "SELECT cid FROM rental WHERE movie_id = ?";
	private PreparedStatement _find_renter_statement;
	
	//check status of movie
    private String _movie_status = "SELECT cid, status, max(times_rented) FROM rental WHERE movie_name LIKE ?";
	private PreparedStatement _movie_status_statement;
	
	//display all available plans
	private String _available_plans = "SELECT * FROM plan";
	private PreparedStatement _available_plans_statement;
	
	//update customer plan
	private String _update_plan = "UPDATE customer SET plan = ? WHERE cid = ?";
	private PreparedStatement _update_plan_statement;
	
	//return movie
	private String _return_movie = "UPDATE rental SET status = 'closed' WHERE cid = ? AND movie_id = ?";
	private PreparedStatement _return_movie_statement;
	
	//not done - fast search
	private String _fast_search = "";
	private PreparedStatement _fast_search_statement;
	
	public Query() {
	}

    /**********************************************************/
    /* Connection to MySQL database */

	public void openConnections() throws Exception {
        
        /* open connections to TWO databases: movie and  customer databases */
        
		configProps.load(new FileInputStream("dbconn.config"));
        
		MySqlServerDriver    = configProps.getProperty("MySqlServerDriver");
		MySqlServerUrl 	   = configProps.getProperty("MySqlServerUrl");
		MySqlServerUser 	   = configProps.getProperty("MySqlServerUser");
		MySqlServerPassword  = configProps.getProperty("MySqlServerPassword");
        
        PsotgreSqlServerDriver    = configProps.getProperty("PostgreSqlServerDriver");
        PostgreSqlServerUrl 	   = configProps.getProperty("PostgreSqlServerUrl");
        PostgreSqlServerUser 	   = configProps.getProperty("PostgreSqlServerUser");
        PostgreSqlServerPassword  = configProps.getProperty("PostgreSqlServerPassword");
                              
		/* load jdbc driver for MySQL */
		Class.forName(MySqlServerDriver);

		/* open a connection to your mySQL database that contains the movie database */
		_mySqlDB = DriverManager.getConnection(MySqlServerUrl, // database
				MySqlServerUser, // user
				MySqlServerPassword); // password
		
     
        /* load jdbc driver for PostgreSQL */
        Class.forName(PsotgreSqlServerDriver);
        
         /* connection string for PostgreSQL */
        String PostgreSqlConnectionString = PostgreSqlServerUrl+"?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory&user="+
        		PostgreSqlServerUser+"&password=" + PostgreSqlServerPassword;
        
        
        /* open a connection to your postgreSQL database that contains the customer database */
        _postgreSqlDB = DriverManager.getConnection(PostgreSqlConnectionString);
        
	
	}

	public void closeConnections() throws Exception {
		_mySqlDB.close();
        _postgreSqlDB.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		_search_statement = _mySqlDB.prepareStatement(_search_sql);
		_producer_id_statement = _mySqlDB.prepareStatement(_producer_id_sql);
		_check_movie_id_statement = _mySqlDB.prepareStatement(_check_movie_id);
		_search_actor_statement = _mySqlDB.prepareStatement(_search_actor);
		
		/* uncomment after you create your customers database */
		_customer_login_statement = _postgreSqlDB.prepareStatement(_customer_login_sql);
		_begin_transaction_read_write_statement = _postgreSqlDB.prepareStatement(_begin_transaction_read_write_sql);
		_commit_transaction_statement = _postgreSqlDB.prepareStatement(_commit_transaction_sql);
		_rollback_transaction_statement = _postgreSqlDB.prepareStatement(_rollback_transaction_sql);

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
		_total_rentals_statement = _postgreSqlDB.prepareStatement(_total_rentals);
		_current_rentals_statement = _postgreSqlDB.prepareStatement(_current_rentals);
		
		_customer_name_statement = _postgreSqlDB.prepareStatement(_customer_name);
		_check_plan_id_statement = _postgreSqlDB.prepareStatement(_check_plan_id);
		
		_total_rentals_statement = _postgreSqlDB.prepareStatement(_total_rentals);
		_current_rentals_statement = _postgreSqlDB.prepareStatement(_current_rentals);
		
        _new_rent_statement = _postgreSqlDB.prepareStatement(_insert_rent);
        _old_rent_statement = _postgreSqlDB.prepareStatement(_update_rent);
        
        _find_renter_statement = postgreSqlDB.prepareStatement(_find_renter);
        
		_movie_status_statement = postgreSqlDB.prepareStatement(_movie_status);
		_update_plan_statement = postgreSqlDB.prepareStatement(_update_plan);
		
		
		_available_plans_statement = postgreSqlDB.prepareStatement(_available_plans);
		_update_plan_statement = postgreSqlDB.prepareStatement(_update_plans);
		_return_movie_statement = _mySqlDB.prepareStatement(_return_movie);
		
		
		_fast_search_statement = postgreSqlDB.prepareStatement(_fast_search_statement);

	}


    /**********************************************************/
    /* suggested helper functions  */

	//done
	public int helper_compute_remaining_rentals(int cid) throws Exception {
		/* how many movies can she/he still rent ? */
		
		
		
		_total_rentals_statement.clearParameters();
        _total_rentals_statement.setInt(1,cid);
		
        ResultSet total_rentals = _total_rentals_statement.executeQuery();
        int total = total_rentals.getInt(1);
        total_rentals.close();
        
        _current_rentals_statement.clearParameters();
        _current_rentals_statement.setString(1,cid);
        
        ResultSet current_rentals = _current_rentals_statement.executeQuery();
        int current = current_rentals.getInt(1);
		current_rentals.close();
        /* you have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
		int remaining = total - current;
		return (remaining);
		
	}
	
	//done
	public String helper_compute_customer_name(int cid) throws Exception {
		/* you find  the name of the current customer */
        
        _customer_name_statement.clearParameters();
        _customer_name_statement.setInt(1,cid);
        
        ResultSet customer_name = _customer_name_statement.executeQuery();
		String name = customer_name.getString(1);
		customer_name.close();
        return (name);

	}
	
	//done
	public boolean helper_check_plan(int plan_id) throws Exception {
		/* is plan_id a valid plan id?  you have to figure out */
		_check_plan_id_statement.clearParameters();
		_check_plan_id_statement.setInt(1, plan_id);
		
		ResultSet plan_id_result = _check_plan_id_statement.executeQuery();
		if (!plan_id_result.next())
		{
			plan_id_result.close();
			return false;
		}
		plan_id_result.close();
		return true;
	}

	//done
	public boolean helper_check_movie(String movie_id) throws Exception {
		/* is movie_id a valid movie id? you have to figure out  */
	
		_check_movie_id_statement.clearParameters();
		_check_movie_id_statement.setString(1, '%' + movie_id + '%');
		
		ResultSet movie_id_result = _check_movie_id_statement.executeQuery();
		if (!movie_id_result.next())
		{
			movie_id_result.close();
			return false;
		}
		movie_id_result.close();
		return true;
	}

	//done
	private int helper_who_has_this_movie(String movie_id) throws Exception {
		/* find the customer id (cid) of whoever currently rents the movie movie_id; return -1 if none */
		_find_renter_statement.clearParameters();
		_find_renter_statement.setString(1, '%' + movie_id + '%');

		ResultSet customer_id = _find_renter_statement.executeQuery();
		
		if (!customer_id.next())
		{
			customer_id.close();
			return (-1);
		}
		int result = customer_id.getInt();
		customer_id.close();
		return (result);
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public int transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		int cid;

		_customer_login_statement.clearParameters();
		_customer_login_statement.setString(1,name);
		_customer_login_statement.setString(2,password);
	    ResultSet cid_set = _customer_login_statement.executeQuery();
	    if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		return(cid);
		//return (55); //comment after you create your own customers database
	}
	
	//done
	public void transaction_personal_data(int cid) throws Exception {
		/* println the customer's personal data: name and number of remaining rentals */
		System.out.println(helper_compute_customer_name(cid) + "\n Remaining rentals: " + Integer.toString(helper_compute_remaining_rentals(cid)));
	}

    /**********************************************************/
    /* main functions in this project: */

	//done?
	public void transaction_search(int cid, String movie_name)
			throws Exception {
		/* searches for movies with matching names: SELECT * FROM movie WHERE movie_name LIKE name */
		/* prints the movies, producers, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

		/* set the first (and single) '?' parameter */
		_search_statement.clearParameters();
		_search_statement.setString(1, '%' + movie_name + '%');

		ResultSet movie_set = _search_statement.executeQuery();
		while (movie_set.next()) {
			String movie_id = movie_set.getString(1);
			System.out.println("ID: " + movie_id + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3) + " RATING: "
					+ movie_set.getString(4));
			/* do a dependent join with producer */
			_producer_id_statement.clearParameters();
			_producer_id_statement.setString(1, movie_id);
			ResultSet producer_set = _producer_id_statement.executeQuery();
			while (producer_set.next()) {
				System.out.println("\t\tProducer name: " + producer_set.getString(2));
			}
			producer_set.close();
			
			// actor
			_search_actor_statement.clearParameters();
			_search_actor_statement.setString(1, '%' + movie_name + '%');
			
			Result actor_set = _search_actor_statement.executeQuery();
			while(movie_set.next()) {
				String actor_id = actor_set.getString(1);
				System.out.println("ID:" + actor_id + " NAME: "
				+ actor_set.getString(2) + " MOVIE_ID: "
				+ actor_set.getString(3) + " MOVIE_NAME: "
				+ actor_set.getString(4));
			}
			actor_set.close();
			
			/* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */
			
			_movie_status_statement.clearParameters();
			_movie_status_statement.setString(1, '%' + movie_name + '%');
			ResultSet movie_status = _movie_status_statement.executeQuery();
			int cid_result = movie_status.getInt(1);
			String status_result = movie_status.getString(2);
			int times_result = movie_status.getInt(3);
			movie_status.close();
			
			if(status_result == 'open')
			{
				if (cid_result == cid) { //you have it}
					System.out.println("STATUS: YOU HAVE IT")
				}
				else //unavailable
				{
					System.out.println("STATUS: UNAVAILABLE")
				}
			}
			else
			{
				System.out.println("STATUS: AVAILABLE")
			}
			
		}
		System.out.println();
	}

	//done
	public void transaction_choose_plan(int cid, int pid) throws Exception {
	    /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
	    /* remember to enforce consistency ! */
	
		_update_plan_statement.clearParameters();
		_update_plan_statement.setInt(1, pid);
		_update_plan_statement.setInt(2, cid);
		
		_update_plan_statement.executeQuery();
		System.out.println("Plan for "+cid+ " has been updated");
	}

	//done
	public void transaction_list_plans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
		
		_available_plans_statement.clearParameters();
		ResultSet all = _available_plans_statement.executeQuery();
		
		while (all.next()) {
			int plan_id = all.getInt(1);
			System.out.println("ID: " + plan_id + " NAME: "
								+ all.getString(2) + " FEE: "
								+ all.getString(3) + " MAX_RENTALS_ALLOWED: "
								+ all.getString(4));
		}
		all.close();
			
	}

	//does not work
	public void transaction_rent(int cid, String movie_id) throws Exception {
	    /* rend the movie movie_id to the customer cid */
	    /* remember to enforce consistency ! */
        _check_rent.clearParameters();
        _check_rent.setString(1,'%' + cid + '%',2, '%' + movie_id + '%' );

        ResultSet check_existing = _check_rent_statement.executeQuery();
        int rented = 0;
        String open = "open";
        if (check_existing.first()) //not empty
        {
            _old_rent_statement.clearParameters();
            _old_rent_statement.setString(1,'%' + cid + '%',2, '%' + movie_id + '%' , 3, '%'+open+'%',rented++ );

        }else  //is empty
        {
            _new_rent_statement.clearParameters();
            _new_rent_statement.setString(1,'%' + cid + '%',2, '%' + movie_id + '%' );
        }

	}
	
	//did not test but done
	public void transaction_return(int cid, String movie_id) throws Exception {
	    /* return the movie_id by the customer cid */
		
		_return_movie_statement.clearParameters();
		_return_movie_statement.setInt(1, cid);
		_return_movie_statement.setString(2, movie_id);
	
		_return_movie_statement.executeQuery();
		
		System.out.println("movie has been returned.")
	}
	
	//does not work
	public void transaction_fast_search(int cid, String movie_name)
			throws Exception {
		/* like transaction_search, but uses joins instead of dependent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join producers, (c) movies join actors
		   Answers are sorted by movie_id.
		   Then merge-joins the three answer sets */
		 _fast_search_statement.clearParameters();
		
	}

}
