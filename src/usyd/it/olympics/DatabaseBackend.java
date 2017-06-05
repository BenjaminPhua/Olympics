package usyd.it.olympics;


/**
 * Database back-end class for simple gui.
 * 
 * The DatabaseBackend class defined in this file holds all the methods to 
 * communicate with the database and pass the results back to the GUI.
 *
 *
 * Make sure you update the dbname variable to your own database name. You
 * can run this class on its own for testing without requiring the GUI.
 */
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

/**
 * Database interfacing backend for client. This class uses JDBC to connect to
 * the database, and provides methods to obtain query data.
 * 
 * Most methods return database information in the form of HashMaps (sets of 
 * key-value pairs), or ArrayLists of HashMaps for multiple results.
 *
 * @author Bryn Jeffries {@literal <bryn.jeffries@sydney.edu.au>}
 */
public class DatabaseBackend {

    ///////////////////////////////
    /// DB Connection details
    ///////////////////////////////
    private final String dbUser;
    private final String dbPass;
	private final String connstring;


    ///////////////////////////////
    /// Student Defined Functions
    ///////////////////////////////

    /////  Login and Member  //////

    /**
     * Validate memberID details
     * 
     * Implements Core Functionality (a)
     *
     * @return true if username is for a valid memberID and password is correct
     * @throws OlympicsDBException 
     * @throws SQLException
     */
    public HashMap<String,Object> checkLogin(String member, char[] password) throws OlympicsDBException  {
        HashMap<String,Object> details = null;
        
        String stringBuffer = "SELECT *, CASE WHEN member.member_id = athlete.member_id THEN 'athlete' " +
											"WHEN member.member_id = staff.member_id THEN 'staff' " +
											"WHEN member.member_id = official.member_id THEN 'official' "+
											"END as member_type " +
							"FROM member JOIN country USING (country_code) " +
												 "JOIN accommodation ON (accommodation = place_id) " +
												 "JOIN place USING (place_id) " +
												 "LEFT OUTER JOIN athlete USING (member_id) " +
												 "LEFT OUTER JOIN staff USING (member_id) " +
												 "LEFT OUTER JOIN official USING (member_id) " + 
							"WHERE member_id = ?";
    	
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.prepareStatement(stringBuffer.toString());
            stmt.setString(1, member);
            
            ResultSet rset = stmt.executeQuery();
            
            while(rset.next()){
        		boolean valid = (member.equals(rset.getString("member_id")) && new String(password).equals(rset.getString("pass_word")));
        		if (valid) {
        			details = new HashMap<String,Object>();
        			// Populate with record data
        			details.put("member_id", member);
        			details.put("title", rset.getString("title"));
        			details.put("first_name", rset.getString("given_names"));
        			details.put("family_name", rset.getString("family_name"));
        			details.put("country_name", rset.getString("country_name"));
        			details.put("residence", rset.getString("place_name") );
        			details.put("member_type", rset.getString("member_type"));
        		}
        	};
        } catch (Exception e) {
            throw new OlympicsDBException("Error checking login details", e);
        }
        return details;
    }

    /**
     * Obtain details for the current memberID
     * @param memberID 
     * @param member_type 
     *
     *
     * @return text to be displayed in the home screen
     * @throws OlympicsDBException
     */
    public HashMap<String, Object> getMemberDetails(String memberID) throws OlympicsDBException {
    	 // FIXME: REPLACE FOLLOWING LINES WITH REAL OPERATION
    	HashMap<String, Object> details = new HashMap<String, Object>();
		
		String memberDetails = "SELECT * " +
								"FROM member " +
											"JOIN country USING (country_code) "+
											"JOIN accommodation ON (accommodation = place_id) " + 
											"JOIN place USING (place_id) " + 
							    "WHERE member_id = ? ;";

		String memberType = "SELECT " +
									"CASE WHEN member.member_id = athlete.member_id THEN 'athlete' " +
									 "WHEN member.member_id = staff.member_id THEN 'staff' " +
									 "WHEN member.member_id = official.member_id THEN 'official' " +
									 "END as member_type " +
						     "FROM member " +
						     				"LEFT OUTER JOIN athlete USING (member_id) "
						     				+ "LEFT OUTER JOIN staff USING (member_id) "
						     				+ "LEFT OUTER JOIN official USING (member_id) "
						     + "WHERE member.member_id = ? ;";
		
		String Bookings = "SELECT COUNT(*) as num_bookings "
						+ "FROM booking WHERE booked_for = ? ;";
		
		String medals = "SELECT COUNT(medal = 'G') as gold, "
				             + "COUNT(medal = 'S') as silver, "
				             + "COUNT(medal = 'B') as bronze "
				      + "FROM participates WHERE athlete_id = ? ;";
		
		 Connection conn = null;
	     PreparedStatement stmt = null;
	     PreparedStatement stmt1 = null;
	     PreparedStatement stmt2 = null;
	     PreparedStatement stmt3 = null;
    	
    	try {
    		 conn = getConnection();
             stmt = conn.prepareStatement(memberDetails.toString());
             stmt1 = conn.prepareStatement(memberType.toString());
             stmt2 = conn.prepareStatement(Bookings.toString());
             stmt3 = conn.prepareStatement(medals.toString());
             stmt.setString(1, memberID);
             stmt1.setString(1, memberID);
             stmt2.setString(1, memberID);
             stmt3.setString(1, memberID);

             ResultSet rset = stmt.executeQuery();
           
			while(rset.next()){
				details.put("member_id", memberID);
     			details.put("title", rset.getString("title"));
     	    	details.put("first_name", rset.getString("given_names"));
     	    	details.put("family_name", rset.getString("family_name"));
     	    	details.put("country_name", rset.getString("country_name"));
     	    	details.put("residence", rset.getString("place_name"));
			 }
			 ResultSet aset = stmt1.executeQuery();

 			while(aset.next()){
 				details.put("member_type", aset.getString("member_type"));
 				//details.put("member_type", aset.getString("member_type"));
 				
 			}
            ResultSet numset = stmt2.executeQuery();

 			while (numset.next()){
 				details.put("num_bookings", numset.getInt("num_bookings"));
 			}
 			if(details.get("member_type").equals("staff") || details.get("member_type").equals("official")){
 				 details.put("num_gold", null);
	 	 		 details.put("num_silver", null);
	 	 		 details.put("num_bronze", null);	
 			}
 			else{
 	 			ResultSet athset = stmt3.executeQuery();
 	 			while (athset.next()){
 		 	 	 	 details.put("num_gold", athset.getInt("gold"));
 		 	 		 details.put("num_silver", athset.getInt("silver"));
 		 	 		 details.put("num_bronze", athset.getInt("bronze"));	
 		 	 	}
 			}

			conn.close();

		} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return details;
			//return new HashMap<String, Object>();
    }


    //////////  Events  //////////

    /**
     * Get all of the events listed in the olympics for a given sport
     *
     * @param sportname the ID of the sport we are filtering by
     * @return List of the events for that sport
     * @throws OlympicsDBException
     */
    public ArrayList<HashMap<String, Object>> getEventsOfSport(Integer sportname) throws OlympicsDBException {

    	/*
		  Gets all the events for a sport
		  Eg Basketball - Men's Basketball, Women's Basketball, Men's 1v1
		 */


		Connection conn = null;
        try {
            conn = getConnection();

			ArrayList<HashMap<String, Object>> events = new ArrayList<>();
			StringBuffer stringBuffer = new StringBuffer();
			stringBuffer.append("SELECT event_id, sport_id, event_name, event_gender, place_name, event_start ");
			stringBuffer.append("FROM event JOIN place ON (event.sport_venue = place.place_id) ");
			stringBuffer.append("WHERE sport_id = ? ");

			PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
            stmt.setInt(1, sportname);
            ResultSet rset = stmt.executeQuery();
//            ResultSet rset = stmt.executeQuery(
//                    "SELECT * " +
//                    "FROM event " +
//                    "WHERE sport_id = '" + sportname + "';"
//            );
            while (rset.next()) {
                HashMap<String,Object> event = new HashMap<>();

                event.put("event_id", rset.getInt(1));
				event.put("sport_id", rset.getInt(2));
                event.put("event_name", rset.getString(3));
                event.put("event_gender", rset.getString(4));
                event.put("sport_venue", rset.getString(5));
                event.put("event_start", rset.getTimestamp(6));
                events.add(event);
            }
            rset.close();
            conn.close();
			return events;
        } catch (Exception e) {
            throw new OlympicsDBException("Error getting events of this sport", e);
        }


    }

    /**
     * Retrieve the results for a single event
     * @param eventId the key of the event
     * @return a hashmap for each result in the event.
     * @throws OlympicsDBException
     */
    public ArrayList<HashMap<String, Object>> getResultsOfEvent(Integer eventId) throws OlympicsDBException {

		Connection conn = null;

        try {
            conn = getConnection();

			ArrayList<HashMap<String, Object>> results = new ArrayList<>();

            StringBuffer individual = new StringBuffer();
            individual.append("SELECT family_name, given_names, country_name, medal ");
			individual.append("FROM participates P JOIN member M ON (athlete_id = member_id) JOIN country C ON (M.country_code = C.country_code) ");
			individual.append("WHERE event_id = ? ORDER BY family_name ");

			PreparedStatement stmt_individual = conn.prepareStatement(individual.toString());
			stmt_individual.setInt(1, eventId);
			ResultSet rs_individual = stmt_individual.executeQuery();

			StringBuffer team = new StringBuffer();
			team.append("SELECT team_name, country_name, medal ");
			team.append("FROM team T JOIN country C ON (T.country_code = C.country_code) ");
			team.append("WHERE event_id = ? GROUP BY team_name, country_name, medal ORDER BY team_name");

			PreparedStatement stmt_team = conn.prepareStatement(team.toString());
			stmt_team.setInt(1, eventId);
			ResultSet rs_team = stmt_team.executeQuery();

            while (rs_individual.next()) {
                HashMap<String,Object> result = new HashMap<>();
                result.put("participant", rs_individual.getString(1) + ", " + rs_individual.getString(2));
                result.put("country_name", rs_individual.getString(3));
                String medal = null;
                if (rs_individual.getString(4) != null) {
					if (rs_individual.getString(4).equals("G")) {
						medal = "Gold";
					} else if (rs_individual.getString(4).equals("S")) {
						medal = "Silver";
					} else if (rs_individual.getString(4).equals("B")) {
						medal = "Bronze";
					}
				}
				result.put("medal", medal);
                results.add(result);
            }

			while (rs_team.next()) {
				HashMap<String,Object> result = new HashMap<>();
				result.put("participant", rs_team.getString(1));
				result.put("country_name", rs_team.getString(2));
				String medal = null;
				if (rs_team.getString(3) != null) {
					if (rs_team.getString(3).equals("G")) {
						medal = "Gold";
					} else if (rs_team.getString(3).equals("S")) {
						medal = "Silver";
					} else if (rs_team.getString(3).equals("B")) {
						medal = "Bronze";
					}
				}
				result.put("medal", medal);
				results.add(result);
			}

            rs_individual.close();
            rs_team.close();
            conn.close();

			return results;
        } catch (Exception e) {
            throw new OlympicsDBException("Error getting results of this event", e);
        }


    }


    ///////   Journeys    ////////

    /**
     * Array list of journeys from one place to another on a given date
     * @param journeyDate the date of the journey
     * @param fromPlace the origin, starting place.
     * @param toPlace the destination, place to go to.
     * @return a list of all journeys from the origin to destination
     */
    @SuppressWarnings("deprecation")
	ArrayList<HashMap<String, Object>> findJourneys(String fromPlace, String toPlace, Date journeyDate) throws OlympicsDBException {
    	
    	Connection conn = null;
    	
    	try {
    		
    		conn = getConnection();
    		
    		// 'Newington Sydney 2127 NSW'
    		// 'Olympic Blvd, Sydney Olympic Park NSW 2127'
    		// 30
    		// 5
    		// 2017
    		StringBuffer stringBuffer = new StringBuffer();
    		stringBuffer.append("SELECT journey_id, vehicle_code, F.place_name AS origin_name, T.place_name AS dest_name, depart_time, arrive_time, capacity - nbooked AS available_seats ");
    		stringBuffer.append("FROM ((Journey NATURAL JOIN Vehicle) JOIN Place F ON (from_place=F.place_id)) JOIN Place T ON (to_place=T.place_id) ");
    		stringBuffer.append("WHERE F.place_name = ? AND ");
    		stringBuffer.append("T.place_name = ? AND ");
    		stringBuffer.append("EXTRACT(DAY FROM depart_time) = ? AND ");
    		stringBuffer.append("EXTRACT(MONTH FROM depart_time) = ? AND ");
    		stringBuffer.append("EXTRACT(YEAR FROM depart_time) = ? ");
    		
    		PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
    		stmt.setString(1, fromPlace);
    		stmt.setString(2, toPlace);
    		stmt.setInt(3, journeyDate.getDate());
    		stmt.setInt(4, journeyDate.getMonth() + 1);
    		stmt.setInt(5, journeyDate.getYear() + 1900);
    		ResultSet rs = stmt.executeQuery();
    		
    		ArrayList<HashMap<String, Object>> journeys = new ArrayList<>();
    		
    		while (rs.next()) {
    			
    			HashMap<String, Object> journey = new HashMap<>();
    			
    			journey.put("journey_id", Integer.valueOf(rs.getInt("journey_id")));
    			journey.put("vehicle_code", rs.getString("vehicle_code"));
    			journey.put("origin_name", rs.getString("origin_name"));
    			journey.put("dest_name", rs.getString("dest_name"));
    			journey.put("when_departs", new Date(rs.getTimestamp("depart_time").getTime()));
    			journey.put("when_arrives", new Date(rs.getTimestamp("arrive_time").getTime()));
    			journey.put("available_seats", Integer.valueOf(rs.getInt("available_seats")));
    			
    			journeys.add(journey);
    			
    		}
    		
    		rs.close();
    		reallyClose(conn);
    		
    		return journeys;
    		
    	} catch (SQLException e) {
            throw new OlympicsDBException("Error finding journeys", e);
    	} finally {
    		reallyClose(conn);
    	}

    	// FIXME: Replace the following with REAL OPERATIONS!
    	/*
        ArrayList<HashMap<String, Object>> journeys = new ArrayList<>();

        HashMap<String,Object> journey1 = new HashMap<String,Object>();
        journey1.put("journey_id", Integer.valueOf(17));
        journey1.put("vehicle_code", "XYZ124");
        journey1.put("origin_name", "SIT");
        journey1.put("dest_name", "Olympic Park");
        journey1.put("when_departs", new Date());
        journey1.put("when_arrives", new Date());
        journey1.put("available_seats", Integer.valueOf(3));
        journeys.add(journey1);
        
        return journeys;
        */
    	
    }

	ArrayList<HashMap<String,Object>> getMemberBookings(String memberID) throws OlympicsDBException {

		/**
		 * Users can access a list of all their bookings, listed chronologically by the
		 * trip start time (newest first).
		 */

		ArrayList<HashMap<String,Object>> bookings = new ArrayList<HashMap<String,Object>>();
		Statement stmt = null;
		try {


			Connection conn = getConnection();
			stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery(
					"SELECT * " +
							"FROM olympics.bookings JOIN olympics.journey ON (journey_id) JOIN olympics.place WHERE (from_place = place_id) JOIN olympics.place WHERE (to_place = place_id)" +
							"WHERE booked_for = '" + memberID + "' ORDER BY depart_time;");
			HashMap<String,Object> booking = new HashMap<String,Object>();
			booking.put("journey_id", rset.getInt("journey_id")); //convert to Object???
			booking.put("vehicle_code", rset.getString(("vehicle_code"))); //char(8()
			booking.put("origin_name", "SIT"); //need to join with olympics.place
			booking.put("dest_name", "Olympic Park"); //need to join with olympics.place
			booking.put("when_departs", new Date());
			booking.put("when_arrives", new Date());
			bookings.add(booking);

			reallyClose(conn);
			return bookings;
		} catch (Exception e) {
			throw new OlympicsDBException("Error getting member bookings", e);
		}




//        // FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
//        HashMap<String,Object> bookingex1 = new HashMap<String,Object>();
//        bookingex1.put("journey_id", Integer.valueOf(17));
//        bookingex1.put("vehicle_code", "XYZ124");
//        bookingex1.put("origin_name", "SIT");
//        bookingex1.put("dest_name", "Olympic Park");
//        bookingex1.put("when_departs", new Date());
//        bookingex1.put("when_arrives", new Date());
//        bookings.add(bookingex1);
//
//        HashMap<String,Object> bookingex2 = new HashMap<String,Object>();
//        bookingex2.put("journey_id", Integer.valueOf(25));
//        bookingex2.put("vehicle_code", "ABC789");
//        bookingex2.put("origin_name", "Olympic Park");
//        bookingex2.put("dest_name", "Sydney Airport");
//        bookingex2.put("when_departs", new Date());
//        bookingex2.put("when_arrives", new Date());
//        bookings.add(bookingex2);


	}
                
    /**
     * Get details for a specific journey
     * 
     * @return Various details of journey - see JourneyDetails.java
     * @throws OlympicsDBException
     */
    public HashMap<String,Object> getJourneyDetails(Integer journeyId) throws OlympicsDBException {
    	
    	Connection conn = null;
    	
    	try {
    		
    		conn = getConnection();
    		
    		// 129
    		StringBuffer stringBuffer = new StringBuffer();
    		stringBuffer.append("SELECT journey_id, vehicle_code, F.place_name, T.place_name, depart_time, arrive_time, capacity, nbooked ");
    		stringBuffer.append("FROM ((Journey NATURAL JOIN Vehicle) JOIN Place F ON (from_place=F.place_id)) JOIN Place T ON (to_place=T.place_id) ");
    		stringBuffer.append("WHERE journey_id = ? ");
    		
    		PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
    		stmt.setInt(1, journeyId);
    		ResultSet rs = stmt.executeQuery();
    		
    		HashMap<String, Object> journey = new HashMap<>();
    			
    		if (rs.next()) {
	    		journey.put("journey_id", Integer.valueOf(rs.getInt(1)));
	    		journey.put("vehicle_code", rs.getString(2));
	    		journey.put("origin_name", rs.getString(3));
	    		journey.put("dest_name", rs.getString(4));
	    		journey.put("when_departs", rs.getTimestamp(5));
	    		journey.put("when_arrives", rs.getTimestamp(6));
	    		journey.put("capacity", Integer.valueOf(rs.getInt(7)));
	    		journey.put("nbooked", Integer.valueOf(rs.getInt(8)));
    		}
    			
    		rs.close();
    		
    		return journey;
    		
    	} catch (Exception e) {
            throw new OlympicsDBException("Error finding journey", e);
    	} finally {
    		reallyClose(conn);
    	}
    	
        // FIXME: REPLACE FOLLOWING LINES WITH REAL OPERATION
    	/*
    	HashMap<String,Object> details = new HashMap<String,Object>();

    	details.put("journey_id", Integer.valueOf(17));
    	details.put("vehicle_code", "XYZ124");
        details.put("origin_name", "SIT");
        details.put("dest_name", "Olympic Park");
        details.put("when_departs", new Date());
        details.put("when_arrives", new Date());
        details.put("capacity", Integer.valueOf(6));
        details.put("nbooked", Integer.valueOf(3));
    	
        return details;
        */
    	
    }
    
    @SuppressWarnings("deprecation")
	public HashMap<String,Object> makeBooking(String byStaff, String forMember, String vehicle, Date departs) throws OlympicsDBException {
    	
    	Connection conn = null;
    	
    	try {
    		
			conn = getConnection();
			
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			conn.setAutoCommit(false);
			
			boolean isStaff = isStaff(conn, byStaff);
			if (!isStaff) {
				conn.rollback();
				reallyClose(conn);
				return null;
			}
			
			int seatsAvailable = seatsAvailable(conn, vehicle, departs);
			if (seatsAvailable < 1) {
				conn.rollback();
				reallyClose(conn);
				return null;
			}
			
			int journeyId = getJourneyId(conn, vehicle, departs);
			long whenBooked = System.currentTimeMillis();
			insertBooking(conn, forMember, byStaff, whenBooked, journeyId);
			incrementNbooked(conn, journeyId);
			
			conn.commit();
			
			conn.setAutoCommit(true);
			
			/*
			// A000024883
			// 3
			StringBuffer stringBuffer = new StringBuffer();
			stringBuffer.append("SELECT T.place_name AS to_place_name, F.place_name AS from_place_name, given_names AS booked_by ");
			stringBuffer.append("FROM (((Booking JOIN Journey USING (journey_id)) ");
			stringBuffer.append("JOIN Place F ON (from_place = F.place_id)) ");
			stringBuffer.append("JOIN Place T ON (to_place = T.place_id)) ");
			stringBuffer.append("JOIN Member M ON (booked_by = member_id) ");
			stringBuffer.append("WHERE booked_for = ? AND ");
			stringBuffer.append("journey_id = ? ");
			
			PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
			stmt.setString(1, forMember);
			stmt.setInt(2, journeyId);
			
			ResultSet rs = stmt.executeQuery();
			
			if (!rs.next()) {
				return null;
			}
			
			HashMap<String, Object> booking = new HashMap<>();
			
			booking.put("vehicle", vehicle);
		
			stringBuffer = new StringBuffer();
			stringBuffer.append(departs.getDay());
			stringBuffer.append('/');
			stringBuffer.append(departs.getMonth() + 1);
			stringBuffer.append('/');
			stringBuffer.append(departs.getYear() + 1900);
			booking.put("start_day", stringBuffer.toString());
			
			booking.put("start_time", departs);
			
			booking.put("to", rs.getString(1));
			booking.put("from", rs.getString(2));
			booking.put("booked_by", rs.getString(3));
			
			booking.put("when_booked", new Date(whenBooked));
			
			return booking;
			*/
			
			return getBookingDetails(forMember, journeyId);
			
		} catch (SQLException e) {
			reallyRollback(conn);
			reallyClose(conn);
			e.printStackTrace();
			throw new OlympicsDBException("Error making booking", e);
		} catch (OlympicsDBException e) {
			reallyRollback(conn);
			reallyClose(conn);
			e.printStackTrace();
			throw e;
		}
    	
    	/*
    	HashMap<String,Object> booking = null;
    	
        // FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
    	booking = new HashMap<String,Object>();
        booking.put("vehicle", "TR870R");
    	booking.put("start_day", "21/12/2020");
    	booking.put("start_time", new Date());
    	booking.put("to", "SIT");
    	booking.put("from", "Wentworth");
    	booking.put("booked_by", "Mike");
    	booking.put("whenbooked", new Date());
    	return booking;
    	*/
    	
    }
    
    private boolean isStaff(Connection conn, String memberId) throws SQLException {
    	
		// A000022173
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("SELECT EXISTS ( ");
		stringBuffer.append("SELECT member_id ");
		stringBuffer.append("FROM Staff ");
		stringBuffer.append("WHERE member_id = ? ");
		stringBuffer.append(") ");
		
		PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
		stmt.setString(1, memberId);
		
		ResultSet rs = stmt.executeQuery();
		rs.next();
		boolean isStaff = rs.getBoolean(1);
		
		rs.close();
		return isStaff;
    	
    }
    
    private int seatsAvailable(Connection conn, String vehicleCode, Date departTime) throws SQLException {
    	
		// AY91AN39
		// 2017-05-30 13:30:00
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("SELECT capacity - nbooked AS seats_avail ");
		stringBuffer.append("FROM Journey JOIN Vehicle USING (vehicle_code) ");
		stringBuffer.append("WHERE vehicle_code = ? AND ");
		stringBuffer.append("depart_time = ? ");
		
		PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
		stmt.setString(1, vehicleCode);
		stmt.setTimestamp(2, new Timestamp(departTime.getTime()));
		
		ResultSet rs = stmt.executeQuery();
		rs.next();
		int seatsAvailable = rs.getInt(1);
		
		rs.close();
		return seatsAvailable;
    	
    }
    
    private int getJourneyId(Connection conn, String vehicleCode, Date departTime) throws SQLException {
    	
		// AY91AN39
		// 2017-05-30 13:30:00
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("SELECT journey_id ");
		stringBuffer.append("FROM Journey ");
		stringBuffer.append("WHERE vehicle_code = ? AND ");
		stringBuffer.append("depart_time = ? ");
		
		PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
		stmt.setString(1, vehicleCode);
		stmt.setTimestamp(2, new Timestamp(departTime.getTime()));
		
		ResultSet rs = stmt.executeQuery();
		rs.next();
		int journeyId = rs.getInt(1);
		
		rs.close();
		return journeyId;
    	
    }
    
    private void reallyRollback(Connection conn) {
    	if (conn != null) {
    		try {
				conn.rollback();
			} catch (SQLException ignore) {}
    	}
    }
    
    private void insertBooking(Connection conn, String memberId, String staffId, long whenBooked, int journeyId) throws SQLException {
    	
		// A000022173
		// A000022173
    	// 129
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append("INSERT INTO Booking ");
		stringBuffer.append("VALUES (?, ?, ?, ?) ");
		
		PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
		stmt.setString(1, memberId);
		stmt.setString(2, staffId);
		stmt.setTimestamp(3, new Timestamp(whenBooked));
		stmt.setInt(4, journeyId);
		
		stmt.executeUpdate();
		
    }
    
    private void incrementNbooked(Connection conn, int journeyId) throws SQLException {
    	
    	StringBuffer stringBuffer = new StringBuffer();
    	stringBuffer.append("UPDATE Journey ");
    	stringBuffer.append("SET nbooked = nbooked + 1 ");
    	stringBuffer.append("WHERE journey_id = ? ");
    	
    	PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
    	stmt.setInt(1, journeyId);
    	
    	stmt.executeUpdate();
    	
    }

	public HashMap<String,Object> getBookingDetails(String memberId, Integer journeyId) throws OlympicsDBException {

    	/*
    	* Users can browse a specific journey, including details for when the booking was
    	* made and by whom
    	* */
		
    	Connection conn = null;
    	
    	try {
    		
    		conn = getConnection();
    		
    		// A000024883
    		// 3
    		StringBuffer stringBuffer = new StringBuffer();
    		stringBuffer.append("SELECT journey_id, vehicle_code, depart_time, ");
    		stringBuffer.append("T.place_name AS to_place_name, ");
    		stringBuffer.append("F.place_name AS from_place_name, ");
    		stringBuffer.append("BB.family_name AS booked_by_family_name, ");
    		stringBuffer.append("BB.given_names AS booked_by_given_names, ");
    		stringBuffer.append("BF.family_name AS booked_for_family_name, ");
    		stringBuffer.append("BF.given_names AS booked_for_given_names, ");
    		stringBuffer.append("when_booked, arrive_time ");
    		stringBuffer.append("FROM (((((Booking JOIN Journey USING (journey_id))) ");
    		stringBuffer.append("JOIN Place F ON (from_place = F.place_id)) ");
    		stringBuffer.append("JOIN Place T ON (to_place = T.place_id)) ");
    		stringBuffer.append("JOIN Member BB ON (booked_by = BB.member_id)) ");
    		stringBuffer.append("JOIN Member BF ON (booked_for = BF.member_id) ");
    		stringBuffer.append("WHERE booked_for = ? AND journey_id = ? ");
    		
    		PreparedStatement stmt = conn.prepareStatement(stringBuffer.toString());
    		stmt.setString(1, memberId);
    		stmt.setInt(2, journeyId);
    		ResultSet rs = stmt.executeQuery();
    		
    		HashMap<String, Object> booking = new HashMap<>();
    			
    		if (rs.next()) {
	    		booking.put("journey_id", Integer.valueOf(rs.getInt("journey_id")));
	    		booking.put("vehicle", rs.getString("vehicle_code"));
	    		//booking.put("vehicle_code", rs.getString("vehicle_code"));
	    		booking.put("when_departs", new Date(rs.getTimestamp("depart_time").getTime()));
	    		booking.put("dest_name", rs.getString("to_place_name"));
	    		booking.put("origin_name", rs.getString("from_place_name"));
	    		booking.put("bookedby_name", rs.getString("booked_by_family_name") + ", " + rs.getString("booked_by_given_names"));
	    		booking.put("bookedfor_name", rs.getString("booked_for_family_name") + ", " + rs.getString("booked_for_given_names"));
	    		booking.put("when_booked", new Date(rs.getTimestamp("when_booked").getTime()));
	    		booking.put("when_arrives", new Date(rs.getTimestamp("arrive_time").getTime()));
    		}
    			
    		rs.close();
    		
    		return booking;
    		
    	} catch (Exception e) {
            throw new OlympicsDBException("Error getting booking details", e);
    	} finally {
    		reallyClose(conn);
    	}

//
//        // FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
//
//
//    	booking.put("journey_id", journeyId);
//        booking.put("vehicle_code", "TR870R");
//    	booking.put("when_departs", new Date());
//    	booking.put("dest_name", "SIT");
//    	booking.put("origin_name", "Wentworth");
//    	booking.put("bookedby_name", "Mrs Piggy");
//    	booking.put("bookedfor_name", "Mike");
//    	booking.put("when_booked", new Date());
//    	booking.put("when_arrives", new Date());
//
//
//        return booking;
	}
    
	public ArrayList<HashMap<String, Object>> getSports() throws OlympicsDBException {
		ArrayList<HashMap<String,Object>> sports = new ArrayList<HashMap<String,Object>>();
		
		String list = "SELECT * FROM sport;";
		
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.prepareStatement(list.toString());
            
            ResultSet rset = stmt.executeQuery();

			 while(rset.next()){
				 HashMap<String,Object> sport = new HashMap<String,Object>();
				 sport.put("sport_id",  rset.getInt("sport_id"));
				 sport.put("sport_name",  rset.getString("sport_name"));
				 sport.put("discipline",  rset.getString("discipline"));
				 sports.add(sport);
			 }
		}
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sports;
	}


    /////////////////////////////////////////
    /// Functions below don't need
    /// to be touched.
    ///
    /// They are for connecting and handling errors!!
    /////////////////////////////////////////

    /**
     * Default constructor that simply loads the JDBC driver and sets to the
     * connection details.
     *
     * @throws ClassNotFoundException if the specified JDBC driver can't be
     * found.
     * @throws OlympicsDBException anything else
     */
    DatabaseBackend(InputStream config) throws ClassNotFoundException, OlympicsDBException {
    	Properties props = new Properties();
    	try {
			props.load(config);
		} catch (IOException e) {
			throw new OlympicsDBException("Couldn't read config data",e);
		}

    	dbUser = props.getProperty("username");
    	dbPass = props.getProperty("userpass");
    	String port = props.getProperty("port");
    	String dbname = props.getProperty("dbname");
    	String server = props.getProperty("address");;
    	
        // Load JDBC driver and setup connection details
    	String vendor = props.getProperty("dbvendor");
		if(vendor==null) {
    		throw new OlympicsDBException("No vendor config data");
    	} else if ("postgresql".equals(vendor)) { 
    		Class.forName("org.postgresql.Driver");
    		connstring = "jdbc:postgresql://" + server + ":" + port + "/" + dbname;
    	} else if ("oracle".equals(vendor)) {
    		Class.forName("oracle.jdbc.driver.OracleDriver");
    		connstring = "jdbc:oracle:thin:@" + server + ":" + port + ":" + dbname;
    	} else throw new OlympicsDBException("Unknown database vendor: " + vendor);
		
		// test the connection
		Connection conn = null;
		try {
			conn = getConnection();
		} catch (SQLException e) {
			throw new OlympicsDBException("Couldn't open connection", e);
		} finally {
			reallyClose(conn);
		}
    }

	/**
	 * Utility method to ensure a connection is closed without 
	 * generating any exceptions
	 * @param conn Database connection
	 */
	private void reallyClose(Connection conn) {
		if(conn!=null)
			try {
				conn.close();
			} catch (SQLException ignored) {}
	}

    /**
     * Construct object with open connection using configured login details
     * @return database connection
     * @throws SQLException if a DB connection cannot be established
     */
    private Connection getConnection() throws SQLException {
        Connection conn;
        conn = DriverManager.getConnection(connstring, dbUser, dbPass);
        return conn;
    }


    
}
