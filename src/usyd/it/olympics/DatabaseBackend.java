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
        
//        String stringBuffer = "SELECT *, CASE WHEN member.member_id = athlete.member_id THEN 'athlete' " +
//        									"WHEN member.member_id = staff.member_id THEN 'staff' " +
//        									"WHEN member.member_id = official.member_id THEN 'offical' "+
//        									"END as member_type " +
//        					"FROM olympics.member JOIN olympics.country USING (country_code) " +
//        										"JOIN olympics.accommodation ON (accommodation = place_id) " +
//        										"JOIN olympics.place USING (place_id) " +
//        										"LEFT OUTER JOIN olympics.athlete USING (member_id) " +
//        										"LEFT OUTER JOIN olympics.staff USING (member_id) " +
//        										"LEFT OUTER JOIN olympics.official USING (member_id) " + 
//        										"WHERE member_id = ?";
        
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
        			
        			String title = rset.getString("title");
        			String first_name = rset.getString("given_names");
        			String family_name = rset.getString("family_name");
        			String country_name = rset.getString("country_name");
        			String residence = rset.getString("place_name");
        			String member_type = rset.getString("member_type");
        			
//        			ResultSet aset = stmt.executeQuery("SELECT CASE WHEN member.member_id = athlete.member_id THEN 'athlete' WHEN member.member_id = staff.member_id THEN 'staff' WHEN member.member_id = official.member_id THEN 'offical' END as member_type FROM member LEFT OUTER JOIN athlete USING (member_id) LEFT OUTER JOIN staff USING (member_id) LEFT OUTER JOIN official USING (member_id) WHERE member.member_id='"+member+"';");
//        			while(aset.next()){
//        				member_type = aset.getString("member_type");
//        			}
        			//System.out.println(rset.getString("place_name") + "  " + rset.getString("country_name")+ " " + rset.getString(3));
        			
        			details = new HashMap<String,Object>();
        			// Populate with record data
        			details.put("member_id", member);
        			details.put("title", title);
        			details.put("first_name", first_name);
        			details.put("family_name", family_name);
        			details.put("country_name", country_name);
        			details.put("residence", residence );
//        			details.put("member_type", "athlete");
        			details.put("member_type", member_type);
        		}
        	} reallyClose(conn);
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
    	Connection conn;
		
    	String title = "";
		String first_name = "";
		String family_name = "";
		String country_name = "";
		String residence = "";
    	String member_type =  "";
		String num_booking = "";
    	
    	
    	try {
			conn = getConnection();
			Statement stmt = conn.createStatement();
//			 ResultSet rset = stmt.executeQuery("SELECT * FROM member JOIN country USING (country_code) JOIN accommodation ON (accommodation = place_id) JOIN place USING (place_id) WHERE member_id='"+memberID+"';");
			 ResultSet rset = stmt.executeQuery("SELECT * " +
												"FROM olympics.member " +
													"JOIN olympics.country USING (country_code) "+
													"JOIN olympics.accommodation ON (accommodation = place_id) " + 
													"JOIN olympics.place USING (place_id) " + 
													"WHERE member_id='"+memberID+"';");
			
			while(rset.next()){
				 title = rset.getString("title");
     			 first_name = rset.getString("given_names");
     			 family_name = rset.getString("family_name");
     			 country_name = rset.getString("country_name");
     			 residence = rset.getString("place_name");
			 }
			
  			ResultSet aset = stmt.executeQuery("SELECT CASE WHEN member.member_id = athlete.member_id THEN 'athlete' WHEN member.member_id = staff.member_id THEN 'staff' WHEN member.member_id = official.member_id THEN 'offical' END as member_type FROM olympics.member LEFT OUTER JOIN olympics.athlete USING (member_id) LEFT OUTER JOIN olympics.staff USING (member_id) LEFT OUTER JOIN olympics.official USING (member_id) WHERE member.member_id='"+memberID+"';");
 			while(aset.next()){
 				member_type = aset.getString("member_type");
 			}
 			
 			ResultSet numset = stmt.executeQuery("SELECT COUNT(*) as num_bookings FROM olympics.booking WHERE booked_for LIKE'"+memberID+"';");
 			while (numset.next()){
 				num_booking = numset.getString("num_bookings");
 			}
 			
 			details.put("member_id", memberID);
 	    	details.put("member_type", member_type);
 	    	details.put("title", title);
 	    	details.put("first_name", first_name);
 	    	details.put("family_name", family_name);
 	    	details.put("country_name", country_name);
 	    	details.put("residence", residence);
 	    	details.put("member_type", member_type);
 	    	details.put("num_bookings", num_booking);
 			
 			ResultSet athset = stmt.executeQuery("SELECT COUNT(medal = 'G') as gold, COUNT(medal = 'S') as silver, COUNT(medal = 'B') as bronze FROM olympics.participates WHERE athlete_id ='"+memberID+"';");
 	 		while (athset.next()){
 	 			details.put("num_gold", athset.getString("gold"));
 	 		    details.put("num_silver", athset.getString("silver"));
 	 		    details.put("num_bronze", athset.getString("bronze"));	
 	 		} reallyClose(conn);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return details;
    }


    //////////  Events  //////////

    /**
     * Get all of the events listed in the olympics for a given sport
     *
     * @param sportname the ID of the sport we are filtering by
     * @return List of the events for that sport
     * @throws OlympicsDBException
     */
    ArrayList<HashMap<String, Object>> getEventsOfSport(Integer sportname) throws OlympicsDBException {
        // FIXME: Replace the following with REAL OPERATIONS!

        ArrayList<HashMap<String, Object>> events = new ArrayList<>();

        Statement stmt = null;
        try {
            Connection conn = getConnection();
            stmt = conn.createStatement();
            ResultSet rset = stmt.executeQuery(
                    "SELECT * " +
                    "FROM event " +
                    "WHERE sport_id = '" + sportname + "';"
            );
            while (rset.next()) {
                HashMap<String,Object> event = new HashMap<>();
                event.put("event_id", rset.getInt("event_id"));
                event.put("sport_id", rset.getInt("sport_id"));
                event.put("event_name", rset.getString("event_name"));
                event.put("event_gender", rset.getString("event_gender"));
                event.put("sport_venue", rset.getInt("sport_venue"));
                event.put("event_start", rset.getTimestamp("event_start"));
                events.add(event);
            } reallyClose(conn);
        } catch (Exception e) {
            throw new OlympicsDBException("Error getting events of this sport", e);
        }

        return events;
    }

    /**
     * Retrieve the results for a single event
     * @param eventId the key of the event
     * @return a hashmap for each result in the event.
     * @throws OlympicsDBException
     */
    ArrayList<HashMap<String, Object>> getResultsOfEvent(Integer eventId) throws OlympicsDBException {
        // FIXME: Replace the following with REAL OPERATIONS!

    	ArrayList<HashMap<String, Object>> results = new ArrayList<>();
        Statement stmt = null;
        try {
            Connection conn = getConnection();
            stmt = conn.createStatement();

            ResultSet rset = stmt.executeQuery(
                    "SELECT * " +
                    "FROM participates JOIN member WHERE (athlete_id = member_id) " +
                    "WHERE event_id = '" + eventId + "';");
            while (rset.next()) {
                HashMap<String,Object> result = new HashMap<>();
                result.put("participant", rset.getString("family_name") + " " + rset.getString("given_names"));
                result.put("country_name", rset.getString("country_code"));
                result.put("medal", rset.getString("medal_character"));
                results.add(result);
            } reallyClose(conn);
        } catch (Exception e) {
            throw new OlympicsDBException("Error getting results of this event", e);
        }

        return results;
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
    		stringBuffer.append("SELECT journey_id, vehicle_code, F.place_name, T.place_name, depart_time, arrive_time, capacity - nbooked AS available_seats ");
    		stringBuffer.append("FROM ((Journey NATURAL JOIN Vehicle) JOIN Place F ON (from_place=F.place_id)) JOIN Place T ON (to_place=T.place_id) ");
    		stringBuffer.append("WHERE F.address = ? AND ");
    		stringBuffer.append("T.address = ? AND ");
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
    			
    			journey.put("journey_id", Integer.valueOf(rs.getInt(1)));
    			journey.put("vehicle_code", rs.getString(2));
    			journey.put("origin_name", rs.getString(3));
    			journey.put("dest_name", rs.getString(4));
    			journey.put("when_departs", rs.getTimestamp(5));
    			journey.put("when_arrives", rs.getTimestamp(6));
    			journey.put("available_seats", Integer.valueOf(rs.getInt(7)));
    			
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

			while (rset.next()) {
				HashMap<String,Object> booking = new HashMap<String,Object>();
				booking.put("journey_id", rset.getInt("journey_id")); //convert to Object???
				booking.put("vehicle_code", rset.getString(("vehicle_code"))); //char(8()
				booking.put("origin_name", "SIT"); //need to join with olympics.place
				booking.put("dest_name", "Olympic Park"); //need to join with olympics.place
				booking.put("when_departs", new Date());
				booking.put("when_arrives", new Date());
				bookings.add(booking);
			} reallyClose(conn);
		} catch (Exception e) {
			throw new OlympicsDBException("Error getting member bookings", e);
		}

		return bookings;

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
			reallyClose(conn);
    		return journey;
    		
    	} catch (Exception e) {
            throw new OlympicsDBException("Error finding journey", e);
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
			
		} catch (Exception e) {
			reallyRollback(conn);
			reallyClose(conn);
			e.printStackTrace();
			throw new OlympicsDBException("Error making booking", e);
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

	public HashMap<String,Object> getBookingDetails(String memberID, Integer journeyId) throws OlympicsDBException {

    	/*
    	* Users can browse a specific journey, including details for when the booking was
    	* made and by whom
    	* */

		HashMap<String,Object> booking = null;
		Statement stmt = null;
		try {
			Connection conn = getConnection();
			stmt = conn.createStatement();
			ResultSet rset = stmt.executeQuery(
					"SELECT * " +
					"FROM olympics.bookings JOIN olympics.journey ON (journey_id) JOIN olympics.member WHERE (athlete_id = member_id)" +
					"WHERE booked_for = '" + memberID + "' AND journey_id = '" + journeyId + "' " +
					"ORDER BY depart_time;");
			booking = new HashMap<String,Object>();
			booking.put("journey_id", rset.getInt("journey_id")); //convert to Object???
			booking.put("vehicle_code", rset.getString(("vehicle_code"))); //char(8()
			booking.put("when_departs", new Date());
			booking.put("dest_name", "Olympic Park"); //need to join with olympics.place
			booking.put("origin_name", "SIT"); //need to join with olympics.place
			booking.put("bookedby_name", rset.getString("title") + " " + rset.getString("family_name"));
			booking.put("bookedfor_name", rset.getString("given_names") + " " + rset.getString("family_name"));
			booking.put("when_booked", new Date());
			booking.put("when_arrives", new Date());
			reallyClose(conn);

		} catch (Exception e) {
			throw new OlympicsDBException("Error getting member bookings", e);
		} return booking;

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
		
		// FIXME: DUMMY FUNCTION NEEDS TO BE PROPERLY IMPLEMENTED
		HashMap<String,Object> sport1 = new HashMap<String,Object>();
		sport1.put("sport_id", Integer.valueOf(1));
		sport1.put("sport_name", "Chillaxing");
		sport1.put("discipline", "Couch Potatoing");
		sports.add(sport1);
		
		HashMap<String,Object> sport2 = new HashMap<String,Object>();
		sport2.put("sport_id", Integer.valueOf(2));
		sport2.put("sport_name", "Frobnicating");
		sport2.put("discipline", "Tweaking");
		sports.add(sport2);
		
		HashMap<String,Object> sport3 = new HashMap<String,Object>();
		sport3.put("sport_id", Integer.valueOf(3));
		sport3.put("sport_name", "Frobnicating");
		sport3.put("discipline", "Fiddling");
		sports.add(sport3);
		
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
