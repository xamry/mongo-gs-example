package com.impetus.code.examples;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.impetus.code.Places;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * Example code for Geospatial queries in MongoDB
 * 
 * @author amresh.singh
 */
public class GeospatialExample 
{
    public static final String dbName = "geospatial";
    public static final String host = "127.0.0.1";
    public static final int port = 27017;
    public static final String collectionName = "places";
    public static final String indexName = "geospatialIdx";
    
    Mongo mongo;
    DBCollection collection;
    
    private Mongo getMongo() {        
        try
        {
            mongo = new Mongo(new DBAddress(host, port, dbName));
        }
        catch (MongoException e)
        {
            e.printStackTrace();
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
        }
        return mongo;
    }
    
    public static void main(String[] args)
    {
        new GeospatialExample().runExample();

    }

    private void runExample()
    {
        collection = getMongo().getDB(dbName).getCollection(collectionName);
        collection.ensureIndex(new BasicDBObject("loc", "2d"), indexName);

        // addPlaces();
        //findWithinCircle();
        //findWithinBox();
        //findWithinPolygon();
        findCenterSphere();
        //findNear();
        //findNearSphere();

    }

    private void findWithinCircle()
    {
        List circle = new ArrayList();
        circle.add(new double[] { 5, 5 }); // Centre of circle
        circle.add(1); // Radius
        BasicDBObject query = new BasicDBObject("loc", new BasicDBObject("$within",
                new BasicDBObject("$center", circle)));       

        printOutputs(query);

    }

    private void findWithinBox()
    {
        List box = new ArrayList();
        box.add(new double[] { 4, 4 }); //Starting coordinate
        box.add(new double[]{6,6}); // Ending coordinate
        BasicDBObject query = new BasicDBObject("loc", new BasicDBObject("$within",
                new BasicDBObject("$box", box)));       

        printOutputs(query);

    }

    private void findWithinPolygon()
    {
        List polygon = new ArrayList();
        polygon.add(new double[] { 3, 3 }); //Starting coordinate
        polygon.add(new double[]{8,3}); // Ending coordinate
        polygon.add(new double[]{6,7}); // Ending coordinate
        BasicDBObject query = new BasicDBObject("loc", new BasicDBObject("$within",
                new BasicDBObject("$polygon", polygon)));       

        printOutputs(query);
    }
    
    private void findNear() {
        BasicDBObject filter = new BasicDBObject("$near", new double[] { 4, 4 });
        filter.put("$maxDistance", 2);

        BasicDBObject query = new BasicDBObject("loc", filter);
        
        printOutputs(query);
    }
    
    private void findNearSphere() {
        BasicDBObject filter = new BasicDBObject("$nearSphere", new double[] { 5, 5 });
        filter.put("$maxDistance", 0.06);
        // Radius of the earth: 3959.8728

        BasicDBObject query = new BasicDBObject("loc", filter);
        printOutputs(query);
    }
    
    private void findCenterSphere() {
        List circle = new ArrayList();
        circle.add(new double[] { 5, 5 }); // Centre of circle
        circle.add(0.06); // Radius
        BasicDBObject query = new BasicDBObject("loc", new BasicDBObject("$within",
                new BasicDBObject("$centerSphere", circle)));       

        printOutputs(query);
    }

    public void printOutputs(BasicDBObject query)
    {
        DBCursor cursor = collection.find(query);
        List<BasicDBList> outputs = new ArrayList<BasicDBList>();
        while (cursor.hasNext())
        {
            DBObject result = cursor.next();
            System.out.println(result.get("name") + "--->" + result.get("loc"));
            outputs.add((BasicDBList) result.get("loc"));
        }        
        
        for (int y = 9; y >= 0; y--)
        {
            String s = "";
            for (int x = 0; x < 10; x++)
            {
                boolean found = false;
                for (BasicDBList obj : outputs)
                {
                    double xVal = (Double) obj.get(0);
                    double yVal = (Double) obj.get(1);
                    if (yVal == y && xVal == x)
                    {
                        found = true;                        
                    }
                }
                if(found) {
                    s = s + " @";
                } else {
                    s = s + " +";
                }                            
            }
            System.out.println(s);
        }
    }
    
    

    private void addPlaces()
    {
        for (int i = 0; i < 100; i++)
        {
            double x = i % 10;
            double y = Math.floor(i / 10);
            addPlace(collection, Places.cities[i], new double[] { x, y });
        }
    }
    
    private void addPlace(DBCollection collection, String name, final double[] location)
    {
        final BasicDBObject place = new BasicDBObject();
        place.put("name", name);
        place.put("loc", location);        
        collection.insert(place);
    }

}
