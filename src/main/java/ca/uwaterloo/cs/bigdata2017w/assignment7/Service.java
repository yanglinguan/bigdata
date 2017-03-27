package ca.uwaterloo.cs.bigdata2017w.assignment7;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.Map;

/**
 * Created by yanglinguan on 2017-03-27.
 */

@Path("")
public class Service {
    @Context
    Configuration config;

    @GET
    @Path("hello")
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
        Map<String, Object> property = config.getProperties();
        System.out.println("index: "  + property.get("index"));
        return "hello";
    }

}
