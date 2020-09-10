package com.gs.poc.kafka;

import com.gs.poc.kafka.pojo.ControlPojo;
import org.openspaces.admin.Admin;
import org.openspaces.admin.AdminFactory;
import org.openspaces.admin.space.Space;
import org.openspaces.core.GigaSpace;


import java.util.concurrent.TimeUnit;

public class SpaceDataWriter {

    private GigaSpace gigaSpace;

    public SpaceDataWriter(){
        AdminFactory adminFactory = new AdminFactory();
        adminFactory.addGroups( "poc-group" );
        Admin admin = adminFactory.createAdmin();
        boolean managerFound = admin.getGridServiceManagers().waitFor( 1, 1, TimeUnit.MINUTES);
        if( !managerFound ){
            throw new RuntimeException( "Manager not found" );
        }
        boolean gscsFound = admin.getGridServiceContainers().waitFor( 2, 1, TimeUnit.MINUTES);
        if( !gscsFound ){
            throw new RuntimeException( "Containers were not found" );
        }

        Space space = admin.getSpaces().waitFor( "mySpace", 1, TimeUnit.MINUTES);
        if( space == null ){
            throw new RuntimeException( "Space not found" );
        }

        gigaSpace = space.getGigaSpace();
    }

    public void writeToSpace(ControlPojo[] pojos){
        if( gigaSpace != null ) {
            try {
                gigaSpace.writeMultiple(pojos);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }
}