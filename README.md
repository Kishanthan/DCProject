A simple distributed overlay network.

Prerequisites:

- Java 7 or 8.
- Apache Maven.


How to run :

1. Do maven clean build, which would generate the node.jar at target build directory. 
2. For convenience, the built jar files are added to the repository under /jars directory.
3. Copy the node.jar file into desired node machines.
4. Make sure that bootstrap server is running.
5. Execute the run command as given below :

````
    Example run command : "java -DbootstrapAddress=192.168.1.2:55555 -DnodeAddress=192.168.2.2:10001 -DnodeName=node1 -DhopsCount=5 -jar node.jar" 
    
    Parameter Info : 
    -DbootstrapAddress=<mandatory> 
    -DnodeAddress=<optional - default : localNodeIp:randomPortNumberFrom10000 > 
    -DnodeName=<optional - default : node+nodePort > 
    -DhopsCount=<optional - default : 4 >
````
