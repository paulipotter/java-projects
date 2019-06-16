[ -e Query.class  ] && rm Query.class 
[ -e SQLassign.class  ] && rm SQLassign.class

javac SQLassign.java
javac Query.java

java -classpath "/usr/share/java/mysql.jar:/home/p/paulipotter/7/postgresql-9.4.1208.jre6.jar:." SQLassign uid passwd
