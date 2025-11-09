package edu.supmti.hadoop;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        // Vérifiez que les arguments *utiles* sont fournis (nom de classe + 3 args)
        if (args.length != 4) { // <--- Changé de 3 à 4
            System.err.println("Usage: HadoopFileStatus <path> <file_name> <new_file_name>");
            System.exit(1);
        }

        // Les arguments réels commencent à l'indice 1
        String pathStr = args[1]; // <--- Changé de args[0] à args[1]
        String fileName = args[2]; // <--- Changé de args[1] à args[2]
        String newFileName = args[3]; // <--- Changé de args[2] à args[3]

        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);

            Path filepath = new Path(pathStr, fileName); // Utilisez les arguments
            FileStatus status = fs.getFileStatus(filepath); // Utilisez 'status' au lieu de 'infos'

            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());

            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Renommer le fichier
            Path newFilepath = new Path(pathStr, newFileName);
            boolean renamed = fs.rename(filepath, newFilepath);
            if (renamed) {
                System.out.println("File renamed successfully from " + fileName + " to " + newFileName);
            } else {
                System.out.println("Failed to rename file.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}