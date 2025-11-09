package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        System.out.println("DEBUG: Nombre d'arguments reçus: " + args.length); // Ligne de débogage
        System.out.println("DEBUG: Arguments: [" + String.join(", ", args) + "]"); // Ligne de débogage
        // Vérifiez que les arguments sont fournis (seulement le chemin du fichier)
        if (args.length != 1) { // Changé : attend maintenant 1 argument (le chemin du fichier)
            System.err.println("Usage: ReadHDFS <hdfs_file_path>");
            System.exit(1);
        }

        String filePathStr = args[0]; // Changé : le chemin du fichier est maintenant args[0]

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(filePathStr);

        // Vérifiez si le fichier existe
        if (!fs.exists(filePath)) {
             System.err.println("File does not exist: " + filePathStr);
             System.exit(1);
        }

        // Ouvrez le fichier pour lecture
        FSDataInputStream inStream = fs.open(filePath);
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);

        String line;
        // Lisez et affichez chaque ligne (modifié pour lire tout le fichier)
        while ((line = br.readLine()) != null) {
            System.out.println(line); // Affiche la ligne lue
        }

        // Fermez les flux
        br.close();
        isr.close();
        inStream.close();
        fs.close();
    }
}