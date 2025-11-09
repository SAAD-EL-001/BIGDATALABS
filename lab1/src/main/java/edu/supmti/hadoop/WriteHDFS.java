package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        // Vérifiez que les arguments sont fournis
        if (args.length != 2) { // On attend 2 arguments : le chemin du fichier et le texte à écrire
            System.err.println("Usage: WriteHDFS <hdfs_file_path> <text_to_write>");
            System.exit(1);
        }

        String filePathStr = args[0]; // Le chemin du fichier est le premier argument
        String textToWrite = args[1]; // Le texte à écrire est le deuxième argument

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(filePathStr);

        // Créer le fichier (écrase s'il existe déjà)
        FSDataOutputStream outStream = fs.create(filePath, true); // true = overwrite if exists
        outStream.writeUTF("Bonjour tout le monde!\n"); // Écrit la première ligne
        outStream.writeUTF(textToWrite); // Écrit le texte fourni en argument
        outStream.close();

        fs.close();
        System.out.println("File written successfully to " + filePathStr);
    }
}