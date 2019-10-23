package main.java.input.implementation;

import main.java.common.implementation.NodeResource;
import main.java.common.implementation.Quad;
import main.java.common.interfaces.IQuint;
import main.java.input.interfaces.IQuintSource;
import main.java.input.interfaces.IQuintSourceListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A {@link IQuintSource} reading quads in the N-Triple or N-Quad format from
 * files
 *
 * @author Bastian
 * @author Till
 */
public class FileQuadSource implements IQuintSource {
    private static final Logger logger = LogManager.getLogger(FileQuadSource.class.getSimpleName());
    private long counter = 0;

    private List<IQuintSourceListener> listeners;
    private List<Path> filePaths;
    private NodeResource defaultContext;
    private FileFilter fileFilter;

    /**
     * Constructor
     *
     * @param filePaths      Paths containing the input files. May be either files or
     *                       directories
     * @param recursive      If true, subdirectories will be included as well, otherwise
     *                       not
     * @param defaultContext Context to be inserted if no context was given for an instance
     */
    public FileQuadSource(List<String> filePaths, boolean recursive,
                          String defaultContext, FileFilter... fileFilters) {
        listeners = new ArrayList<>();
        this.defaultContext = new NodeResource(new Resource(defaultContext));
        this.filePaths = new ArrayList<>();
        for (String s : filePaths) {
            Path path = Paths.get(s).toAbsolutePath();
            // Continue if given path is nonexistant
            if (!Files.exists(path)) {
                logger.warn("\"" + path + "\" file not found!");
                continue;
            }

            // In case of a given directory -> check insides but disregards
            // following folders
            if (Files.isDirectory(path)) {
                traverseDirectory(path, recursive, this.filePaths);

            } else if (Files.isRegularFile(path)) {
                this.filePaths.add(path);
            }
        }
        if (fileFilters != null && fileFilters.length > 0)
            fileFilter = fileFilters[0];
    }

    private void traverseDirectory(Path dir, boolean recursive, List<Path> paths) {
        Queue<Path> q = new ArrayDeque<>();
        q.add(dir);
        while (!q.isEmpty()) {
            Path element = q.poll();
            try {
                DirectoryStream<Path> d = Files.newDirectoryStream(element);
                for (Path p : d) {
                    if (Files.isRegularFile(p)) {
                        paths.add(p);
                    } else if (Files.isDirectory(p)) {
                        if (recursive) {
                            q.add(p);
                        }
                    }
                }
            } catch (IOException e) {
            }
        }

    }

    @Override
    public void close() {
        logger.info("Source Closed, processed " + counter + " quints");
        for (IQuintSourceListener i : listeners)
            i.sourceClosed();

    }

    @Override
    public void start() {
        if (fileFilter != null) {
            List<Path> filteredPaths = new LinkedList<>();
            filePaths.forEach(FPath -> {
                if (fileFilter.accept(FPath.toFile()))
                    filteredPaths.add(FPath);
            });
            filePaths = filteredPaths;
        }

        for (IQuintSourceListener i : listeners)
            i.sourceStarted();

        //NxParser nxp = new NxParser();
        Iterator<Path> paths = filePaths.iterator();
        while (paths.hasNext()) {
            Path p = paths.next();
            logger.debug("Processing: " + p.toString());
            FileInputStream is = null;
            try {
                InputStream in = Files.newInputStream(p);

                if (p.toString().endsWith(".zip")) {
                    ZipInputStream zin = new ZipInputStream(in);
                    ZipEntry entry;
                    while ((entry = zin.getNextEntry()) != null) {
                        NxParser parser = new NxParser();
                        parser.parse(new BufferedInputStream(zin));
                        iterateParser(parser);
                    }
                } else {
                    if (p.toString().endsWith(".gz"))
                        in = new GZIPInputStream(in);

                    NxParser parser = new NxParser();
                    parser.parse(new BufferedInputStream(new BufferedInputStream(in)));
                    iterateParser(parser);
                }
                // Close stream
                in.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        close();
    }

    private void iterateParser(NxParser parser) {
        while (parser.hasNext()) {
            try {
                Node[] nodes = parser.next();
                counter++;
                if (nodes.length < 3)
                    continue;

                // get nodes
                Node subject = nodes[0];
                Node predicate = nodes[1];
                Node object = nodes[2];

                IQuint quint = null;

                // Check whether context was present
                if (nodes.length == 3) {
                    quint = new Quad(new NodeResource(subject), new NodeResource(
                            predicate), new NodeResource(object), defaultContext);
                } else if (nodes.length == 4) {
                    Node context = nodes[3];
                    quint = new Quad(new NodeResource(subject), new NodeResource(
                            predicate), new NodeResource(object), new NodeResource(
                            context));

                }

                // If something happened, continue
                if (quint == null)
                    continue;

                // Notify listeners
                for (IQuintSourceListener l : listeners)
                    l.pushedQuint(quint);

            } catch (Exception e) {
                continue;
            }
        }

    }

    @Override
    public void registerQuintListener(IQuintSourceListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeQuintListener(IQuintSourceListener listener) {
        listeners.remove(listener);
    }

}
