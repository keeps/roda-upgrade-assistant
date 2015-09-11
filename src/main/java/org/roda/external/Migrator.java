package org.roda.external;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.roda.model.ModelService;
import org.roda.storage.DefaultStoragePath;
import org.roda.storage.StorageService;
import org.roda.storage.StorageServiceException;
import org.roda.storage.fs.FSPathContentPayload;
import org.roda.storage.fs.FileStorageService;

import com.google.common.collect.Sets;

import pt.gov.dgarq.roda.core.Downloader;
import pt.gov.dgarq.roda.core.DownloaderException;
import pt.gov.dgarq.roda.core.RODAClient;
import pt.gov.dgarq.roda.core.common.BrowserException;
import pt.gov.dgarq.roda.core.common.LoginException;
import pt.gov.dgarq.roda.core.common.NoSuchRODAObjectException;
import pt.gov.dgarq.roda.core.common.RODAClientException;
import pt.gov.dgarq.roda.core.data.DescriptionObject;
import pt.gov.dgarq.roda.core.data.EventPreservationObject;
import pt.gov.dgarq.roda.core.data.RODAObject;
import pt.gov.dgarq.roda.core.data.RODAObjectPermissions;
import pt.gov.dgarq.roda.core.data.RepresentationFile;
import pt.gov.dgarq.roda.core.data.RepresentationObject;
import pt.gov.dgarq.roda.core.data.RepresentationPreservationObject;
import pt.gov.dgarq.roda.core.data.SimpleDescriptionObject;
import pt.gov.dgarq.roda.core.data.adapter.ContentAdapter;
import pt.gov.dgarq.roda.core.data.adapter.filter.Filter;
import pt.gov.dgarq.roda.core.data.adapter.filter.SimpleFilterParameter;
import pt.gov.dgarq.roda.core.data.adapter.sort.SortParameter;
import pt.gov.dgarq.roda.core.data.adapter.sort.Sorter;
import pt.gov.dgarq.roda.core.data.preservation.RepresentationFilePreservationObject;
import pt.gov.dgarq.roda.core.stubs.Browser;

/**
 * Class/command-line utility for migrating all AIPs from roda 1.x to 2.x
 * 
 * @author Sebastien Leroux <sleroux@keep.pt>
 * @author Hélder Silva <hsilva@keep.pt>
 */
public class Migrator implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(Migrator.class);
  private static final int AIPS_PER_CYCLE = 200;

  private RODAClient client;
  private Browser browser;
  private Downloader downloader;
  private StorageService fs;
  private ModelService ms;
  private int counter;
  public boolean sucessfulRun;
  private String rodaCoreUrl;
  private String rodaCoreUsername;
  private String rodaCorePassword;

  public Migrator(String rodaCoreUrl, String rodaCoreUsername, String rodaCorePassword, StorageService storage)
    throws LoginException, RODAClientException, StorageServiceException, IOException, DownloaderException {
    LOGGER.info("Creating client...");
    client = new RODAClient(new URL(rodaCoreUrl), rodaCoreUsername, rodaCorePassword);
    browser = client.getBrowserService();
    downloader = client.getDownloader();
    LOGGER.info("Client created...");
    this.fs = storage;
    this.ms = new ModelService(this.fs);
    this.counter = 0;
    this.sucessfulRun = true;
    this.rodaCoreUrl = rodaCoreUrl;
    this.rodaCoreUsername = rodaCoreUsername;
    this.rodaCorePassword = rodaCorePassword;
  }

  @Override
  public void run() {
    try {
      String[] pids;
      Set<String> agentPids = new HashSet<String>();
      sucessfulRun = true;

      LOGGER.info("Getting PIDS...");
      pids = client.getBrowserService().getDOPIDs();
      LOGGER.info("Obtained " + pids.length + " PIDS");

      for (String pid : pids) {
        long startPID = System.currentTimeMillis();
        boolean processAIP = true;
        processAIP = verifyIfAIPShouldBeProcessed(pid);
        if (processAIP) {
          processAIP(pid, agentPids, startPID);
        }
      }
    } catch (BrowserException | RemoteException | RODAClientException e) {
      LOGGER.error("Error obtaining AIPs PIDS", e);
    }
  }

  private void processAIP(String pid, Set<String> agentPids, long startPID) {
    try {
      // get DO and permissions
      RODAObjectPermissions doPermissions = client.getBrowserService().getRODAObjectPermissions(pid);
      DescriptionObject doDescriptionObject = client.getBrowserService().getDescriptionObject(pid);

      // create necessary folders for AIP
      createFoldersForAIP(pid, doPermissions, doDescriptionObject);

      // get and store descriptive metadata
      DefaultStoragePath binaryPath = DefaultStoragePath.parse("AIP", pid, "metadata", "descriptive", "ead-c.xml");
      Map<String, Set<String>> properties = new HashMap<String, Set<String>>();
      createBinaryFromStream(binaryPath, downloader.getFile(pid, "EAD-C"), properties, ".xml");

      // see if DO representations have preservation information
      RepresentationPreservationObject[] preservationObjects = browser.getDOPreservationObjects(pid);
      if (preservationObjects != null) {

        for (RepresentationPreservationObject pObj : preservationObjects) {
          String roPid = pObj.getRepresentationObjectPID();
          if (roPid != null) {
            // get RO and permissions
            RepresentationObject rObject = browser.getRepresentationObject(roPid);
            RODAObjectPermissions rPermissions = browser.getRODAObjectPermissions(rObject.getPid());

            createFoldersForPreservationObject(pid, pObj, rObject, rPermissions);

            copyDatastreams(pid, pObj, downloader, rPermissions);

            EventPreservationObject[] repEvents = browser.getPreservationEvents(pObj.getPid());
            if (repEvents != null) {
              for (EventPreservationObject repEvent : repEvents) {
                String eventPid = repEvent.getPid();
                binaryPath = DefaultStoragePath.parse("AIP", pid, "metadata", "preservation", roPid,
                  "event_" + eventPid.replace(':', '_') + ".premis.xml");
                properties = new HashMap<String, Set<String>>();
                createBinaryFromStream(binaryPath, downloader.getFile(eventPid, "PREMIS"), properties, ".premis.xml");
                agentPids.add(repEvent.getAgentPID());
                LOGGER.info(repEvent.getAgentPID());
              }
            }

            LOGGER.info("Save representations(doPidO=" + pid + ",roPID=" + roPid);
            saveRepresentationObject(pid, roPid, client);
          }

        }
      }
      // ms.retrieveAIP(pid);
      counter++;
      long stopPID = System.currentTimeMillis();
      if (counter % AIPS_PER_CYCLE == 0) {
        client = new RODAClient(new URL(rodaCoreUrl), rodaCoreUsername, rodaCorePassword);
        browser = client.getBrowserService();
        downloader = client.getDownloader();
      }

      LOGGER.info("AIP processed in " + ((stopPID - startPID) / 1000) + " s");
    } catch (Throwable t) {
      sucessfulRun = false;
      LOGGER.error("Error processing AIP with pid " + pid, t);
      try {
        ms.deleteAIP(pid);
      } catch (Throwable t1) {
        LOGGER.error("Error deleting AIP with pid " + pid, t1);
      }
    }
  }

  private void createFoldersForAIP(String pid, RODAObjectPermissions doPermissions,
    DescriptionObject doDescriptionObject) throws StorageServiceException, IOException {
    fs.createDirectory(DefaultStoragePath.parse("AIP", pid), getProperties(pid, doDescriptionObject, doPermissions));
    fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "metadata"),
      getProperties("metadata", doDescriptionObject, doPermissions));
    fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "metadata", "descriptive"),
      getProperties("descriptive", doDescriptionObject, doPermissions));
    fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "metadata", "preservation"),
      getProperties("preservation", doDescriptionObject, doPermissions));
    fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "data"),
      getProperties(pid, doDescriptionObject, doPermissions));
  }

  private void createFoldersForPreservationObject(String pid, RepresentationPreservationObject pObj,
    RepresentationObject rObject, RODAObjectPermissions rPermissions) throws StorageServiceException, IOException {
    fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "data", pObj.getRepresentationObjectPID()),
      getRepresentationProperties(pObj.getRepresentationObjectPID(), rObject, rPermissions, pObj));
    fs.createDirectory(
      DefaultStoragePath.parse("AIP", pid, "metadata", "preservation", pObj.getRepresentationObjectPID()), null);
  }

  private boolean verifyIfAIPShouldBeProcessed(String pid) {
    boolean processAIP;
    try {
      LOGGER.info("Checking if AIP " + pid + " already exists in the file system...");
      fs.getDirectory(DefaultStoragePath.parse("AIP", pid));
      LOGGER.info("  Exists!");
      processAIP = false;
    } catch (StorageServiceException t) {
      LOGGER.info("  Doesn't exist!");
      processAIP = true;
    }
    return processAIP;
  }

  private void createBinaryFromStream(DefaultStoragePath binaryPath, InputStream file,
    Map<String, Set<String>> properties, String extension) throws IOException, StorageServiceException {
    File temp = File.createTempFile("temp", extension);
    FileOutputStream eadFOS = new FileOutputStream(temp);
    IOUtils.copy(file, eadFOS);
    eadFOS.close();
    fs.createBinary(binaryPath, properties, new FSPathContentPayload(temp.toPath()), false);
    temp.delete();
    temp = null;
  }

  @SuppressWarnings("unused")
  private String humanReadableByteCount(long bytes, boolean si) {
    int unit = si ? 1000 : 1024;
    if (bytes < unit)
      return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

  @SuppressWarnings("unused")
  private List<String> getParents(String pid) {
    List<String> parents = new ArrayList<String>();
    try {
      while (true) {
        DescriptionObject d = client.getBrowserService().getDescriptionObject(pid);
        if (d.getParentPID() == null) {
          break;
        }
        parents.add(d.getParentPID());
        pid = d.getParentPID();
      }
    } catch (Throwable t) {
      LOGGER.error("Error getting parents of " + pid + ": " + t.getMessage());
    }
    return parents;
  }

  @SuppressWarnings("unused")
  private List<String> getAllChildrens(String pid) throws BrowserException, RemoteException, RODAClientException {
    List<String> children = new ArrayList<String>();

    List<String> directChildren = getChildrenPIDS(pid);
    if (directChildren != null) {
      children.addAll(directChildren);
      for (String s : directChildren) {
        children.addAll(getAllChildrens(s));
      }
    }

    return children;
  }

  private Map<String, Set<String>> getRepresentationProperties(String pid, RepresentationObject representationObject,
    RODAObjectPermissions permissions, RepresentationPreservationObject pObj) throws IOException {

    Map<String, Set<String>> data = new HashMap<String, Set<String>>();

    data = processObjectPermissions(permissions, data);
    data.put("active", Sets.newHashSet("true"));
    data.put("date.created", Sets.newHashSet(dateToString(representationObject.getCreatedDate())));
    data.put("date.modified", Sets.newHashSet(dateToString(representationObject.getLastModifiedDate())));
    data.put("representation.type", Sets.newHashSet(representationObject.getType()));
    data.put("representation.content.model", Sets.newHashSet(representationObject.getContentModel()));
    data.put("representation.dObject.pid", Sets.newHashSet(representationObject.getDescriptionObjectPID()));
    data.put("representation.id", Sets.newHashSet(representationObject.getId()));
    data.put("representation.label", Sets.newHashSet(representationObject.getLabel()));
    data.put("representation.pid", Sets.newHashSet(representationObject.getPid()));
    data.put("representation.state", Sets.newHashSet(representationObject.getState()));
    data.put("representation.subtype", Sets.newHashSet(representationObject.getSubType()));
    data.put("representation.type", Sets.newHashSet(representationObject.getType()));
    data = processRepresentationProperty(data, "representation.statuses", representationObject.getStatuses());

    return data;
  }

  private Map<String, Set<String>> processObjectPermissions(RODAObjectPermissions permissions,
    Map<String, Set<String>> data) {
    data = processRepresentationProperty(data, "grant.groups", permissions.getGrantGroups());
    data = processRepresentationProperty(data, "grant.users", permissions.getGrantUsers());
    data = processRepresentationProperty(data, "modify.groups", permissions.getModifyGroups());
    data = processRepresentationProperty(data, "modify.users", permissions.getModifyUsers());
    data = processRepresentationProperty(data, "read.groups", permissions.getReadGroups());
    data = processRepresentationProperty(data, "read.users", permissions.getReadUsers());
    data = processRepresentationProperty(data, "remove.groups", permissions.getRemoveGroups());
    data = processRepresentationProperty(data, "remove.users", permissions.getRemoveUsers());
    return data;
  }

  private Map<String, Set<String>> processRepresentationProperty(Map<String, Set<String>> data, String propertyKey,
    String[] values) {
    if (values != null && values.length > 0) {
      data.put(propertyKey, Sets.newHashSet(values));
    }
    return data;
  }

  private Map<String, Set<String>> getProperties(String title, RODAObject rodaObject, RODAObjectPermissions permissions)
    throws IOException {
    Map<String, Set<String>> data = new HashMap<String, Set<String>>();
    data.put("name", Sets.newHashSet(title));
    data = processObjectPermissions(permissions, data);
    if (rodaObject.getCreatedDate() != null) {
      data.put("date.created", Sets.newHashSet(dateToString(rodaObject.getCreatedDate())));
    }
    if (rodaObject.getLastModifiedDate() != null) {
      data.put("date.modified", Sets.newHashSet(dateToString(rodaObject.getLastModifiedDate())));
    }
    if (rodaObject instanceof DescriptionObject) {
      if (((DescriptionObject) rodaObject).getParentPID() != null) {
        data.put("parentId", Sets.newHashSet(((DescriptionObject) rodaObject).getParentPID()));
      }
    }
    data.put("content.model", Sets.newHashSet(rodaObject.getContentModel()));
    data.put("label", Sets.newHashSet(rodaObject.getLabel()));
    data.put("state", Sets.newHashSet(rodaObject.getState()));
    data.put("pid", Sets.newHashSet(rodaObject.getPid()));
    return data;
  }

  private void saveRepresentationObject(String doPid, String rPid, RODAClient rodaClient)
    throws RemoteException, RODAClientException, NoSuchRODAObjectException, BrowserException, IOException,
    LoginException, DownloaderException, StorageServiceException {
    RepresentationObject rep = rodaClient.getBrowserService().getRepresentationObject(rPid);
    if (rep != null) {
      DefaultStoragePath binaryPath = DefaultStoragePath.parse("AIP", doPid, "data", rPid,
        rep.getRootFile().getId() + "_" + rep.getRootFile().getOriginalName());
      Map<String, Set<String>> properties = new HashMap<String, Set<String>>();
      createBinaryFromStream(binaryPath, rodaClient.getDownloader().getFile(rPid, rep.getRootFile().getId()),
        properties, ".temp");
      if (rep.getPartFiles() != null) {
        for (RepresentationFile part : rep.getPartFiles()) {
          binaryPath = DefaultStoragePath.parse("AIP", doPid, "data", rPid,
            part.getId() + "_" + part.getOriginalName());
          properties = new HashMap<String, Set<String>>();
          createBinaryFromStream(binaryPath, rodaClient.getDownloader().getFile(rPid, part.getId()), properties,
            ".temp");
        }
      }
    }
  }

  private void copyDatastreams(String aipID, RepresentationPreservationObject obj, Downloader downloader,
    RODAObjectPermissions rPermissions) throws NoSuchRODAObjectException, DownloaderException, FileNotFoundException,
      IOException, StorageServiceException {

    File temp = File.createTempFile("representation", ".premis.xml");
    FileOutputStream repFOS = new FileOutputStream(temp);
    IOUtils.copy(downloader.getFile(obj.getPid(), "PREMIS"), repFOS);
    repFOS.close();
    fs.createBinary(
      DefaultStoragePath.parse("AIP", aipID, "metadata", "preservation", obj.getRepresentationObjectPID(),
        "representation.premis.xml"),
      new HashMap<String, Set<String>>(), new FSPathContentPayload(temp.toPath()), false);
    temp.delete();
    temp = null;
    if (obj.getRootFile() != null) {
      DefaultStoragePath binaryPath = DefaultStoragePath.parse("AIP", aipID, "metadata", "preservation",
        obj.getRepresentationObjectPID(), obj.getRootFile().getID() + ".premis.xml");
      Map<String, Set<String>> properties = new HashMap<String, Set<String>>();
      createBinaryFromStream(binaryPath, downloader.getFile(obj.getPid(), obj.getRootFile().getID()), properties,
        ".premis.xml");
    }
    if (obj.getPartFiles() != null) {
      for (RepresentationFilePreservationObject part : obj.getPartFiles()) {
        DefaultStoragePath binaryPath = DefaultStoragePath.parse("AIP", aipID, "metadata", "preservation",
          obj.getRepresentationObjectPID(), part.getID() + ".premis.xml");
        Map<String, Set<String>> properties = new HashMap<String, Set<String>>();
        createBinaryFromStream(binaryPath, downloader.getFile(obj.getPid(), part.getID()), properties, ".premis.xml");
      }
    }
  }

  private String dateToString(Date d) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    return sdf.format(d);
  }

  private List<String> getChildrenPIDS(String pid) throws BrowserException, RemoteException, RODAClientException {
    Filter filter = new Filter();
    filter.add(new SimpleFilterParameter("parentpid", pid));
    Sorter sorter = new Sorter();
    sorter.add(new SortParameter("lastModifiedDate", true));
    SimpleDescriptionObject[] childrens = client.getBrowserService()
      .getSimpleDescriptionObjects(new ContentAdapter(filter, sorter, null));
    if (childrens != null && childrens.length > 0) {
      List<String> pids = new ArrayList<String>();
      for (SimpleDescriptionObject d : childrens) {
        pids.add(d.getPid());
      }
      return pids;
    } else {
      return null;
    }
  }

  private static StorageService createStorageForAIPs(String baseOutputDir) throws StorageServiceException {
    Path path = Paths.get(baseOutputDir, "STORAGE");
    try {
      Files.createDirectories(path);
    } catch (IOException e) {
      throw new StorageServiceException("Unable to create folder " + path,
        StorageServiceException.INTERNAL_SERVER_ERROR, e);
    }
    StorageService fs = new FileStorageService(path);
    return fs;
  }

  public static void printUsageAndExit() {
    LOGGER.warn("****************************************************");
    LOGGER.warn("*              RODA Migration Utility              *");
    LOGGER.warn("****************************************************");
    LOGGER.warn("- Migrates all AIPs from RODA 1.x to 2.x");
    LOGGER.warn("");
    LOGGER.warn("Usage: java -jar Migrator.jar RODA_CORE_URL RODA_CORE_USERNAME RODA_CORE_PASSWORD BASE_OUTPUT_DIR");
    LOGGER.warn("");
    LOGGER.warn(
      "E.g. : java -jar Migrator.jar http://192.168.2.2:8080/roda-core administrator administrator /home/myuser/xpto/");
    LOGGER.warn("Note : All AIPS will be stored inside BASE_OUTPUT_DIR. E.g. /home/myuser/xpto/STORAGE/");
    System.exit(1);
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      printUsageAndExit();
    }

    try {
      LOGGER.info("Creating all necessary conditions to start the migration...");
      StorageService fs = createStorageForAIPs(args[3]);
      Migrator m = new Migrator(args[0], args[1], args[2], fs);

      while (true) {
        LOGGER.info("Starting new migration...");
        m.run();
        if (m.sucessfulRun) {
          break;
        }
        Thread.sleep(10000);
      }
      LOGGER.info("Success migrating AIPS from RODA 1.x!");
    } catch (InterruptedException e) {
      LOGGER.error("Error while trying to pause (sleep) thread!", e);
    } catch (Exception e) {
      LOGGER.error("Error while creating all the necessary conditions to do the migration!", e);
    }
    System.exit(0);
  }

}
