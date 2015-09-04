package org.roda.external;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.roda.model.ModelService;
import org.roda.model.ModelServiceException;
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

public class Migrator {
	RODAClient client;
	StorageService fs;
	ModelService ms;
	File log;
	File agentLog;
	File errorLog;
	File timeLog;
	List<String> pids;
	long beginning;
	int counter;

	public Migrator(String ip, String username, String password, StorageService storage, List<String> pids)
			throws LoginException, RODAClientException, StorageServiceException, IOException {
		log = new File("log.txt");
		agentLog = new File("agents.txt");
		errorLog = new File("errors.txt");
		timeLog = new File("time.txt");
		writeToLog(log, "Creating client...", true, true);
		client = new RODAClient(new URL(ip), username, password);
		writeToLog(log, "Client created...", true, true);
		this.fs = storage;
		this.ms = new ModelService(this.fs);
		this.pids = pids;
		this.beginning = System.currentTimeMillis();
		this.counter = 0;
		// fs.createContainer(DefaultStoragePath.parse("AIP"), new
		// HashMap<String, Set<String>>());
		// fs.createContainer(DefaultStoragePath.parse("Preservation"), new
		// HashMap<String, Set<String>>());
		// fs.createContainer(DefaultStoragePath.parse("Preservation",
		// "agents"), new HashMap<String, Set<String>>());
	}

	public RODAClient getClient() {
		return client;
	}

	public void setClient(RODAClient client) {
		this.client = client;
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage:_java -jar Migrator.jar RODA_CORE_URL RODA_CORE_USERNAME RODA_CORE_PASSWORD");
			System.exit(1);
		}

		try {
			File base = new File("STORAGE");
			base.mkdirs();
			StorageService fs = new FileStorageService(base.toPath());
			for (int i = 0; i < args.length; i++) {
				System.out.println("" + i + " - " + args[i]);
			}
			Migrator m = new Migrator(args[0], args[1], args[2], fs, Arrays.asList(args));
			while (true) {
				System.out.println("Starting new RUN...");
				boolean endSuccess = m.start();
				if (endSuccess) {
					break;
				}
				Thread.sleep(10000);
			}
			System.out.println("SUCCESS!!!!!!!!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	private boolean start() throws RODAClientException, LoginException, DownloaderException, BrowserException,
			NoSuchRODAObjectException, IOException, StorageServiceException, ModelServiceException {
		writeToLog(log, "START", true, true);
		Browser browser = client.getBrowserService();
		Downloader downloader = client.getDownloader();
		writeToLog(log, "Getting PIDS...", true, true);
		String[] pids = client.getBrowserService().getDOPIDs();
		writeToLog(log, "PIDS: " + pids.length, true, true);
		Set<String> agentPids = new HashSet<String>();

		/*
		 * LinkedHashSet<String> allPids = new LinkedHashSet<String>();
		 * 
		 * for(String pid : pids){
		 * 
		 * List<String> parents = getParents(pid); List<String> children =
		 * getAllChildrens(pid);
		 * 
		 * allPids.add(pid); allPids.addAll(parents); allPids.addAll(children);
		 * }
		 * 
		 * 
		 * for(String s : allPids){ System.out.println(s); }
		 */

		boolean endSuccess = true;

		for (String pid : pids) {

			boolean process = true;
			try {
				writeToLog(log, "Checking if AIP " + pid + " already exists...", true, true);
				fs.getDirectory(DefaultStoragePath.parse("AIP", pid));
				writeToLog(log, "Exists...", true, true);
				process = false;
			} catch (Throwable t) {
				writeToLog(log, "Doesn't exist...", true, true);
				process = true;
			}
			if (process) {
				try {
					RODAObjectPermissions doPermissions = client.getBrowserService().getRODAObjectPermissions(pid);
					DescriptionObject doDescriptionObject = client.getBrowserService().getDescriptionObject(pid);
					fs.createDirectory(DefaultStoragePath.parse("AIP", pid),
							getProperties(pid, doDescriptionObject, doPermissions));
					fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "metadata"),
							getProperties("metadata", doDescriptionObject, doPermissions));
					fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "metadata", "descriptive"),
							getProperties("descriptive", doDescriptionObject, doPermissions));
					fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "metadata", "preservation"),
							getProperties("preservation", doDescriptionObject, doPermissions));
					fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "data"),
							getProperties(pid, doDescriptionObject, doPermissions));

					DefaultStoragePath binaryPath = DefaultStoragePath.parse("AIP", pid, "metadata", "descriptive",
							"ead-c.xml");
					Map<String, Set<String>> properties = new HashMap<String, Set<String>>();
					createBinaryFromStream(binaryPath, downloader.getFile(pid, "EAD-C"), properties, ".xml");

					RepresentationPreservationObject[] preservationObjects = browser.getDOPreservationObjects(pid);
					if (preservationObjects != null) {

						for (RepresentationPreservationObject pObj : preservationObjects) {
							if (pObj.getRepresentationObjectPID() != null) {
								RepresentationObject rObject = browser
										.getRepresentationObject(pObj.getRepresentationObjectPID());
								RODAObjectPermissions rPermissions = browser.getRODAObjectPermissions(rObject.getPid());

								fs.createDirectory(
										DefaultStoragePath.parse("AIP", pid, "data", pObj.getRepresentationObjectPID()),
										getRepresentationProperties(pObj.getRepresentationObjectPID(), rObject,
												rPermissions, pObj));
								fs.createDirectory(DefaultStoragePath.parse("AIP", pid, "metadata", "preservation",
										pObj.getRepresentationObjectPID()), null);

								copyDatastreams(pid, pObj, downloader, rPermissions);

								EventPreservationObject[] repEvents = browser.getPreservationEvents(pObj.getPid());
								if (repEvents == null) {
									repEvents = new EventPreservationObject[] {};
								}

								for (EventPreservationObject repEvent : repEvents) {
									String eventPid = repEvent.getPid();
									binaryPath = DefaultStoragePath.parse("AIP", pid, "metadata", "preservation",
											pObj.getRepresentationObjectPID(),
											"event_" + eventPid.replace(':', '_') + ".premis.xml");
									properties = new HashMap<String, Set<String>>();
									createBinaryFromStream(binaryPath, downloader.getFile(eventPid, "PREMIS"),
											properties, ".premis.xml");
									agentPids.add(repEvent.getAgentPID());
									writeToLog(agentLog, repEvent.getAgentPID(), false, false);
								}
							}
							String roPid = pObj
									.getRepresentationObjectPID();/*
																	 * pObj.
																	 * getRootFile
																	 * ().
																	 * getContentLocationValue
																	 * ().split(
																	 * "/")[0];
																	 */
							if (roPid != null && !roPid.trim().equals("")) {
								System.out.println("Save representations(doPidO=" + pid + ",roPID=" + roPid);
								saveRepresentationObject(pid, roPid, client);
							}
						}
					}
					// ms.retrieveAIP(pid);
					counter++;
					if (counter % 10 == 0) {
						// writeToLog(timeLog, createStatLogLine(), false);
						long current = System.currentTimeMillis();
						writeToLog(timeLog,
								"PIDS PROCESSED: " + getNumberOfChildren(new File(new File("STORAGE"), "AIP"))
										+ " FOLDERSIZE: "
										+ humanReadableByteCount(FileUtils.sizeOfDirectory(new File("STORAGE")), true)
										+ " TIME ELAPSED:" + ((current - beginning) / 1000) + " s",
								false, true);
					}
				} catch (Throwable t) {
					endSuccess = false;
					writeToLog(errorLog, "" + pid + "Error: " + t.getMessage(), true, true);
					try {
						ms.deleteAIP(pid);
					} catch (Throwable t1) {
						writeToLog(errorLog, "Error deleting AIP " + pid + " : " + t.getMessage(), true, true);
					}
				}
			}
		}
		return endSuccess;
		/*
		 * if(agentPids!=null){ for (String agentPid : agentPids) {
		 * if(agentPid!=null){ try{
		 * 
		 * DefaultStoragePath binaryPath =
		 * DefaultStoragePath.parse("Preservation", "agents", "agent_" +
		 * agentPid.replace(':', '_') + ".premis.xml"); Map<String,Set<String>>
		 * properties = new HashMap<String, Set<String>>();
		 * createBinaryFromStream(binaryPath,downloader.getFile(agentPid,
		 * "PREMIS"),properties,".premis.xml"); }catch(Throwable t){
		 * writeToLog(log,"Error writing agent: "+t.getMessage(),true); } } } }
		 */
	}

	private long getNumberOfChildren(File file) {
		return file.list().length;
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

	public String humanReadableByteCount(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
		if (bytes < unit)
			return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	public void writeToLog(File logFile, String msg, boolean addTimestamp, boolean sysout) {
		if (sysout) {
			System.out.println(msg);
		}
		FileWriter fileWriter = null;
		BufferedWriter bufferedWriter = null;
		try {
			fileWriter = new FileWriter(logFile.getAbsoluteFile(), true); // true
																			// to
																			// append
			bufferedWriter = new BufferedWriter(fileWriter);
			if (addTimestamp) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm");
				Date resultdate = new Date(System.currentTimeMillis());
				bufferedWriter.write(sdf.format(resultdate) + " - ");
				bufferedWriter.write(msg);
			}
			bufferedWriter.write(msg);
			bufferedWriter.newLine();
			bufferedWriter.close();
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			bufferedWriter.close();
		} catch (Throwable t) {

		}
		try {
			fileWriter.close();
		} catch (Throwable t) {

		}
	}

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
			writeToLog(log, "Error getting parents of " + pid + ": " + t.getMessage(), true, true);
		}
		return parents;
	}

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
		data.put("active", Sets.newHashSet("true"));
		data.put("date.created", Sets.newHashSet(dateToString(representationObject.getCreatedDate())));
		data.put("date.modified", Sets.newHashSet(dateToString(representationObject.getLastModifiedDate())));
		data.put("type", Sets.newHashSet(representationObject.getType()));
		if (representationObject.getStatuses() != null && representationObject.getStatuses().length > 0) {
			data.put("representation.statuses", Sets.newHashSet(representationObject.getStatuses()));
		}
		return data;

	}

	private Map<String, Set<String>> getProperties(String title, RODAObject rodaObject,
			RODAObjectPermissions permissions) throws IOException {
		Map<String, Set<String>> data = new HashMap<String, Set<String>>();
		data.put("name", Sets.newHashSet(title));
		if (permissions.getGrantGroups() != null && permissions.getGrantGroups().length > 0) {
			data.put("grant.groups", Sets.newHashSet(permissions.getGrantGroups()));
		}
		if (permissions.getGrantUsers() != null && permissions.getGrantUsers().length > 0) {
			data.put("grant.users", Sets.newHashSet(permissions.getGrantUsers()));
		}
		if (permissions.getModifyGroups() != null && permissions.getModifyGroups().length > 0) {
			data.put("modify.groups", Sets.newHashSet(permissions.getModifyGroups()));
		}
		if (permissions.getModifyUsers() != null && permissions.getModifyUsers().length > 0) {
			data.put("modify.users", Sets.newHashSet(permissions.getModifyUsers()));
		}
		if (permissions.getReadGroups() != null && permissions.getReadGroups().length > 0) {
			data.put("read.groups", Sets.newHashSet(permissions.getReadGroups()));
		}
		if (permissions.getReadUsers() != null && permissions.getReadUsers().length > 0) {
			data.put("read.users", Sets.newHashSet(permissions.getReadUsers()));
		}
		if (permissions.getRemoveGroups() != null && permissions.getRemoveGroups().length > 0) {
			data.put("remove.groups", Sets.newHashSet(permissions.getRemoveGroups()));
		}
		if (permissions.getRemoveUsers() != null && permissions.getRemoveUsers().length > 0) {
			data.put("remove.users", Sets.newHashSet(permissions.getRemoveUsers()));
		}
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
					createBinaryFromStream(binaryPath, rodaClient.getDownloader().getFile(rPid, part.getId()),
							properties, ".temp");
				}
			}
		}
	}

	private void copyDatastreams(String aipID, RepresentationPreservationObject obj, Downloader downloader,
			RODAObjectPermissions rPermissions) throws NoSuchRODAObjectException, DownloaderException,
					FileNotFoundException, IOException, StorageServiceException {

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
			createBinaryFromStream(binaryPath, downloader.getFile(obj.getPid(), "PREMIS"), properties, ".premis.xml");
		}
		if (obj.getPartFiles() != null) {
			for (RepresentationFilePreservationObject part : obj.getPartFiles()) {
				DefaultStoragePath binaryPath = DefaultStoragePath.parse("AIP", aipID, "metadata", "preservation",
						obj.getRepresentationObjectPID(), part.getID() + ".premis.xml");
				Map<String, Set<String>> properties = new HashMap<String, Set<String>>();
				createBinaryFromStream(binaryPath, downloader.getFile(obj.getPid(), part.getID()), properties,
						".premis.xml");
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
}
