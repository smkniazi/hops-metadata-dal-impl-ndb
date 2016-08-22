package io.hops.metadata.ndb.dalimpl.hdfs;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.INode;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

/**
 * Created by salman on 2016-08-21.
 */
public class RootINodeCache {
  private static final int UPDATE_TIMER = 200; //ms
  private static final RootINodeCacheUpdaterThread rootCacheUpdater = new RootINodeCacheUpdaterThread();
  private static INode rootINode = null;
  private static boolean started = false;
  private static ClusterjConnector connector = ClusterjConnector.getInstance();
  private static RootINodeCache instance;
  private static final boolean ENABLE_CACHE = true;

  static {
    instance = new RootINodeCache();
  }

  private RootINodeCache() {
  }

  public static RootINodeCache getInstance() {
    return instance;
  }

  public static void start(){
    if(!started && ENABLE_CACHE){
      rootCacheUpdater.start();
    }
  }

  public static INode getRootINode() {
    synchronized (rootCacheUpdater) {
      return rootINode;
    }
  }

  public static boolean isRootInCache() {
    return rootINode != null;
  }

  private static class RootINodeCacheUpdaterThread extends Thread {
    @Override
    public void run() {
      started = true;
      INodeClusterj.LOG.debug("RootCache Started");
      while (true) {
        try {
          Thread.sleep(UPDATE_TIMER);
          HopsSession session = connector.obtainSession();
          Object key[] = new Object[3];
          key[0] = new Integer(0);
          key[1] = new Integer(0);
          key[2] = new String("");
          INodeClusterj.InodeDTO rootINodeDto = session.find(INodeClusterj.InodeDTO.class, key);
          if (rootINodeDto != null) {
            synchronized (rootCacheUpdater) {
              rootINode = INodeClusterj.convert(rootINodeDto);
            }
          }else{
            INodeClusterj.LOG.debug("RootCache: root does not exist.");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (StorageException e) {
          e.printStackTrace();
        }
      } // end while
    }
  }
}
