package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.*;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.FileInodeDataDataAccess;
import io.hops.metadata.hdfs.entity.FileInodeData;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.log4j.Logger;

/**
 * Created by salman on 3/10/16.
 */
public class FileInodeDataClusterj
        implements TablesDef.FileInodeData, FileInodeDataDataAccess<FileInodeData>
{
    static final Logger LOG = Logger.getLogger(FileInodeDataClusterj.class);
    private ClusterjConnector connector = ClusterjConnector.getInstance();


    @PersistenceCapable(table = TABLE_NAME)
    public interface FileInodeDataDTO {

        @PrimaryKey
        @Column(name = ID)
        int getInodeId();
        void setInodeId(int inodeId);

        @Column(name = DATA)
        byte[] getData();
        void setData(byte[] data);
    }



    @Override
    public void add(FileInodeData fileInodeData) throws StorageException {
        final HopsSession session = connector.obtainSession();

        FileInodeDataDTO dto =  session.newInstance(FileInodeDataClusterj.FileInodeDataDTO.class);
        dto.setInodeId(fileInodeData.getInodeId());
        dto.setData(fileInodeData.getInodeData());
        session.savePersistent(dto);
        session.release(dto);
    }

    @Override
    public void delete(FileInodeData fileInodeData) throws StorageException {
        final HopsSession session = connector.obtainSession();
        session.deletePersistent(FileInodeDataClusterj.FileInodeDataDTO.class, fileInodeData.getInodeData());
    }
}
